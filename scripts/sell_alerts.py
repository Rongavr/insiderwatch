#!/usr/bin/env python3
import os, re, time, smtplib, ssl, math
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dtp

SEC_EMAIL = os.getenv("SEC_EMAIL", "alerts@example.com")
HEADERS = {
    "User-Agent": f"InsiderSellAlerts/1.0 ({SEC_EMAIL})",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
    "Connection": "keep-alive",
}
ATOM_FEED = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=200&output=atom"

MIN_USD = float(os.getenv("MIN_USD", "100000"))  # $100k
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))  # per-run window (digest)
EXCLUDE_10B5 = os.getenv("EXCLUDE_10B5", "1") == "1"
EXCLUDE_COVER = os.getenv("EXCLUDE_COVER", "1") == "1"  # sell-to-cover
PEOPLE_ONLY = os.getenv("PEOPLE_ONLY", "1") == "1"

SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
TO_EMAIL      = os.getenv("TO_EMAIL")
FROM_EMAIL    = os.getenv("FROM_EMAIL", MAIL_USERNAME)

OUT_TRADES = "insider_trades.csv"
OUT_ALERTS = "alerts.csv"

def fetch(url, is_html=False, tries=3, sleep_sec=1.5):
    last = None
    for i in range(tries):
        r = requests.get(url, headers=HEADERS, timeout=30)
        last = r
        if r.status_code == 200:
            return r.text if is_html else r.content
        time.sleep(sleep_sec * (i+1))
    raise RuntimeError(f"HTTP {last.status_code} for {url}")

def get_atom_entries():
    html = fetch(ATOM_FEED, is_html=True)
    soup = BeautifulSoup(html, "lxml-xml")
    entries = []
    for e in soup.find_all("entry"):
        link = e.find("link")
        if not link or not link.get("href"): 
            continue
        href = link["href"]
        # filing time
        updated = e.find("updated")
        when = dtp.parse(updated.text).astimezone(timezone.utc) if updated else datetime.now(timezone.utc)
        entries.append({"index_url": href, "filing_dt": when})
    return entries

def find_xml_candidates(index_url):
    html = fetch(index_url, is_html=True)
    # priority candidates
    pats = [
        r'href="([^"]*ownership\.xml)"',
        r'href="([^"]*primary_doc\.xml)"',
        r'href="([^"]*\.xml)"',
        r'href="([^"]*\.txt)"',
    ]
    cands = []
    for pat in pats:
        for m in re.finditer(pat, html, flags=re.I):
            href = m.group(1)
            if href.lower().endswith((".xml",".txt")) and href not in cands:
                # resolve relative to index
                base = index_url.rsplit("/", 1)[0]
                if href.startswith("http"):
                    cands.append(href)
                else:
                    if href.startswith("/"):
                        cands.append("https://www.sec.gov" + href)
                    else:
                        cands.append(base + "/" + href)
    return cands

def parse_form4_sells(xml_bytes):
    """Return list of sells: [{'symbol','owner','shares','price','amount_usd','tenb5','txn_date','sell_to_cover'}]"""
    # normalize to xml
    try:
        text = xml_bytes.decode("utf-8", errors="ignore")
    except:
        text = str(xml_bytes)
    # crude 10b5-1 detection
    tenb5 = bool(re.search(r'10b5-1', text, flags=re.I))
    soup = BeautifulSoup(text, "lxml-xml")
    out = []

    # symbol
    sym = None
    ts = soup.find("issuerTradingSymbol")
    if ts and ts.text.strip():
        sym = ts.text.strip().upper()

    # owner name
    owner_name = "UNKNOWN"
    ro = soup.find("reportingOwner")
    if ro:
        rn = ro.find("rptOwnerName")
        if rn and rn.text.strip():
            owner_name = rn.text.strip()

    # quick entity filter (people only)
    def looks_like_entity(name):
        return bool(re.search(r'\b(LLC|L\.?P\.?|LTD|HOLDINGS|CAPITAL|FUND|PARTNERS|ADVISORS|MGMT|MANAGEMENT|CORP|INC)\b', name, re.I))

    # collect all non-derivative transactions
    has_M = False
    txrows = []
    for tx in soup.find_all("nonDerivativeTransaction"):
        code = tx.find("transactionCode")
        code = code.text.strip().upper() if code else ""
        if code == "M":
            has_M = True
        if code != "S":
            continue
        sh = tx.find("transactionShares")
        pr = tx.find("transactionPricePerShare")
        dt = tx.find("transactionDate")
        try:
            shares = float(sh.find("value").text) if sh and sh.find("value") else float(sh.text)
        except:
            shares = float("nan")
        try:
            price = float(pr.find("value").text) if pr and pr.find("value") else float(pr.text)
        except:
            price = float("nan")
        try:
            when = dtp.parse(dt.find("value").text).date().isoformat() if dt and dt.find("value") else dtp.parse(dt.text).date().isoformat()
        except:
            when = None
        amt = shares * price if (shares==shares and price==price) else float("nan")
        txrows.append({"symbol": sym, "owner": owner_name, "shares": shares, "price": price, "amount_usd": amt, "txn_date": when})

    # mark sell-to-cover if M present on same form
    for r in txrows:
        r["tenb5"] = tenb5
        r["sell_to_cover"] = has_M

    # filters
    if PEOPLE_ONLY and looks_like_entity(owner_name):
        return []
    rows = []
    for r in txrows:
        if EXCLUDE_10B5 and r["tenb5"]:
            continue
        if EXCLUDE_COVER and r["sell_to_cover"]:
            continue
        if r["amount_usd"] != r["amount_usd"]:
            continue
        if r["amount_usd"] < MIN_USD:
            continue
        rows.append(r)
    return rows

def load_df(path):
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except:
            pass
    return pd.DataFrame(columns=["filing_dt","symbol","owner","shares","price","amount_usd","tenb5","txn_date","filing_url","xml_url","sell_to_cover"])

def save_df(df, path):
    df.to_csv(path, index=False)

def send_email(subject, body):
    if not (MAIL_USERNAME and MAIL_PASSWORD and TO_EMAIL and FROM_EMAIL):
        print("[WARN] Missing mail envs; skipping email.")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls(context=ctx)
        s.login(MAIL_USERNAME, MAIL_PASSWORD)
        s.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())

def main():
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=LOOKBACK_HOURS)

    entries = get_atom_entries()
    # keep within lookback by filing time
    entries = [e for e in entries if e["filing_dt"] >= since]

    all_rows = []
    for e in entries:
        idx = e["index_url"]
        cands = find_xml_candidates(idx)
        found = False
        for u in cands:
            try:
                data = fetch(u)
                rows = parse_form4_sells(data)
                for r in rows:
                    r["filing_dt"] = e["filing_dt"].isoformat()
                    r["filing_url"] = idx
                    r["xml_url"] = u
                if rows:
                    all_rows.extend(rows)
                    found = True
                    break
            except Exception as ex:
                # try next candidate
                continue

    # append to insider_trades.csv (sell events only)
    trades = load_df(OUT_TRADES)
    if all_rows:
        newdf = pd.DataFrame(all_rows)
        trades = pd.concat([trades, newdf], ignore_index=True)
        save_df(trades, OUT_TRADES)

    # per-run digest (alerts.csv)
    run_rows = [r for r in all_rows]  # already filtered by lookback + min_usd
    if run_rows:
        df = pd.DataFrame(run_rows)
        # build digest text
        lines = []
        lines.append(f"US Insider SELL digest (last {LOOKBACK_HOURS}h, >= ${int(MIN_USD):,}, filters: 10b5-1 excluded={EXCLUDE_10B5}, sell-to-cover excluded={EXCLUDE_COVER}, people_only={PEOPLE_ONLY})")
        lines.append("")
        for sym, grp in df.groupby("symbol"):
            total = grp["amount_usd"].sum()
            lines.append(f"{sym}  —  {len(grp)} sale(s), total ${total:,.0f}")
            for _, r in grp.sort_values("amount_usd", ascending=False).iterrows():
                lines.append(f"  • {r['owner']}  {int(r['shares']):,} @ ${r['price']:,.2f} = ${r['amount_usd']:,.0f}  on {r['txn_date']}  (10b5-1={bool(r['tenb5'])}, cover={bool(r['sell_to_cover'])})")
                lines.append(f"    filing: {r['filing_url']}")
        body = "\n".join(lines)
        subject = f"[InsiderWatch] SELL digest — {len(df)} trade(s), {df['symbol'].nunique()} tickers"

        # persist run alerts.csv (summary per symbol)
        alerts = (df.groupby("symbol", as_index=False)
                    .agg(owners_count=("owner", "nunique"),
                         total_usd=("amount_usd","sum"))
                 )
        alerts["last_when"] = now.isoformat()
        alerts.to_csv(OUT_ALERTS, index=False)

        # email
        send_email(subject, body)
        print(body)
    else:
        # still write empty alerts.csv for artifact visibility
        pd.DataFrame(columns=["symbol","owners_count","total_usd","last_when"]).to_csv(OUT_ALERTS, index=False)
        print("No qualifying sells found in lookback window.")

if __name__ == "__main__":
    main()

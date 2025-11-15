#!/usr/bin/env python3
import os, re, time, smtplib, ssl, math, random
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dtp

# -------- SEC polite headers & feed --------
SEC_EMAIL = os.getenv("SEC_EMAIL", "alerts@example.com")
HEADERS = {
    "User-Agent": f"InsiderSellAlerts/1.0 ({SEC_EMAIL})",
    "From": SEC_EMAIL,
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}
# Smaller page size = fewer index hits (helps avoid 429)
ATOM_FEED = os.getenv(
    "ATOM_FEED",
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=60&output=atom",
)

# -------- Filters / window --------
MIN_USD = float(os.getenv("MIN_USD", "100000"))       # $100k minimum per transaction
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))
EXCLUDE_10B5 = os.getenv("EXCLUDE_10B5", "1") == "1"  # exclude 10b5-1 plans
EXCLUDE_COVER = os.getenv("EXCLUDE_COVER", "1") == "1"  # exclude sell-to-cover
PEOPLE_ONLY = os.getenv("PEOPLE_ONLY", "1") == "1"    # ignore funds/LLCs etc

# -------- Email settings --------
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
TO_EMAIL      = os.getenv("TO_EMAIL")
FROM_EMAIL    = os.getenv("FROM_EMAIL", MAIL_USERNAME)

# -------- Outputs (saved as artifacts) --------
OUT_TRADES = os.getenv("OUT_TRADES", "insider_trades.csv")
OUT_ALERTS = os.getenv("OUT_ALERTS", "alerts.csv")

# -------- Throttle / retry knobs (tunable via workflow env) --------
SEC_THROTTLE_MS = int(os.getenv("SEC_THROTTLE_MS", "1200"))  # ~1.2s between requests
SEC_MAX_RETRIES = int(os.getenv("SEC_MAX_RETRIES", "6"))

_session = requests.Session()
_retry = Retry(
    total=SEC_MAX_RETRIES,
    connect=SEC_MAX_RETRIES,
    read=SEC_MAX_RETRIES,
    backoff_factor=1.2,
    respect_retry_after_header=True,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("https://", _adapter)
_session.mount("http://",  _adapter)
_last_ts = 0.0

def fetch(url, is_html=False):
    """Polite SEC fetch: global throttle + retry + Retry-After support."""
    global _last_ts
    now = time.time()
    wait = SEC_THROTTLE_MS / 1000.0 - (now - _last_ts)
    if wait > 0:
        time.sleep(wait + random.uniform(0, 0.2))

    r = _session.get(url, headers=HEADERS, timeout=30)
    _last_ts = time.time()

    if r.status_code == 429:
        ra = r.headers.get("Retry-After")
        time.sleep(float(ra) if ra and ra.isdigit() else 3.0)
        return fetch(url, is_html=is_html)

    r.raise_for_status()
    return r.text if is_html else r.content

def get_atom_entries():
    html = fetch(ATOM_FEED, is_html=True)
    soup = BeautifulSoup(html, "lxml-xml")
    entries = []
    for e in soup.find_all("entry"):
        link = e.find("link")
        if not link or not link.get("href"):
            continue
        href = link["href"]
        updated = e.find("updated")
        when = dtp.parse(updated.text).astimezone(timezone.utc) if updated else datetime.now(timezone.utc)
        entries.append({"index_url": href, "filing_dt": when})
    return entries

def find_xml_candidates(index_url):
    html = fetch(index_url, is_html=True)
    cands = []
    for pat in [
        r'href="([^"]*ownership\.xml)"',
        r'href="([^"]*primary_doc\.xml)"',
        r'href="([^"]*\.xml)"',
        r'href="([^"]*\.txt)"',
    ]:
        for m in re.finditer(pat, html, flags=re.I):
            href = m.group(1)
            abs_url = urljoin(index_url, href)
            if abs_url.lower().endswith(("-index.htm", "-index.html")):
                continue
            if abs_url not in cands:
                cands.append(abs_url)
    return cands

def parse_form4_sells(xml_bytes):
    """
    Return list of sells:
    [{'symbol','owner','shares','price','amount_usd','tenb5','txn_date','sell_to_cover'}]
    """
    try:
        text = xml_bytes.decode("utf-8", errors="ignore")
    except Exception:
        text = str(xml_bytes)

    tenb5 = bool(re.search(r'10b5-1', text, flags=re.I))
    soup = BeautifulSoup(text, "lxml-xml")

    # symbol
    sym = None
    ts = soup.find("issuerTradingSymbol")
    if ts and ts.text and ts.text.strip():
        sym = ts.text.strip().upper()

    # owners (can be multiple)
    owners = []
    for ro in soup.find_all("reportingOwner"):
        rn = ro.find("rptOwnerName")
        if rn and rn.text and rn.text.strip():
            owners.append(rn.text.strip())
    if not owners:
        owners = ["UNKNOWN"]

    def looks_like_entity(name):
        return bool(re.search(r'\b(LLC|L\.?P\.?|LTD|HOLDINGS|CAPITAL|FUND|PARTNERS|ADVISORS|MGMT|MANAGEMENT|CORP|INC|TRUST)\b', name, re.I))

    # mark sell-to-cover if option exercises (M) present on same form
    has_M = any(
        (tx.find("transactionCode") and tx.find("transactionCode").text.strip().upper() == "M")
        for tx in soup.find_all("nonDerivativeTransaction")
    )

    rows = []
    for tx in soup.find_all("nonDerivativeTransaction"):
        code = tx.find("transactionCode")
        code = code.text.strip().upper() if code else ""
        if code != "S":  # only sells
            continue

        def val(node):
            if not node:
                return None
            v = node.find("value")
            return (v.text if v else node.text).strip()

        try:
            shares = float(val(tx.find("transactionShares"))) if val(tx.find("transactionShares")) is not None else float("nan")
        except:
            shares = float("nan")
        try:
            price = float(val(tx.find("transactionPricePerShare"))) if val(tx.find("transactionPricePerShare")) is not None else float("nan")
        except:
            price = float("nan")
        try:
            dt_node = tx.find("transactionDate")
            when = dtp.parse(val(dt_node)).date().isoformat() if val(dt_node) else None
        except:
            when = None

        amt = shares * price if (shares == shares and price == price) else float("nan")

        for owner_name in owners:
            r = {
                "symbol": sym,
                "owner": owner_name,
                "shares": shares,
                "price": price,
                "amount_usd": amt,
                "tenb5": tenb5,
                "txn_date": when,
                "sell_to_cover": has_M
            }
            # Filters
            if PEOPLE_ONLY and looks_like_entity(owner_name):
                continue
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
        except Exception:
            pass
    return pd.DataFrame(columns=[
        "filing_dt","symbol","owner","shares","price","amount_usd",
        "tenb5","txn_date","filing_url","xml_url","sell_to_cover"
    ])

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

    entries = [e for e in get_atom_entries() if e["filing_dt"] >= since]

    all_rows = []
    for e in entries:
        idx = e["index_url"]
        cands = find_xml_candidates(idx)
        for u in cands:
            try:
                data = fetch(u)
                rows = parse_form4_sells(data)
                if rows:
                    for r in rows:
                        r["filing_dt"] = e["filing_dt"].isoformat()
                        r["filing_url"] = idx
                        r["xml_url"] = u
                    all_rows.extend(rows)
                    break  # first good candidate is enough
            except Exception:
                # try next candidate
                continue

    # Append to insider_trades.csv (sell events only)
    trades = load_df(OUT_TRADES)
    if all_rows:
        newdf = pd.DataFrame(all_rows)
        trades = pd.concat([trades, newdf], ignore_index=True)
        save_df(trades, OUT_TRADES)

    # Per-run digest (alerts.csv)
    run_rows = list(all_rows)
    if run_rows:
        df = pd.DataFrame(run_rows)
        lines = []
        lines.append(f"US Insider SELL digest (last {LOOKBACK_HOURS}h, >= ${int(MIN_USD):,}, "
                     f"filters: 10b5-1 excluded={EXCLUDE_10B5}, sell-to-cover excluded={EXCLUDE_COVER}, people_only={PEOPLE_ONLY})")
        lines.append("")
        for sym, grp in df.groupby("symbol"):
            total = grp["amount_usd"].sum()
            lines.append(f"{sym}  —  {len(grp)} sale(s), total ${total:,.0f}")
            for _, r in grp.sort_values("amount_usd", ascending=False).iterrows():
                sh = int(r['shares']) if pd.notna(r['shares']) else 0
                pr = float(r['price']) if pd.notna(r['price']) else 0.0
                amt = float(r['amount_usd']) if pd.notna(r['amount_usd']) else 0.0
                lines.append(f"  • {r['owner']}  {sh:,} @ ${pr:,.2f} = ${amt:,.0f}  on {r['txn_date']}  "
                             f"(10b5-1={bool(r['tenb5'])}, cover={bool(r['sell_to_cover'])})")
                lines.append(f"    filing: {r['filing_url']}")
        body = "\n".join(lines)
        subject = f"[InsiderWatch] SELL digest — {len(df)} trade(s), {df['symbol'].nunique()} tickers"

        alerts = (df.groupby("symbol", as_index=False)
                    .agg(owners_count=("owner", "nunique"),
                         total_usd=("amount_usd","sum")))
        alerts["last_when"] = now.isoformat()
        alerts.to_csv(OUT_ALERTS, index=False)

        send_email(subject, body)
        print(body)
    else:
        pd.DataFrame(columns=["symbol","owners_count","total_usd","last_when"]).to_csv(OUT_ALERTS, index=False)
        print("No qualifying sells found in lookback window.")

if __name__ == "__main__":
    main()

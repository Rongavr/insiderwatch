#!/usr/bin/env python3
# scripts/sell_alerts.py
import os, re, time, smtplib, ssl
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtp

# -------- config (from GitHub Secrets / env) ----------
SEC_EMAIL = os.getenv("SEC_EMAIL") or os.getenv("MAIL_USER") or "you@example.com"
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "12"))
MIN_USD        = float(os.getenv("MIN_USD_SELL", "100000"))

MAIL_USER = os.getenv("MAIL_USER")
MAIL_PASS = os.getenv("MAIL_PASS")
MAIL_FROM = os.getenv("MAIL_FROM", MAIL_USER)
MAIL_TO   = os.getenv("MAIL_TO", MAIL_USER)

ATOM_FEED = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=100&output=atom"
HEADERS = {
    "User-Agent": f"InsiderWatch/1.0 ({SEC_EMAIL})",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Exclude funds/entities; keep only named people
EXCLUDE_ENT_RE = re.compile(
    r"\b(LLC|L\.?P\.?|LP|LTD|LIMITED|HOLDINGS?|CAPITAL|PARTNERS?|FUND|TRUST|ADVIS(OR|ERS)|MGMT|MANAGEMENT|CORP|INC)\b",
    re.I,
)

def fetch(url, is_html=False, tries=6, base_sleep=1.5):
    """
    Polite fetch with exponential backoff and specific handling for 429.
    """
    last = None
    for i in range(tries):
        r = requests.get(url, headers=HEADERS, timeout=30)
        last = r
        if r.status_code == 200:
            return r.text if is_html else r.content

        # Respect Retry-After on 429
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            try:
                wait = int(ra)
            except Exception:
                wait = int(base_sleep * (2 ** i))
            time.sleep(max(wait, 2))
            continue

        # Other transient errors → backoff
        time.sleep(base_sleep * (2 ** i))

    raise RuntimeError(f"HTTP {last.status_code} for {url}")

def get_atom_entries():
    html = fetch(ATOM_FEED, is_html=True)
    soup = BeautifulSoup(html, "lxml-xml")
    out = []
    for e in soup.find_all("entry"):
        link = e.find("link")
        if not link or not link.get("href"):
            continue
        updated = e.find("updated")
        out.append({
            "index_url": link.get("href"),
            "updated": dtp.parse(updated.text).astimezone(timezone.utc) if updated else None,
        })
    return out

def find_xml_candidates(index_url):
    html = fetch(index_url, is_html=True)
    cands = set()
    for pat in [r'href="([^"]*ownership\.xml)"',
                r'href="([^"]*primary_doc\.xml)"',
                r'href="([^"]*\.xml)"',
                r'href="([^"]*\.txt)"']:
        for m in re.finditer(pat, html, flags=re.I):
            cands.add(urljoin(index_url, m.group(1)))
    return list(cands)

def parse_form4_sells(xml_bytes):
    """
    Return list of SELL rows:
    {symbol, owner, shares, price, amount_usd, txn_date}
    (filters: people only; amount >= MIN_USD)
    """
    text = xml_bytes.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(text, "lxml-xml")

    sym_tag = soup.find("issuerTradingSymbol") or soup.find("issuerSymbol")
    symbol = (sym_tag.text or "").strip().upper() if sym_tag else None

    # choose first human reporter
    owner = None
    for ro in soup.find_all("reportingOwner"):
        nm = ro.find("rptOwnerName")
        if not nm:
            continue
        name = (nm.text or "").strip()
        if not EXCLUDE_ENT_RE.search(name):
            owner = name
            break
    if not owner:
        return []  # only people

    sells = []
    for tr in soup.find_all("nonDerivativeTransaction"):
        code = tr.find("transactionCode")
        code = (code.text or "").strip().upper() if code else ""
        if code != "S":
            continue
        sh = tr.find("transactionShares")
        pr = tr.find("transactionPricePerShare")
        dt = tr.find("transactionDate")
        try:
            shares = float((sh.value.text if (sh and sh.value) else sh.text))
        except Exception:
            shares = 0.0
        try:
            price = float((pr.value.text if (pr and pr.value) else pr.text))
        except Exception:
            price = 0.0
        amt = shares * price
        if amt < MIN_USD:
            continue
        when = None
        if dt:
            v = dt.find("value") or dt
            try:
                when = dtp.parse(v.text).date().isoformat()
            except Exception:
                when = None
        sells.append({
            "symbol": symbol,
            "owner": owner,
            "shares": shares,
            "price": price,
            "amount_usd": amt,
            "txn_date": when
        })
    return sells

def send_email(subject, body):
    if not (MAIL_USER and MAIL_PASS and MAIL_TO and MAIL_FROM):
        print("[WARN] Missing mail creds; skip email.")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    ctx = ssl.create_default_context()
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls(context=ctx)
        s.login(MAIL_USER, MAIL_PASS)
        s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())

def main():
    since = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    entries = [e for e in get_atom_entries() if (e["updated"] or since) >= since]

    hits = []
    for e in entries:
        for cand in find_xml_candidates(e["index_url"]):
            try:
                xml = fetch(cand)
            except Exception:
                continue
            rows = parse_form4_sells(xml)
            if rows:
                for r in rows:
                    r["filing_url"] = e["index_url"]
                    r["xml_url"] = cand
                hits.extend(rows)
                break  # first good candidate is enough

    # group + format
    by_symbol = {}
    for h in hits:
        sym = h.get("symbol") or "UNKNOWN"
        by_symbol.setdefault(sym, {"total": 0.0, "owners": set(), "rows": []})
        by_symbol[sym]["total"] += h["amount_usd"]
        if h.get("owner"):
            by_symbol[sym]["owners"].add(h["owner"])
        by_symbol[sym]["rows"].append(h)

    lines = []
    tickers = 0
    for sym, agg in sorted(by_symbol.items(), key=lambda kv: -kv[1]["total"]):
        if agg["total"] < MIN_USD:
            continue
        tickers += 1
        lines.append(f"{sym}: ${agg['total']:,.0f} across {len(agg['owners'])} insider(s)")
        for r in sorted(agg["rows"], key=lambda x: -x["amount_usd"]):
            lines.append(f"  - {r.get('owner','Unknown')}: {int(r['shares']):,} @ ${r['price']:,.2f} = ${r['amount_usd']:,.0f} (date: {r.get('txn_date')})")
        lines.append(f"  Filing: {agg['rows'][0]['filing_url']}")
        lines.append("")

    if not lines:
        subject = f"US Insider SELLs (last {LOOKBACK_HOURS}h): none"
        body = f"No insider sells ≥ ${MIN_USD:,.0f} in the last {LOOKBACK_HOURS} hours."
    else:
        subject = f"US Insider SELLs (last {LOOKBACK_HOURS}h): {tickers} ticker(s)"
        body = f"Filters: only named people, sells ≥ ${MIN_USD:,.0f}\nWindow: last {LOOKBACK_HOURS} hours\n\n" + "\n".join(lines)

    print(body)
    send_email(subject, body)

if __name__ == "__main__":
    main()


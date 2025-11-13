#!/usr/bin/env python3
import os, re, sys, smtplib, csv, io, time
from datetime import datetime, timezone, timedelta
import feedparser
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

NOW = datetime.now(timezone.utc)
WINDOW_HOURS = 12  # look-back per run

RSS_LIST = [u.strip() for u in os.getenv("TASE_RSS_URLS", "").split(",") if u.strip()]
MIN_NIS = float(os.getenv("MIN_NIS", "0"))

# Simple heuristics to detect people (not funds/LLCs) and sells
BAD_ENTITIES = [
    "Ltd", "Limited", "Trust", "Fund", "Partnership", "ETF", "LP",
    "בע\"מ", "שותפות", "קרן", "נאמנות"
]
SELL_WORDS = [
    "sell", "sale", "disposed", "disposal",
    "מכר", "מכירה", "הפחתת", "הקטנת", "מימש"
]
HOLDINGS_WORDS = [
    "interested party", "officer", "holdings", "change in holdings",
    "בעל עניין", "נושא משרה", "שינוי בהחזקות", "דיווח החזקה"
]

def is_personish(text: str) -> bool:
    return not any(b.lower() in text.lower() for b in BAD_ENTITIES)

def looks_like_sell(text: str) -> bool:
    t = text.lower()
    return any(w in t for w in SELL_WORDS) and any(w in t for w in HOLDINGS_WORDS)

def parse_amount_nis(text: str) -> float:
    # Try to pull a number like 1,234,567 or 1.2M from the text (best-effort).
    m = re.search(r'([\d,\.]+)\s*(אלף|מיליון|אלפים|אלופים|K|M)?', text)
    if not m: return 0.0
    val = m.group(1).replace(",", "")
    try:
        x = float(val)
    except:
        return 0.0
    mult = m.group(2) or ""
    mult = mult.lower()
    if mult in ("m", "מיליון"): x *= 1_000_000
    elif mult in ("k", "אלף", "אלפים"): x *= 1_000
    return x

def send_email(subject: str, html_body: str):
    user = os.environ["MAIL_USERNAME"]
    pwd  = os.environ["MAIL_PASSWORD"]
    host = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    from_addr = os.environ.get("FROM_EMAIL", user)
    to_addr   = os.environ.get("TO_EMAIL", user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.sendmail(from_addr, [to_addr], msg.as_string())

def main():
    log = io.StringIO()
    print(f"[{NOW.isoformat()}] TASE run start", file=log)

    if not RSS_LIST:
        print("No TASE_RSS_URLS configured → exiting OK.", file=log)
        open("tase.log","w").write(log.getvalue())
        return

    rows = []
    cutoff = NOW - timedelta(hours=WINDOW_HOURS)

    for url in RSS_LIST:
        try:
            feed = feedparser.parse(url)
        except Exception as e:
            print(f"ERR loading RSS: {url} → {e}", file=log)
            continue

        for e in feed.entries:
            # published_parsed may be None
            dt_struct = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
            if not dt_struct:
                continue
            dt = datetime(*dt_struct[:6], tzinfo=timezone.utc)
            if dt < cutoff: 
                continue

            title = e.get("title", "")
            summary = re.sub("<.*?>"," ", e.get("summary",""))
            txt = f"{title} — {summary}"

            if not looks_like_sell(txt): 
                continue
            if not is_personish(txt):
                continue

            amt = parse_amount_nis(txt)
            if amt < MIN_NIS:
                pass  # keep even if amount not detected; many reports lack explicit NIS sum

            rows.append({
                "when_utc": dt.isoformat(),
                "title": title.strip(),
                "summary": summary.strip(),
                "link": e.get("link",""),
                "amount_nis_guess": round(amt,2),
            })

    df = pd.DataFrame(rows).sort_values("when_utc") if rows else pd.DataFrame(columns=["when_utc","title","summary","link","amount_nis_guess"])
    df.to_csv("tase_trades.csv", index=False)

    # Alerts = same table for now (we can add grouping/thresholds later)
    df.to_csv("tase_alerts.csv", index=False)

    if not df.empty:
        html = df.to_html(index=False, escape=False)
        send_email(subject=f"TASE insider sells — {len(df)} hits", html_body=html)
        print(f"Emailed {len(df)} rows.", file=log)
    else:
        print("No TASE sells matched this window.", file=log)

    open("tase.log","w").write(log.getvalue())

if __name__ == "__main__":
    main()

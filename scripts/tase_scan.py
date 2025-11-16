#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, time, csv, ssl, smtplib, math
from email.mime.text import MIMEText
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# -------- settings (envs with sane defaults) --------
MAIL_USER     = os.getenv("MAIL_USERNAME")
MAIL_PASS     = os.getenv("MAIL_PASSWORD")
MAIL_FROM     = os.getenv("FROM_EMAIL", MAIL_USER)
MAIL_TO       = os.getenv("TO_EMAIL", MAIL_USER)
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))

# how many ids to probe each run
SCAN_AHEAD    = int(os.getenv("TASE_SCAN_AHEAD", "500"))
# seed last seen id (will be overwritten by state file once created)
SEED_LAST_ID  = int(os.getenv("TASE_LAST_ID", "1700000"))

OUT_TRADES = "tase_trades.csv"
OUT_ALERTS = "tase_alerts.csv"
STATE_FILE = ".tase_state.txt"

ENTITY_PAT = re.compile(r"(בע.?מ|החזקות|שותפות|קרן|Holdings|Capital|Partners|Ltd|Inc|LLC)", re.I)
SELL_PAT   = re.compile(r"(קיטון|מכירה)", re.U)
NIS_IN_AG  = 0.01  # price is often in אג'

HEADERS = {
    "User-Agent": "TASE-InsiderWatch/1.0 (contact: alerts@nowhere.example)",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

def bucket_for(report_id: int) -> str:
    start = ((report_id - 1)//1000)*1000 + 1
    end   = start + 999
    return f"{start}-{end}"

def htm_url(report_id: int) -> str:
    b = bucket_for(report_id)
    return f"https://mayafiles.tase.co.il/rhtm/{b}/H{report_id}.htm"

def fetch(url: str, tries=3, sleep=1.2):
    last = None
    for i in range(tries):
        r = requests.get(url, headers=HEADERS, timeout=30)
        last = r
        if r.status_code == 200:
            # pages are Hebrew; let BeautifulSoup handle decoding
            return r.content
        if r.status_code == 404:
            raise FileNotFoundError()
        time.sleep(sleep*(i+1))
    raise RuntimeError(f"HTTP {last.status_code} for {url}")

def text(el):
    return (el.get_text(" ", strip=True) if el else "").strip()

def parse_report(html_bytes, report_id):
    soup = BeautifulSoup(html_bytes, "lxml")

    # quick guard: look for the common headline container text
    doc_text = soup.get_text(" ", strip=True)
    if "החזקות בעלי עניין" not in doc_text and "שינוי החזקות בעלי עניין" not in doc_text:
        return None

    # symbol (if present)
    symbol = None
    for m in re.finditer(r"מספר נייר ערך בבורסה[:\s]+(\d+)", doc_text):
        symbol = m.group(1)
        break

    # holder name
    holder = None
    for lab in ("שם תאגיד/שם משפחה ושם פרטי", "שם משפחה ושם פרטי", "שם בעל העניין"):
        m = re.search(lab + r".{0,10}[:\s]+([^\n]+)", doc_text)
        if m:
            holder = m.group(1).strip()
            break

    if not holder or ENTITY_PAT.search(holder):
        return None  # ignore funds/entities

    # change type → must be sell/decrease
    change = None
    for lab in ("מהות השינוי",):
        m = re.search(lab + r".{0,10}[:\s]+([^\n]+)", doc_text)
        if m:
            change = m.group(1).strip()
            break
    if not change or not SELL_PAT.search(change):
        return None

    # quantity delta (negative for sell)
    qty = None
    m = re.search(r"שינוי בכמות ניירות הערך[:\s]+([\-–]?\d[\d,\.]*)", doc_text)
    if m:
        qty = m.group(1).replace(",", "")
        try:
            qty = float(qty)
        except:
            qty = None
    if not qty:
        return None

    # price (אגורות → ₪)
    price_nis = None
    m = re.search(r"שער העסקה[:\s]+([\d\.]+)\s*מטבע\s*אג", doc_text)
    if m:
        try:
            price_nis = float(m.group(1)) * NIS_IN_AG
        except:
            price_nis = None

    # date
    date_str = None
    m = re.search(r"תאריך השינוי[:\s]+(\d{2}/\d{2}/\d{4})", doc_text)
    if m:
        date_str = m.group(1)

    amount_nis = None
    if price_nis is not None and qty is not None:
        amount_nis = abs(qty) * price_nis

    return {
        "report_id": report_id,
        "symbol": symbol,
        "holder": holder,
        "change": change,
        "qty_delta": qty,
        "price_nis": price_nis,
        "amount_nis": amount_nis,
        "date": date_str,
        "print_url": htm_url(report_id),
    }

def read_last_id():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return int(f.read().strip())
        except:
            pass
    return SEED_LAST_ID

def write_last_id(n):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(n))

def append_trades(rows):
    new_file = not os.path.exists(OUT_TRADES)
    with open(OUT_TRADES, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["report_id","symbol","holder","change","qty_delta","price_nis","amount_nis","date","print_url"])
        for r in rows:
            w.writerow([r["report_id"], r["symbol"], r["holder"], r["change"], r["qty_delta"],
                        r["price_nis"], r["amount_nis"], r["date"], r["print_url"]])

def send_email(subject, body):
    if not (MAIL_USER and MAIL_PASS and MAIL_FROM and MAIL_TO):
        print("[WARN] Missing mail envs; skipping email.")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"]   = MAIL_TO
    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls(context=ctx)
        s.login(MAIL_USER, MAIL_PASS)
        s.sendmail(MAIL_FROM, [MAIL_TO], msg.as_string())

def main():
    last = read_last_id()
    hi = last + SCAN_AHEAD
    found = []

    current = last + 1
    while current <= hi:
        url = htm_url(current)
        try:
            html = fetch(url)
        except FileNotFoundError:
            current += 1
            continue
        except Exception as e:
            print(f"[WARN] {e} @ {url}")
            current += 1
            time.sleep(0.5)
            continue

        row = parse_report(html, current)
        if row and row.get("amount_nis") and row["amount_nis"] > 0:
            found.append(row)
        current += 1
        time.sleep(1.0)  # be polite

    # update state
    write_last_id(hi)

    # nothing? still write empty alerts file
    if not found:
        with open(OUT_ALERTS, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["symbol","holders","total_nis","last_when"])
        print("No qualifying TASE insider sells in this scan window.")
        return

    # group by symbol
    by_sym = {}
    for r in found:
        sym = r["symbol"] or "—"
        if sym not in by_sym:
            by_sym[sym] = {"total":0.0, "holders":set(), "rows":[]}
        by_sym[sym]["rows"].append(r)
        by_sym[sym]["total"] += (r["amount_nis"] or 0.0)
        if r["holder"]:
            by_sym[sym]["holders"].add(r["holder"])

    # write trades and alerts
    append_trades(found)
    with open(OUT_ALERTS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["symbol","holders","total_nis","last_when"])
        for sym, agg in sorted(by_sym.items(), key=lambda kv: -kv[1]["total"]):
            w.writerow([sym, len(agg["holders"]), f"{agg['total']:.0f}", datetime.now(timezone.utc).isoformat()])

    # email digest
    lines = []
    for sym, agg in sorted(by_sym.items(), key=lambda kv: -kv[1]["total"]):
        lines.append(f"{sym}: ₪{agg['total']:,.0f} across {len(agg['holders'])} insider(s)")
        for r in sorted(agg["rows"], key=lambda x: -x["amount_nis"]):
            lines.append(f"  - {r['holder']}: {int(abs(r['qty_delta'])):,} @ ₪{(r['price_nis'] or 0):.2f} = ₪{(r['amount_nis'] or 0):,.0f}  (תאריך: {r['date']})")
            lines.append(f"    {r['print_url']}")
        lines.append("")
    body = "TASE Insider SELLs — latest scan\n\n" + "\n".join(lines)
    subject = f"[InsiderWatch] TASE SELL digest — {len(found)} trade(s), {len(by_sym)} tickers"
    send_email(subject, body)
    print(body)

if __name__ == "__main__":
    main()

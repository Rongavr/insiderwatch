#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import ssl
import csv
import time
import smtplib
import traceback
from email.mime.text import MIMEText
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

# ---------- Config via env ----------
FROM_EMAIL    = os.getenv("FROM_EMAIL")
MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
TO_EMAIL      = os.getenv("TO_EMAIL", FROM_EMAIL)

# Comma-separated list of MAYA report links OR search/list pages that contain links
# Example item: https://maya.tase.co.il/he/reports/1702671?attachmentType=htm
TASE_RSS_URLS = [u.strip() for u in (os.getenv("TASE_RSS_URLS", "") or "").split(",") if u.strip()]

# Floor for reporting (in NIS). Start with 0; raise later if too noisy.
MIN_NIS = float(os.getenv("MIN_NIS", "0"))

# Outputs
OUT_ALERTS = "tase_alerts.csv"   # per-run summary
OUT_TRADES = "tase_trades.csv"   # detailed rows
OUT_LOG    = "tase.log"

HEADERS = {
    "User-Agent": "TASE-InsiderWatch/1.0 (+mail:{})".format(MAIL_USERNAME or "unknown"),
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# ---------- Helpers ----------

def log(msg: str):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(OUT_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def fetch(url: str, is_html: bool = True, tries: int = 3, sleep_sec: float = 1.2) -> str:
    last = None
    for i in range(tries):
        r = requests.get(url, headers=HEADERS, timeout=30)
        last = r
        if r.status_code == 200:
            return r.text if is_html else r.content
        time.sleep(sleep_sec * (i + 1))
    raise RuntimeError(f"HTTP {last.status_code if last else '??'} for {url}")

def normalize_report_url(url: str) -> str:
    """
    Ensure we hit the HTML attachment page (not PDF),
    i.e. add ?attachmentType=htm when missing.
    """
    try:
        pr = urlparse(url)
        if "/reports/" in pr.path and "attachmentType" not in (parse_qs(pr.query) or {}):
            sep = "&" if pr.query else "?"
            return url + f"{sep}attachmentType=htm"
    except Exception:
        pass
    return url

def discover_report_links(container_url: str) -> list:
    """
    If the provided URL is a listing/search page, extract links to individual reports.
    If it's already a /reports/<id> link, just return that.
    """
    u = container_url.strip()
    if "/reports/" in u:
        return [normalize_report_url(u)]

    # Otherwise, scrape all /he/reports/<id> links on the page and normalize them.
    try:
        html = fetch(u, is_html=True)
    except Exception as e:
        log(f"fetch list failed: {u} :: {e}")
        return []
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/reports/" in href:
            full = urljoin(u, href)
            links.append(normalize_report_url(full))
    # De-dup, keep order
    seen = set()
    out = []
    for link in links:
        if link not in seen:
            seen.add(link)
            out.append(link)
    return out

_HEB_NUM = re.compile(r"[0-9,\.\-]+")
def _to_num(s):
    if s is None:
        return None
    m = _HEB_NUM.search(s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except Exception:
        return None

DATE_PAT = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})")

def _find_after(label, text):
    """
    Find the Hebrew label and return the rest of the line after it.
    Tries a few common label variants.
    """
    variants = [
        label,
        label.replace(":", " :"),
        label.replace(":", ""),
    ]
    for v in variants:
        idx = text.find(v)
        if idx >= 0:
            # take rest of that line
            line = text[idx:].splitlines()[0]
            return line[len(v):].strip(" :\u200f\u200e")
    return None

def parse_hebrew_report(html: str, url: str) -> list:
    """
    Extract events from a single MAYA 'attachmentType=htm' report page.
    Returns a list of dicts with keys:
      kind: 'sell' | 'ceased'
      company, paper_number, holder, change_shares, price_agorot, price_nis, amount_nis, txn_date, url, raw_title
    """
    soup = BeautifulSoup(html, "lxml")
    # Flatten to text for robust regex
    text = soup.get_text("\n", strip=True)

    events = []

    # Company (prefer "שם מקוצר:"; fallback to first header-looking line)
    company = None
    short = _find_after("שם מקוצר:", text)
    if short:
        company = short.split()[0] if short else None
        company = short
    if not company:
        # crude fallback: first line ending with בע"מ / בע״מ or company name block
        lines = text.splitlines()
        for ln in lines[:20]:
            if "בע\"מ" in ln or "בע״מ" in ln:
                company = ln.strip()
                break

    # Paper number
    paper_number = None
    m = re.search(r"מספר נייר(?:\s+ערך)?(?:\s+בבורסה)?\s*:\s*([0-9]+)", text)
    if m:
        paper_number = m.group(1)

    # Title
    raw_title = None
    # try to find a strong clue near the top
    for ln in text.splitlines()[:40]:
        if "דוח מיידי" in ln or "שינוי החזקות" in ln or "חדל להיות בעל עניין" in ln:
            raw_title = ln.strip()
            break

    # Transaction date
    txn_date = None
    z = _find_after("תאריך השינוי:", text)
    if not z:
        # sometimes it's "תאריך ביצוע הפעולה"
        z = _find_after("תאריך ביצוע הפעולה", text)
    if z:
        dm = DATE_PAT.search(z)
        if dm:
            txn_date = dm.group(1)

    # Holder (person or entity)
    holder = None
    holder = _find_after("שם תאגיד/שם משפחה ושם פרטי של המחזיק:", text) or \
             _find_after("שם משפחה/שם תאגיד:", text) or \
             _find_after("שם המחזיק:", text)
    if holder:
        holder = holder.strip("_ ").strip()

    # Change in quantity (negative means sell)
    delta_shares = None
    ds = _find_after("שינוי בכמות ניירות הערך:", text)
    if ds is not None:
        delta_shares = _to_num(ds)

    # Price (agorot → NIS)
    price_agorot = None
    pa = _find_after("שער העסקה:", text)
    if pa is not None:
        price_agorot = _to_num(pa)
    price_nis = (price_agorot / 100.0) if price_agorot is not None else None

    # Amount (approx)
    amount_nis = None
    if delta_shares is not None and price_nis is not None:
        amount_nis = abs(delta_shares) * price_nis

    # Nature of change
    mehut = _find_after("מהות השינוי:", text)
    if mehut:
        mehut = mehut.strip("_ ").strip()

    # Kind detection
    is_ceased = ("חדל להיות בעל ענין" in text) or ("חדל להיות בעל עניין" in text)
    is_change_holdings = ("שינוי החזקות בעלי עניין" in text) or ("דוח מיידי על שינויים בהחזקות" in text)

    # Build events
    if is_ceased:
        events.append({
            "kind": "ceased",
            "company": company,
            "paper_number": paper_number,
            "holder": holder,
            "change_shares": None,
            "price_agorot": None,
            "price_nis": None,
            "amount_nis": None,
            "txn_date": txn_date,
            "url": url,
            "raw_title": raw_title or "חדל להיות בעל עניין",
            "mehuts": mehut,
        })
    elif is_change_holdings or delta_shares is not None or mehut is not None:
        # Treat negative delta or a mehut that indicates sell ("קיטון", "מכירה")
        looks_sell = False
        if delta_shares is not None and delta_shares < 0:
            looks_sell = True
        if mehut:
            if ("קיטון" in mehut) or ("מכירה" in mehut) or ("מכר" in mehut):
                looks_sell = True
        if looks_sell:
            # Respect minimum amount if calculable; if price or delta missing, still include with amount None
            if (amount_nis is None) or (amount_nis >= MIN_NIS):
                events.append({
                    "kind": "sell",
                    "company": company,
                    "paper_number": paper_number,
                    "holder": holder,
                    "change_shares": delta_shares,
                    "price_agorot": price_agorot,
                    "price_nis": price_nis,
                    "amount_nis": amount_nis,
                    "txn_date": txn_date,
                    "url": url,
                    "raw_title": raw_title or "שינוי החזקות (מכירה/קיטון)",
                    "mehuts": mehut,
                })
    else:
        # Not a holdings/ceased item we care about
        pass

    return events

def send_email(subject: str, body: str):
    if not (MAIL_USERNAME and MAIL_PASSWORD and FROM_EMAIL and TO_EMAIL):
        log("[WARN] Missing mail credentials; skip email.")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"]    = FROM_EMAIL
    msg["To"]      = TO_EMAIL
    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
        s.starttls(context=ctx)
        s.login(MAIL_USERNAME, MAIL_PASSWORD)
        s.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())

def write_csv(path: str, rows: list, header: list):
    # Always write (even if empty) so artifacts show up
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in header})

def main():
    start_ts = datetime.now(timezone.utc).isoformat()
    all_links = []
    for base in TASE_RSS_URLS:
        all_links.extend(discover_report_links(base))

    # De-dup
    seen = set()
    links = []
    for u in all_links:
        if u not in seen:
            seen.add(u)
            links.append(u)

    log(f"Scanning {len(links)} MAYA report page(s)")

    all_events = []
    for link in links:
        try:
            u = normalize_report_url(link)
            html = fetch(u, is_html=True)
            evs = parse_hebrew_report(html, u)
            if evs:
                all_events.extend(evs)
        except Exception as e:
            log(f"parse failed: {link} :: {e}")
            traceback.print_exc()

    # Split by kind
    sells   = [e for e in all_events if e["kind"] == "sell"]
    ceaseds = [e for e in all_events if e["kind"] == "ceased"]

    # Build digest text
    lines = []
    lines.append(f"TASE insider digest — {start_ts}")
    lines.append(f"Links scanned: {len(links)}  •  MIN_NIS={MIN_NIS:g}")
    lines.append("")

    if sells:
        lines.append("SELLS:")
        # Group by company for readability
        by_co = {}
        for e in sells:
            by_co.setdefault(e.get("company") or "?", []).append(e)
        for co, rows in by_co.items():
            total = sum([r["amount_nis"] or 0 for r in rows])
            lines.append(f"{co} — {len(rows)} sale(s), total ₪{total:,.0f}")
            for r in rows:
                holder = r.get("holder") or "לא צוין"
                sh = r.get("change_shares")
                px = r.get("price_nis")
                amt = r.get("amount_nis")
                when = r.get("txn_date") or "תאריך לא צוין"
                parts = []
                if sh is not None:
                    parts.append(f"{int(abs(sh)):,} מניות")
                if px is not None:
                    parts.append(f"@ ₪{px:,.2f}")
                if amt is not None:
                    parts.append(f"= ₪{amt:,.0f}")
                lines.append(f"  • {holder}: " + " ".join(parts) + f"  ({when})")
                lines.append(f"    קישור לדיווח: {r['url']}")
        lines.append("")

    if ceaseds:
        lines.append("CEASED TO BE INTERESTED PARTY:")
        for e in ceaseds:
            co = e.get("company") or "?"
            holder = e.get("holder") or "לא צוין"
            when = e.get("txn_date") or "תאריך לא צוין"
            lines.append(f"  • {co}: {holder} — ceased ( {when} )")
            lines.append(f"    קישור לדיווח: {e['url']}")
        lines.append("")

    if not sells and not ceaseds:
        lines.append("No relevant TASE insider items found in these links.")

    body = "\n".join(lines)
    subject = f"[TASE Insider] {len(sells)} sells, {len(ceaseds)} ceased — {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    # Persist CSVs (for artifacts)
    # alerts: one row per company for sells; plus each ceased row
    alerts_rows = []
    by_co = {}
    for e in sells:
        co = e.get("company") or "?"
        by_co.setdefault(co, {"company": co, "sell_rows": [], "total_nis": 0.0})
        by_co[co]["sell_rows"].append(e)
        by_co[co]["total_nis"] += (e["amount_nis"] or 0)
    for co, agg in by_co.items():
        alerts_rows.append({
            "company": co,
            "kind": "sell",
            "items": len(agg["sell_rows"]),
            "total_nis": round(agg["total_nis"], 2),
            "generated_at": start_ts
        })
    for e in ceaseds:
        alerts_rows.append({
            "company": e.get("company") or "?",
            "kind": "ceased",
            "items": 1,
            "total_nis": "",
            "generated_at": start_ts
        })

    write_csv(
        OUT_ALERTS,
        alerts_rows,
        header=["company", "kind", "items", "total_nis", "generated_at"]
    )

    write_csv(
        OUT_TRADES,
        sells,
        header=[
            "kind","company","paper_number","holder",
            "change_shares","price_agorot","price_nis","amount_nis",
            "txn_date","url","raw_title","mehuts"
        ]
    )

    # Email
    try:
        send_email(subject, body)
        log("Email sent.")
    except Exception as e:
        log(f"email failed: {e}")

if __name__ == "__main__":
    main()


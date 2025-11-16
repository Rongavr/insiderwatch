#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TASE insider SELL notifier
- Mode A (links provided): parse specific Maya report/print links from env TASE_LINKS
- Mode B (auto-scan): probe a moving window of Maya "print" ids (H<ID>.htm/.pdf)

Outputs (in repo workspace):
  - tase_trades.csv  (append-only rows of parsed sells)
  - tase_alerts.csv  (per-run digest summary)
  - .tase_state.txt  (last scanned id; best-effort if using Mode B)

Email:
  Uses FROM_EMAIL / MAIL_USERNAME / MAIL_PASSWORD / SMTP_SERVER / SMTP_PORT / TO_EMAIL
"""
import os, re, time, io, ssl, smtplib, sys
from email.mime.text import MIMEText
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text
import pandas as pd

# ----------- config from env -----------
FROM_EMAIL    = os.getenv("FROM_EMAIL")
MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
SMTP_SERVER   = os.getenv("SMTP_SERVER", "smtp.gmail.com")
try:
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
except:
    SMTP_PORT = 587
TO_EMAIL      = os.getenv("TO_EMAIL", MAIL_USERNAME or FROM_EMAIL)

TASE_LINKS    = os.getenv("TASE_LINKS", "").strip()  # space/newline separated; if empty → Mode B
STATE_FILE    = ".tase_state.txt"
LAST_ID_SEED  = int(os.getenv("TASE_LAST_ID", "1702000"))
SCAN_AHEAD    = int(os.getenv("TASE_SCAN_AHEAD", "400"))  # how many ids to probe per run
SLEEP_SEC     = float(os.getenv("TASE_SLEEP", "1.0"))     # politeness delay

# Heuristics
HEBREW_ENTITY_WORDS = r'(בע"?מ|בע״מ|חברה|שותפות|קרן|החזקות|נאמנות|בע"מ)'
SELL_MARKERS = [
    "קיטון עקב מכירה", "קיטון  עקב  מכירה", "מכירה בבורסה", "מכר", "מכרה",
    "מכירה", "מכירות", "שינוי בכמות ניירות הערך", "חדל להיות בעל ענין"
]

# ----------- utils -----------
UA = {"User-Agent": "TASE-Insider-Scan/1.0 (+mailto:%s)" % (TO_EMAIL or "example@example.com")}

def http_get(url: str, as_bytes=False, tries=3, backoff=1.4) -> str | bytes:
    last = None
    for i in range(tries):
        r = requests.get(url, headers=UA, timeout=30)
        last = r
        if r.status_code == 200:
            return r.content if as_bytes else r.text
        time.sleep(backoff*(i+1))
    raise RuntimeError(f"HTTP {last.status_code if last else '??'} for {url}")

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    # remove script/style
    for t in soup(["script", "style", "noscript"]):
        t.extract()
    return soup.get_text(" ", strip=True)

def pdf_bytes_to_text(data: bytes) -> str:
    with io.BytesIO(data) as bio:
        return extract_text(bio)

def normalize_num(txt: str) -> Optional[float]:
    if not txt: return None
    t = txt.replace('\u200f','').replace('\u200e','').replace(',', '')
    t = t.replace('\u2212','-').replace('−','-')  # minus variants
    m = re.search(r'-?\d+(?:\.\d+)?', t)
    if not m: return None
    try:
        return float(m.group(0))
    except:
        return None

def looks_like_entity(name: str) -> bool:
    if not name: return False
    return bool(re.search(HEBREW_ENTITY_WORDS, name))

def send_email(subject: str, body: str):
    if not (MAIL_USERNAME and MAIL_PASSWORD and FROM_EMAIL and TO_EMAIL):
        print("[WARN] email creds missing; printing digest instead")
        print(subject)
        print(body)
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

# ----------- parsing -----------
def parse_tase_report_text(text: str, source_url: str) -> List[Dict]:
    """
    Very tolerant heuristic parser for TASE 'change in holdings' reports.
    Returns a list of rows dicts for sells we could detect.
    """
    # only react if we see sell markers
    if not any(marker in text for marker in SELL_MARKERS):
        return []

    # basic fields
    # holder (שם המחזיק)
    holder = None
    m = re.search(r'שם\s+תאגיד/שם\s+משפחה\s+ושם\s+פרטי\s+של\s+המחזיק:\s*([^\n\r]+)', text)
    if not m:
        # fallback: "שם המחזיק"
        m = re.search(r'שם\s+המחזיק:\s*([^\n\r]+)', text)
    if m:
        holder = m.group(1).strip()

    # company (שם מקוצר / שם חברה בעמ)
    company = None
    m = re.search(r'שם\s+מקוצר:\s*([^\n\r]+)', text)
    if m: company = m.group(1).strip()
    if not company:
        m = re.search(r'שם\s+החברה[:\s]+([^\n\r]+)', text)
        if m: company = m.group(1).strip()

    # tase code (מספר נייר ערך בבורסה)
    tase_code = None
    m = re.search(r'מספר\s+נייר\s+ערך\s+בבורסה:\s*([0-9]+)', text)
    if m: tase_code = m.group(1)

    # change quantity (negative means sell)
    change_qty = None
    m = re.search(r'שינוי\s+בכמות\s+ניירות\s+הערך:\s*([\-–−]?\s*[\d,]+)', text)
    if m:
        change_qty = normalize_num(m.group(1))
    else:
        # sometimes text narrates "קיטון" without explicit number; try to infer minus
        if "קיטון" in text:
            change_qty = -0.0  # sentinel for unknown negative

    # trade date
    trade_date = None
    m = re.search(r'תאריך\s+השינוי:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})', text)
    if m: trade_date = m.group(1)

    # price & currency
    price_nis = None
    m = re.search(r'שער\s+העסקה:\s*([\d\.]+)\s*מטבע\s*(אג\'|אג”|אג״|ש\"ח|ש"ח|שח)', text)
    if m:
        p = normalize_num(m.group(1))
        unit = m.group(2)
        if p is not None:
            price_nis = p/100.0 if "אג" in unit else p

    # decide if this is a sell
    is_sell_context = any(marker in text for marker in SELL_MARKERS) or (change_qty is not None and change_qty < 0)
    if not is_sell_context:
        return []

    # amount estimate (if possible)
    amount_nis = None
    if (change_qty is not None) and (price_nis is not None):
        amount_nis = abs(change_qty) * price_nis

    # filter out obvious entities if we have a holder field with “בע"מ/קרן/חברה”
    people_only = False  # you can flip this default if you want people-only
    if people_only and holder and looks_like_entity(holder):
        return []

    row = {
        "company": company or "",
        "tase_code": tase_code or "",
        "holder": holder or "",
        "change_qty": change_qty if change_qty is not None else "",
        "price_nis": price_nis if price_nis is not None else "",
        "amount_nis": amount_nis if amount_nis is not None else "",
        "trade_date": trade_date or "",
        "source_url": source_url,
    }
    return [row]

def find_print_links_from_report(url: str) -> List[str]:
    """Given https://maya.tase.co.il/he/reports/<ID>, return any mayafiles print links found."""
    try:
        html = http_get(url)
    except Exception as e:
        print(f"[WARN] fetch report page failed {url}: {e}")
        return []
    links = re.findall(r'https://mayafiles\.tase\.co\.il/[^\s"\']+H[0-9]+\.htm', html, flags=re.I)
    links += re.findall(r'https://mayafiles\.tase\.co\.il/[^\s"\']+H[0-9]+\.pdf', html, flags=re.I)
    # de-dup preserve order
    seen, out = set(), []
    for L in links:
        if L not in seen:
            seen.add(L); out.append(L)
    return out

def fetch_and_parse_maya(url: str) -> List[Dict]:
    """Fetch either .htm or .pdf maya print page and parse text."""
    rows: List[Dict] = []
    try:
        if url.lower().endswith(".pdf"):
            data = http_get(url, as_bytes=True)
            text = pdf_bytes_to_text(data)
            rows = parse_tase_report_text(text, url)
        elif url.lower().endswith(".htm") or url.lower().endswith(".html"):
            html = http_get(url)
            text = html_to_text(html)
            rows = parse_tase_report_text(text, url)
        elif "maya.tase.co.il/he/reports/" in url:
            # resolve to print links
            for L in find_print_links_from_report(url):
                rows.extend(fetch_and_parse_maya(L))
        else:
            # unknown; try as html
            html = http_get(url)
            text = html_to_text(html)
            rows = parse_tase_report_text(text, url)
    except Exception as e:
        print(f"[WARN] failed parse {url}: {e}")
    return rows

# ----------- Mode A / Mode B -----------
def mode_a_links() -> List[str]:
    # split on whitespace/newlines
    raw = TASE_LINKS.strip()
    if not raw:
        return []
    # normalize commas/newlines to spaces
    raw = raw.replace(",", " ")
    parts = [p for p in raw.split() if p.strip()]
    norm: List[str] = []
    for p in parts:
        u = p.strip()
        # if it's maya report page, keep; if it's mayafiles print link, keep as is
        norm.append(u)
    return norm

def load_state() -> int:
    if os.path.exists(STATE_FILE):
        try:
            return int(open(STATE_FILE,"r",encoding="utf-8").read().strip())
        except:
            pass
    return LAST_ID_SEED

def save_state(last_id: int):
    try:
        with open(STATE_FILE,"w",encoding="utf-8") as f:
            f.write(str(last_id))
    except Exception as e:
        print(f"[WARN] failed writing state: {e}")

def mode_b_probe(start_id: int, ahead: int) -> Tuple[List[str], int]:
    """
    Probe H<id>.htm and H<id>.pdf for id in [start_id+1, start_id+ahead]
    Return (links_found, new_last_id)
    """
    found: List[str] = []
    new_last = start_id
    base = "https://mayafiles.tase.co.il/rhtm"
    for i in range(start_id + 1, start_id + ahead + 1):
        # files are split into 1000-block directories e.g. 1702001-1703000
        block_start = (i // 1000) * 1000 + 1
        block_end   = block_start + 999
        prefix = f"{base}/{block_start}-{block_end}/H{i}"
        for ext in (".htm", ".pdf"):
            url = f"{prefix}{ext}"
            try:
                r = requests.head(url, headers=UA, timeout=15, allow_redirects=True)
                if r.status_code == 200:
                    found.append(url)
                    print(f"[+] found {url}")
                    break  # prefer first existing (.htm over .pdf)
            except Exception:
                pass
            time.sleep(SLEEP_SEC/4)
        new_last = i
        time.sleep(SLEEP_SEC)
    return found, new_last

# ----------- main -----------
def main():
    now = datetime.now(timezone.utc).isoformat()
    links = mode_a_links()
    scanned_links: List[str] = []

    if links:
        # Mode A — user-provided links (report pages or print pages)
        for u in links:
            if "maya.tase.co.il/he/reports/" in u:
                # expand to print links on that page
                prints = find_print_links_from_report(u)
                if not prints:
                    print(f"[WARN] no print links on {u}")
                scanned_links.extend(prints or [u])
            else:
                scanned_links.append(u)
    else:
        # Mode B — probe a block of print ids
        last = load_state()
        prints, new_last = mode_b_probe(last, SCAN_AHEAD)
        save_state(new_last)
        scanned_links = prints

    # parse each link for sells
    all_rows: List[Dict] = []
    for u in scanned_links:
        rows = fetch_and_parse_maya(u)
        if rows:
            all_rows.extend(rows)

    # persist trades (append) + run alerts summary
    trades_path = "tase_trades.csv"
    alerts_path = "tase_alerts.csv"

    if os.path.exists(trades_path):
        try:
            trades_df = pd.read_csv(trades_path)
        except Exception:
            trades_df = pd.DataFrame()
    else:
        trades_df = pd.DataFrame()

    run_df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=[
        "company","tase_code","holder","change_qty","price_nis","amount_nis","trade_date","source_url"
    ])

    if not run_df.empty:
        # append & drop exact duplicates (by all cols)
        combined = pd.concat([trades_df, run_df], ignore_index=True)
        combined = combined.drop_duplicates()
        combined.to_csv(trades_path, index=False)

        # per-run digest (group by company/code)
        grp = (run_df.groupby(["company","tase_code"], dropna=False)
                      .agg(trades=("holder","count"),
                           est_total_nis=("amount_nis","sum"))
                      .reset_index())
        grp["when"] = now
        grp.to_csv(alerts_path, index=False)

        # email digest
        lines = []
        lines.append(f"TASE insider SELL digest — {len(run_df)} trade(s), {grp.shape[0]} ticker(s)")
        for _, r in grp.sort_values("est_total_nis", ascending=False).iterrows():
            tot = r["est_total_nis"]
            lines.append(f"{(r['company'] or '').strip()} ({str(r['tase_code'] or '').strip()}): {int(r['trades'])} trade(s){' — ~₪{:,.0f}'.format(tot) if pd.notna(tot) else ''}")
            # add details per ticker
            sub = run_df[(run_df["company"]==r["company"]) & (run_df["tase_code"]==r["tase_code"])]
            for _, rr in sub.iterrows():
                holder = (rr.get("holder") or "").strip() or "—"
                qty = rr.get("change_qty")
                price = rr.get("price_nis")
                amt = rr.get("amount_nis")
                date = rr.get("trade_date") or "—"
                lines.append(f"  • {holder}  Δ{int(qty) if pd.notna(qty) and qty==qty else '—'} @ ₪{price:,.2f} ≈ ₪{amt:,.0f} on {date}" if pd.notna(price) and pd.notna(amt) else
                             f"  • {holder}  Δ{int(qty) if pd.notna(qty) and qty==qty else '—'} on {date}")
                lines.append(f"    {rr.get('source_url')}")
            lines.append("")
        subject = f"[InsiderWatch] TASE SELL digest — {len(run_df)} trade(s)"
        send_email(subject, "\n".join(lines))
        print(subject)
    else:
        # still write empty for artifact visibility
        pd.DataFrame(columns=["company","tase_code","trades","est_total_nis","when"]).to_csv(alerts_path, index=False)
        print("No qualifying TASE sells parsed in this run.")

if __name__ == "__main__":
    main()

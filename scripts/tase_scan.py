# scripts/tase_scan.py
# TASE scanner that accepts:
#  - TASE_LINKS: space/newline-separated list of MAYA pages (.htm wrapper OR company report page) or direct .pdf
#  - If TASE_LINKS is "", auto-scan print-page ids starting from TASE_LAST_ID (state saved in .tase_state.txt)
#
# Outputs:
#   tase_trades.csv  (one row per parsed sell)
#   tase_alerts.csv  (grouped digest by company/code)
#
# Emails are handled by the workflow's mail step (already set up).

import os, re, sys, time
from io import BytesIO
from urllib.parse import urljoin
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pdfminer.high_level import extract_text

UA = "TaseSellScanner/1.0 (+github actions)"
HEADERS = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate", "Connection": "keep-alive"}

# ---------- helpers ----------
def fetch(url, binary=False, tries=3, sleep=1.2):
    last = None
    for i in range(tries):
        r = requests.get(url, headers=HEADERS, timeout=30)
        last = r
        if r.status_code == 200:
            return r.content if binary else r.text
        time.sleep(sleep * (i+1))
    raise RuntimeError(f"HTTP {last.status_code} for {url}")

PDF_HINT_PAT = re.compile(r'(?i)\.pdf(?:[#?\s"\']|$)')
MAYAFILES_BASE = "https://mayafiles.tase.co.il/"

def find_pdf_from_html_page(url, html):
    # Common places the PDF lives inside MAYA wrapper pages
    # 1) <a href="...pdf">, <embed src="...pdf">, <iframe src="...pdf">
    soup = BeautifulSoup(html, "lxml")
    # All attributes that can carry the pdf link
    cand_attrs = []
    for tag in soup.find_all(["a", "embed", "iframe"]):
        for attr in ["href", "src"]:
            v = tag.get(attr)
            if v and PDF_HINT_PAT.search(v):
                cand_attrs.append(v)
    # Fallback: search in raw html
    if not cand_attrs:
        for m in re.finditer(r'href=["\']([^"\']+?\.pdf[^"\']*)["\']', html, flags=re.I):
            cand_attrs.append(m.group(1))
    # Normalize & prefer mayafiles
    cands = []
    for c in cand_attrs:
        cands.append(urljoin(url, c))
    # Deduplicate, keep mayafiles first
    seen, ordered = set(), []
    for c in sorted(cands, key=lambda u: (not u.startswith(MAYAFILES_BASE), len(u))):
        if c not in seen:
            seen.add(c); ordered.append(c)
    return ordered[0] if ordered else None

def resolve_to_pdf(url):
    """Accepts:
        - company report page like https://maya.tase.co.il/he/reports/1702640
        - mayafiles wrapper like  .../H1702656.htm
        - direct .pdf
       Returns (pdf_url, pdf_bytes)
    """
    if PDF_HINT_PAT.search(url):
        return url, fetch(url, binary=True)

    # HTML wrapper
    html = fetch(url, binary=False)
    pdf_url = find_pdf_from_html_page(url, html)
    if not pdf_url:
        # Some company pages use attachmentType=pdf links
        # Try to synthesize if we see attachmentType in the html
        m = re.search(r'(https?://[^\s"\']+?attachmentType=pdf[^\s"\']*)', html, flags=re.I)
        if m:
            pdf_url = m.group(1)
    if not pdf_url:
        raise RuntimeError(f"Could not find a PDF link inside: {url}")
    return pdf_url, fetch(pdf_url, binary=True)

HEB_DIGITS = re.compile(r'[\d,]+')
def _to_int(s):
    if not s: return None
    s = s.replace(',', '').strip()
    if not s: return None
    try: return int(s)
    except: 
        # negative like "23,571 -" or "-23,571"
        s = s.replace(' -', '').replace('-', '')
        try: return int(s)
        except: return None

def _to_float(s):
    if not s: return None
    s = s.replace(',', '').strip()
    try: return float(s)
    except: return None

def parse_pdf(pdf_bytes, src_url, pdf_url):
    """Return list of trade dicts for sells found in the PDF."""
    text = extract_text(BytesIO(pdf_bytes)) or ""
    # Normalize spaces
    t = re.sub(r'[ \t]+', ' ', text)
    # Company (try a few hints)
    company = None
    for pat in [
        r'שם מקוצר:\s*([^\n]+)',
        r'שם תאגיד/שם משפחה.*?:\s*([^\n]+)',
        r'\n([א-ת].{2,40}בע"מ)\n',
    ]:
        m = re.search(pat, t)
        if m:
            company = m.group(1).strip()
            break

    # TASE code
    tase_code = None
    for pat in [
        r'מספר נייר ערך בבורסה[:\s]+(\d+)',
        r'מספר ני"?ע בבורסה[:\s]+(\d+)',
    ]:
        m = re.search(pat, t)
        if m:
            tase_code = m.group(1)
            break

    # Change date
    when = None
    m = re.search(r'תאריך השינוי[:\s]+(\d{2}/\d{2}/\d{4})', t)
    if m: when = m.group(1)

    # Detect SELL: look for קיטון + מכירה anywhere
    is_sell = ('קיטון' in t) and (('מכירה' in t) or ('מכר' in t))
    if not is_sell:
        return []  # ignore non-sells (e.g., “חדל להיות בעל עניין”)

    # Shares change (negative)
    shares_sold = None
    m = re.search(r'שינוי בכמות ניירות הערך[:\s]+([-\d, ]+)', t)
    if m:
        shares_sold = _to_int(m.group(1))
        if shares_sold is not None:
            shares_sold = abs(shares_sold)

    # Price + unit
    price_nis = None
    unit = ''
    m = re.search(r'שער העסקה[:\s]+([\d\.,]+)\s*(?:מטבע\s*([^\s\n]+))?', t)
    if m:
        raw_price = _to_float(m.group(1))
        unit = (m.group(2) or '').strip()
        if raw_price is not None:
            # If unit mentions אג (agorot), convert to NIS
            if 'אג' in unit:
                price_nis = raw_price / 100.0
            else:
                price_nis = raw_price

    est_total_nis = None
    if shares_sold and price_nis:
        est_total_nis = round(shares_sold * price_nis, 2)

    # Owner (optional)
    owner = None
    for pat in [
        r'שם תאגיד/שם משפחה ושם פרטי של המחזיק[:\s]+([^\n]+)',
        r'שם.*?פרטי[:\s]+([^\n]+)',
    ]:
        m = re.search(pat, t)
        if m:
            owner = m.group(1).strip()
            break

    return [{
        "company": company or "",
        "tase_code": tase_code or "",
        "action": "SELL",
        "owner": owner or "",
        "shares": shares_sold or "",
        "price_nis": price_nis or "",
        "est_total_nis": est_total_nis or "",
        "when": when or "",
        "source_url": src_url,
        "pdf_url": pdf_url,
    }]

def save_csv(trades, alerts_path="tase_alerts.csv", trades_path="tase_trades.csv"):
    df = pd.DataFrame(trades)
    if df.empty:
        # still write headers so you can see it's “empty”
        pd.DataFrame(columns=["company","tase_code","trades","est_total_nis","when"]).to_csv(alerts_path, index=False)
        pd.DataFrame(columns=["company","tase_code","action","owner","shares","price_nis","est_total_nis","when","source_url","pdf_url"]).to_csv(trades_path, index=False)
        return
    # write trades
    df.to_csv(trades_path, index=False, encoding="utf-8-sig")
    # aggregate (sum total by company+code)
    when = df["when"].dropna().iloc[0] if "when" in df.columns and not df["when"].dropna().empty else ""
    agg = (df.groupby(["company","tase_code"], dropna=False)["est_total_nis"].sum().reset_index())
    agg["trades"] = df.groupby(["company","tase_code"], dropna=False).size().values
    agg["when"] = when
    agg = agg[["company","tase_code","trades","est_total_nis","when"]]
    agg.to_csv(alerts_path, index=False, encoding="utf-8-sig")

def auto_scan_mode():
    # Probe print-page ids: …/rhtm/1702001-1703000/H<ID>.htm, follow pdf, parse
    seed_env = os.getenv("TASE_LAST_ID", "").strip()
    try:
        last_id = int(seed_env) if seed_env else 1702000
    except:
        last_id = 1702000
    ahead = int(os.getenv("TASE_SCAN_AHEAD","400"))
    sleep_s = float(os.getenv("TASE_SLEEP","1.0"))

    # Persist state
    state_path = ".tase_state.txt"
    if os.path.exists(state_path):
        try:
            last_id = int(open(state_path, "r", encoding="utf-8").read().strip() or last_id)
        except:
            pass

    base_prefix = "https://mayafiles.tase.co.il/rhtm/1702001-1703000/"
    trades = []
    max_id = last_id + ahead
    for i in range(last_id+1, max_id+1):
        url = f"{base_prefix}H{i}.htm"
        try:
            html = fetch(url)
        except Exception:
            time.sleep(sleep_s); continue
        try:
            pdf_url = find_pdf_from_html_page(url, html)
            if not pdf_url:
                time.sleep(sleep_s); continue
            pdf_bytes = fetch(pdf_url, binary=True)
            trades.extend(parse_pdf(pdf_bytes, url, pdf_url))
        except Exception:
            # ignore individual failures
            pass
        time.sleep(sleep_s)

    # Save new state
    with open(state_path, "w", encoding="utf-8") as f:
        f.write(str(max_id))

    return trades

def main():
    links = (os.getenv("TASE_LINKS") or "").strip()
    trades = []
    if links:
        for raw in re.split(r'[\s\r\n]+', links):
            if not raw: continue
            try:
                pdf_url, pdf_bytes = resolve_to_pdf(raw)
                trades.extend(parse_pdf(pdf_bytes, raw, pdf_url))
            except Exception as e:
                # Continue on individual link failures
                print(f"[WARN] {raw}: {e}")
    else:
        trades = auto_scan_mode()

    save_csv(trades)

if __name__ == "__main__":
    main()

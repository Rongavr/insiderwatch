#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, csv, sys
import requests
from datetime import datetime, timezone

# ------------ HTTP session ------------
UA = os.getenv('TASE_USER_AGENT', 'insiderwatch-bot/1.0 (+github actions)')
SESSION = requests.Session()
SESSION.headers.update({'User-Agent': UA})
TIMEOUT = float(os.getenv('TASE_TIMEOUT_PER_FETCH', '12'))

def now_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def bucket_url_for(hid: int) -> str:
    # Example: H1702640 -> /rhtm/1702001-1703000/H1702640.htm
    start = (hid // 1000) * 1000 + 1
    end   = start + 999
    return f"https://mayafiles.tase.co.il/rhtm/{start}-{end}/H{hid}.htm"

def maya_to_print_url(link: str):
    # Accept either maya.tase.co.il/.../reports/1702640 or direct print Hxxxxx.htm
    m = re.search(r'/reports/(\d+)', link)
    if m:
        hid = int(m.group(1))
        return bucket_url_for(hid), hid
    m2 = re.search(r'/H(\d+)\.htm', link)
    if m2:
        hid = int(m2.group(1))
        return link, hid
    return link, None

def exists(url: str) -> bool:
    try:
        r = SESSION.head(url, allow_redirects=True, timeout=TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False

def fetch(url: str) -> str:
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    return r.text

# ------------ Parsing helpers ------------
def _to_int(s: str):
    if s is None: return None
    try: return int(s.replace(',', '').strip())
    except: return None

def _to_float(s: str):
    if s is None: return None
    try: return float(s.replace(',', '').strip())
    except: return None

def _extract_company(text: str) -> str:
    # Heuristic: company usually appears just before "מספר ברשם"
    lines = [ln.strip() for ln in text.splitlines()]
    for i, ln in enumerate(lines[:80]):
        if 'מספר ברשם' in ln:
            for j in range(i-1, max(-1, i-6), -1):
                if lines[j]:
                    return lines[j]
    # Fallback: scan first non-empty line
    for ln in lines:
        if ln: return ln
    return ''

def parse_report(text: str, url: str, hid: int | None):
    # Look for insider/interest keywords
    keywords = [
        'שינויים בהחזקות בעלי עניין',
        'שינוי החזקות בעלי עניין',
        'החזקות בעלי עניין/נושאי משרה',
        'החל להיות בעל ענין',
        'חדל להיות בעל ענין',
    ]
    if not any(k in text for k in keywords):
        return None

    company = _extract_company(text)
    m_code = re.search(r'מספר נייר ערך בבורסה[:\s]+(\d{6,7})', text)
    tase_code = m_code.group(1) if m_code else ''

    # Quantity change (may be negative)
    m_qty = re.search(r'שינוי בכמות נייר(?:ות)? הערך[:\s]+([-\d,]+)', text)
    qty = _to_int(m_qty.group(1)) if m_qty else None

    # Price
    price_nis = None
    m_px = re.search(r'שער העסקה[:\s]+([\d,]+(?:\.\d+)?)\s*(אג\'|ש"ח|ש”ח|ש׳׳ח|שח)?', text)
    if m_px:
        p = _to_float(m_px.group(1))
        unit = (m_px.group(2) or '').strip()
        if p is not None:
            price_nis = p * 0.01 if unit.startswith('אג') else p

    est = None
    if qty is not None and price_nis is not None:
        est = abs(qty) * price_nis

    # Change date (optional)
    date_iso = ''
    m_dt = re.search(r'תאריך השינוי[:\s]+(\d{2}/\d{2}/\d{4})', text)
    if m_dt:
        dd, mm, yyyy = m_dt.group(1).split('/')
        date_iso = f'{yyyy}-{mm}-{dd}'

    direction = ''
    if qty is not None:
        direction = 'SELL' if qty < 0 else 'BUY'

    return {
        'report_id': str(hid) if hid else '',
        'company': company,
        'tase_code': tase_code,
        'qty_change': qty if qty is not None else '',
        'price_nis': f'{price_nis:.4f}' if price_nis is not None else '',
        'est_total_nis': f'{est:.2f}' if est is not None else '',
        'direction': direction,
        'date': date_iso,
        'link': url
    }

def write_csv(path: str, rows: list[dict], header: list[str]):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ------------ State ------------
def load_state() -> int:
    if not os.path.exists('.tase_state.txt'):
        seed = os.getenv('TASE_LAST_ID', '1702000')
        try: return int(seed)
        except: return 1702000
    try:
        t = open('.tase_state.txt', 'r', encoding='utf-8').read()
        m = re.search(r'last=(\d+)', t)
        if m: return int(m.group(1))
    except: pass
    return 1702000

def save_state(last_id: int):
    with open('.tase_state.txt', 'w', encoding='utf-8') as f:
        f.write(f"last={last_id}\nupdated={now_iso()}\n")

# ------------ Scan modes ------------
def scan_links(links: list[str], max_seconds: int):
    start = time.time()
    out = []
    for raw in links:
        link = raw.strip()
        if not link: continue
        url, hid = maya_to_print_url(link)
        if not url: continue
        if not exists(url):  # fast skip
            continue
        try:
            html = fetch(url)
            parsed = parse_report(html, url, hid)
            if parsed: out.append(parsed)
        except Exception:
            pass
        if time.time() - start > max_seconds:
            break
    return out

def scan_range(max_seconds: int, scan_ahead: int, sleep_s: float, checkpoint_every: int):
    start = time.time()
    last = load_state()
    begin = last + 1
    end   = begin + max(0, scan_ahead)

    out = []
    scanned = 0
    current = begin - 1

    for hid in range(begin, end + 1):
        current = hid
        url = bucket_url_for(hid)
        try:
            if exists(url):                # cheap HEAD
                html = fetch(url)          # only GET if exists
                parsed = parse_report(html, url, hid)
                if parsed:
                    out.append(parsed)
        except Exception:
            pass

        scanned += 1
        if scanned % checkpoint_every == 0:
            save_state(hid)

        if (time.time() - start) > max_seconds:
            save_state(hid)
            break

        time.sleep(sleep_s)
    else:
        save_state(current)

    return out

# ------------ Main ------------
def main():
    max_seconds       = int(os.getenv('TASE_MAX_SECONDS', '540'))   # 9m budget
    scan_ahead        = int(os.getenv('TASE_SCAN_AHEAD', '280'))    # ids to probe
    sleep_s           = float(os.getenv('TASE_SLEEP', '0.25'))      # politeness
    checkpoint_every  = int(os.getenv('TASE_CHECKPOINT_EVERY', '25'))
    min_nis           = float(os.getenv('MIN_NIS', '0'))
    links_env         = os.getenv('TASE_LINKS', '').strip()

    rows = []
    if links_env:
        links = [p for p in re.split(r'[\s,]+', links_env) if p]
        rows.extend(scan_links(links, max_seconds))
    else:
        rows.extend(scan_range(max_seconds, scan_ahead, sleep_s, checkpoint_every))

    # Always emit artifacts (even if empty)
    trades_header = ['report_id','company','tase_code','qty_change','price_nis','est_total_nis','direction','date','link']
    write_csv('tase_trades.csv', rows, trades_header)

    # Aggregate to alerts
    alerts = {}
    for r in rows:
        try:
            val = float(r['est_total_nis']) if r['est_total_nis'] else 0.0
        except:
            val = 0.0
        key = (r.get('company',''), r.get('tase_code',''))
        if key not in alerts:
            alerts[key] = {'company': key[0], 'tase_code': key[1], 'trades': 0, 'est_total_nis': 0.0, 'when': now_iso()}
        alerts[key]['trades'] += 1
        alerts[key]['est_total_nis'] += val

    alert_rows = []
    for v in alerts.values():
        if v['est_total_nis'] >= min_nis:
            alert_rows.append({
                'company': v['company'],
                'tase_code': v['tase_code'],
                'trades': v['trades'],
                'est_total_nis': f"{v['est_total_nis']:.2f}",
                'when': v['when']
            })

    alert_header = ['company','tase_code','trades','est_total_nis','when']
    write_csv('tase_alerts.csv', alert_rows, alert_header)

    return 0

if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, io, sys, math, csv, json
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import pandas as pd

# pdf text extraction
from pdfminer.high_level import extract_text as pdf_extract_text

UA = {"User-Agent": "Mozilla/5.0 (compatible; TASEScanner/1.0)"}

# ------------------------
# Env + state management
# ------------------------
def getenv_str(name, default=""):
    v = os.getenv(name)
    return default if v is None else str(v)

def getenv_int(name, default):
    v = os.getenv(name)
    if v in (None, ""):
        return int(default)
    return int(v)

def getenv_float(name, default):
    v = os.getenv(name)
    if v in (None, ""):
        return float(default)
    return float(v)

LINKS          = getenv_str("TASE_LINKS", "").strip()
LAST_ID_SEED   = getenv_int("TASE_LAST_ID", 1702700)
SCAN_AHEAD     = getenv_int("TASE_SCAN_AHEAD", 180)
SLEEP_SEC      = getenv_float("TASE_SLEEP", 0.25)
TIME_BUDGET_S  = getenv_int("TIME_BUDGET_S", 420)
MIN_NIS        = getenv_float("MIN_NIS", 0.0)

STATE_FILE = ".tase_state.txt"
LOG_FILE   = "tase.log"

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def read_state():
    # If file exists and valid → use it; else use seed
    try:
        if os.path.exists(STATE_FILE):
            s = open(STATE_FILE, "r", encoding="utf-8").read().strip()
            if s.isdigit():
                return int(s)
    except Exception:
        pass
    return LAST_ID_SEED

def write_state(last_id):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            f.write(str(last_id))
    except Exception as e:
        log(f"WARN: failed writing state: {e}")

# ------------------------
# URL helpers
# ------------------------
def mayafiles_htm_url(report_id: int) -> str:
    # H1702656.htm lives under bucket "1702001-1703000"
    start = (report_id // 1000) * 1000 + 1
    end   = start + 999
    return f"https://mayafiles.tase.co.il/rhtm/{start}-{end}/H{report_id}.htm"

def maya_htm_attachment(report_id: int) -> str:
    return f"https://maya.tase.co.il/he/reports/{report_id}?attachmentType=htm"

def maya_pdf_attachment(report_id: int) -> str:
    return f"https://maya.tase.co.il/he/reports/{report_id}?attachmentType=pdf1"

def fetch_text(url: str, timeout=15) -> str:
    r = requests.get(url, headers=UA, timeout=timeout, allow_redirects=True)
    ct = r.headers.get("Content-Type", "")
    if "application/pdf" in ct or url.lower().endswith(".pdf"):
        # convert pdf bytes to text
        text = pdf_extract_text(io.BytesIO(r.content))
        return text or ""
    else:
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text or ""

def try_fetch_report_text(report_id: int):
    # Try stable order: mayafiles H*.htm → maya htm attachment → maya pdf attachment
    for url in (mayafiles_htm_url(report_id),
                maya_htm_attachment(report_id),
                maya_pdf_attachment(report_id)):
        try:
            t = fetch_text(url)
            if t and len(t) > 100:
                return url, t
        except Exception as e:
            log(f"INFO: fetch failed {url}: {e}")
    return None, None

# ------------------------
# Parsers (Hebrew forms)
# ------------------------

RE_COMPANY   = re.compile(r"שם\s+מקוצר:\s*(.+)", re.UNICODE)
RE_SEC_NAME  = re.compile(r"שם\s+וסוג\s+נייר\s+הערך[:\s]+(.+)", re.UNICODE)
RE_TASE_CODE = re.compile(r"מספר\s+נייר\s+ערך\b[:\s]+(\d+)", re.UNICODE)

# sale (“קיטון … עקב מכירה”) pattern:
RE_SALE_KIND = re.compile(r"מהות\s+השינוי[:\s]+.*קיטון.*מכירה", re.UNICODE)
RE_QTY_DOWN  = re.compile(r"שינוי\s+בכמות\s+ניירות\s+הערך[:\s]+([\d,\.]+)\s*-\s*", re.UNICODE)
RE_PRICE_AG  = re.compile(r"שער\s+העסקה[:\s]+([\d\.,]+)\s*.*אג", re.UNICODE)

# “חדל להיות בעל ענין” cards (Gencell example)
RE_STOP_BEI  = re.compile(r"חדל\s+להיות\s+בעל\s+ענין", re.UNICODE)

def text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    return soup.get_text("\n", strip=True)

def extract_trades_from_text(text: str, report_id: int, src_url: str):
    """
    Return list of dicts with computed NIS for clear 'sale' cases.
    We keep it conservative: if we lack qty or price in agorot → skip.
    """
    out = []

    # Try to locate security name / code (best-effort)
    sec_name = None
    m = RE_SEC_NAME.search(text)
    if m: sec_name = m.group(1).strip()

    tase_code = None
    m = RE_TASE_CODE.search(text)
    if m: tase_code = m.group(1).strip()

    # Detect explicit sale forms
    sale_like = bool(RE_SALE_KIND.search(text)) or ("קיטון" in text and "מכירה" in text)

    qty = None
    m = RE_QTY_DOWN.search(text)
    if m:
        qty = int(re.sub(r"[^\d]", "", m.group(1)))

    price_agorot = None
    m = RE_PRICE_AG.search(text)
    if m:
        # "28.45" agorot -> NIS = 0.2845
        try:
            price_agorot = float(m.group(1).replace(",", ""))
        except Exception:
            price_agorot = None

    # Compute if we have enough data
    if sale_like and qty and price_agorot is not None:
        est_nis = qty * (price_agorot / 100.0)
        out.append({
            "company": sec_name or "",
            "tase_code": tase_code or "",
            "qty_sold": qty,
            "price_agorot": price_agorot,
            "est_total_nis": round(est_nis, 2),
            "report_id": report_id,
            "url": src_url,
            "kind": "sale"
        })
        return out

    # Otherwise, record “signal” items we might care about later (no NIS)
    if RE_STOP_BEI.search(text):
        out.append({
            "company": sec_name or "",
            "tase_code": tase_code or "",
            "qty_sold": 0,
            "price_agorot": None,
            "est_total_nis": 0.0,
            "report_id": report_id,
            "url": src_url,
            "kind": "ceased_interested_party"
        })

    return out

# ------------------------
# Main
# ------------------------
def main():
    start_time = time.time()
    rows = []

    if LINKS:
        # Mode A: explicit links (space/newline separated)
        urls = [u for u in re.split(r"[\s\r\n]+", LINKS) if u.strip()]
        log(f"MODE A: parsing {len(urls)} provided URLs")
        for u in urls:
            if time.time() - start_time > TIME_BUDGET_S:
                log("Time budget reached; stopping.")
                break
            try:
                txt = fetch_text(u)
                if "<html" in txt.lower():
                    txt = text_from_html(txt)
                rows.extend(extract_trades_from_text(txt, report_id=0, src_url=u))
                time.sleep(SLEEP_SEC)
            except Exception as e:
                log(f"ERROR parsing {u}: {e}")

        # No state updates in Mode A
    else:
        # Mode B: auto-scan mayafiles id bucket
        last_id = read_state()
        log(f"MODE B: auto-scan starting at id={last_id} window={SCAN_AHEAD}")

        scanned = 0
        cur = last_id
        end_id = last_id + SCAN_AHEAD - 1

        while cur <= end_id:
            if time.time() - start_time > TIME_BUDGET_S:
                log("Time budget reached; stopping.")
                break

            url, txt = try_fetch_report_text(cur)
            if txt:
                if "<html" in txt.lower():
                    txt2 = text_from_html(txt)
                else:
                    txt2 = txt
                rows.extend(extract_trades_from_text(txt2, report_id=cur, src_url=url))
            else:
                log(f"MISS {cur}")

            scanned += 1
            cur += 1
            time.sleep(SLEEP_SEC)

        # Persist next starting id to avoid rescanning
        write_state(cur)

    # Build DataFrames + filter + save
    if rows:
        df = pd.DataFrame(rows)

        # filter by MIN_NIS when applicable
        def pass_filter(r):
            if r.get("kind") == "sale" and r.get("est_total_nis"):
                return r["est_total_nis"] >= MIN_NIS
            return True

        df = df[df.apply(pass_filter, axis=1)].reset_index(drop=True)

        # Save raw trades
        df_cols = ["company", "tase_code", "qty_sold", "price_agorot",
                   "est_total_nis", "report_id", "url", "kind"]
        for c in df_cols:
            if c not in df.columns:
                df[c] = None
        df[df_cols].to_csv("tase_trades.csv", index=False, encoding="utf-8")

        # Aggregate alerts by company
        ag = (df.groupby(["company", "tase_code"], dropna=False)
                .agg(trades=("report_id","count"),
                     est_total_nis=("est_total_nis","sum"))
                .reset_index())
        ag["when"] = datetime.now(timezone.utc).isoformat()
        ag.to_csv("tase_alerts.csv", index=False, encoding="utf-8")

        log(f"SAVED: {len(df)} trade rows; {len(ag)} alert rows")
    else:
        # still emit headers so the artifact exists
        pd.DataFrame(columns=["company","tase_code","qty_sold","price_agorot",
                              "est_total_nis","report_id","url","kind"]).to_csv("tase_trades.csv", index=False, encoding="utf-8")
        pd.DataFrame(columns=["company","tase_code","trades","est_total_nis","when"]).to_csv("tase_alerts.csv", index=False, encoding="utf-8")
        log("No rows this run.")

if __name__ == "__main__":
    # fresh log each run
    try:
        if os.path.exists(LOG_FILE):
            os.remove(LOG_FILE)
    except Exception:
        pass
    main()

#!/usr/bin/env python3
import sys, os; sys.path.insert(0, os.path.dirname(__file__))
import os, re, time, argparse
from datetime import datetime, date
from urllib.parse import urljoin
import requests
import pandas as pd
from tqdm import tqdm
from insider_scanner import parse_form4_xml  # reuses your working parser

SEC_EMAIL = os.getenv("SEC_EMAIL", "your.name@example.com")
HEADERS = {
    "User-Agent": f"InsiderBacktest/1.0 ({SEC_EMAIL})",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
    "Connection": "keep-alive",
}
BASE = "https://www.sec.gov/Archives/edgar/"

def quarter(dt: date) -> int: return (dt.month - 1)//3 + 1
def iter_quarters(start: date, end: date):
    y, q = start.year, quarter(start)
    while True:
        yield (y, q)
        if y == end.year and q == quarter(end): break
        q += 1
        if q == 5: q = 1; y += 1

def fetch(url: str, text=False):
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code} {url}")
    return r.text if text else r.content

def list_form4_in_quarter(y: int, q: int) -> pd.DataFrame:
    idx_url = f"{BASE}full-index/{y}/QTR{q}/master.idx"
    raw = fetch(idx_url, text=True)
    lines = raw.splitlines()
    start = 0
    for i, ln in enumerate(lines):
        if ln.startswith("-----"): start = i+1; break
    entries = []
    for ln in lines[start:]:
        parts = ln.split("|")
        if len(parts) != 5: continue
        cik, company, form, filed, fname = parts
        if form == "4":
            entries.append((int(cik), company, form, filed, fname))
    return pd.DataFrame(entries, columns=["cik","company","form","filed","filename"])

def extract_xml_blobs(sub_txt: str):
    for m in re.finditer(r"(<ownershipDocument[\\s\\S]*?</ownershipDocument>)", sub_txt, re.I):
        yield m.group(1).encode("utf-8")

def parse_submission_to_rows(submission_url: str):
    txt = fetch(urljoin(BASE, submission_url), text=True)
    rows, found = [], False
    for xml_bytes in extract_xml_blobs(txt):
        found = True
        try:
            rows.extend(parse_form4_xml(xml_bytes))
        except Exception:
            continue
    if not found:
        idx_url = urljoin(BASE, submission_url.replace(".txt","-index.htm"))
        try:
            html = fetch(idx_url, text=True)
            m = re.search(r'href="([^"]*ownership\\.xml)"', html, flags=re.I)
            if m:
                xml_url = urljoin(idx_url.rsplit("/",1)[0]+"/", m.group(1))
                xml_bytes = fetch(xml_url, text=False)
                rows.extend(parse_form4_xml(xml_bytes))
        except Exception:
            pass
    for r in rows:
        r["filing_url"] = urljoin(BASE, submission_url.replace(".txt","-index.htm"))
        r["xml_url"]    = urljoin(BASE, submission_url)
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end",   default=datetime.utcnow().date().isoformat())
    ap.add_argument("--out",   default="trades.parquet")
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--perq_limit", type=int, default=200000)
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).date()
    end   = datetime.fromisoformat(args.end).date()

    all_rows = []
    for y,q in iter_quarters(start, end):
        dfq = list_form4_in_quarter(y,q)
        if dfq.empty: continue
        dfq = dfq[(dfq["filed"] >= args.start) & (dfq["filed"] <= args.end)].head(args.perq_limit)
        for _, row in tqdm(dfq.iterrows(), total=len(dfq), desc=f"{y}Q{q}"):
            fname = row["filename"]
            try:
                items = parse_submission_to_rows(fname)
                for it in items:
                    it["filing_dt"] = row["filed"] + "T00:00:00Z"
                all_rows.extend(items)
            except Exception:
                pass
            time.sleep(args.sleep)

    if not all_rows:
        print("No rows parsed."); return
    df = pd.DataFrame(all_rows)
    df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce")
    df["price"]      = pd.to_numeric(df["price"], errors="coerce")
    df["shares"]     = pd.to_numeric(df["shares"], errors="coerce")
    df = df.dropna(subset=["symbol","amount_usd"])
    df.to_parquet(args.out, index=False)
    print(f"Saved {len(df):,} rows to {args.out}")

if __name__ == "__main__":
    main()

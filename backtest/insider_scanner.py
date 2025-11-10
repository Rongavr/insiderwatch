#!/usr/bin/env python3
import os, re, time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dtp

SEC_EMAIL = os.getenv("SEC_EMAIL", "your.name@example.com")
HEADERS = {
    "User-Agent": f"InsiderScanner/1.0 ({SEC_EMAIL})",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
    "Connection": "keep-alive",
}
ATOM_FEED = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=100&output=atom"

OUT_TRADES_CSV = "insider_trades.csv"
OUT_ALERTS_CSV = "alerts.csv"

def fetch(url, is_html=False, tries=3, sleep_sec=1.0):
    for i in range(tries):
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.text if is_html else r.content
        time.sleep(sleep_sec * (i + 1))
    raise RuntimeError(f"HTTP {r.status_code} for {url}")

def get_atom_entries():
    html = fetch(ATOM_FEED, is_html=True)
    soup = BeautifulSoup(html, "lxml-xml")
    entries = []
    for e in soup.find_all("entry"):
        link = e.find("link")
        if not link or not link.get("href"):
            continue
        updated = e.find("updated")
        entries.append({
            "link": link.get("href"),
            "updated": updated.text if updated else None
        })
    return entries

def find_xml_candidates(index_url):
    """Return a preference-ordered list of XML/TXT URLs from the filing index page.
       Falls back to the Form 4 document page if the index has no direct XML/TXT links.
    """
    html = fetch(index_url, is_html=True)
    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.I)
    abs_hrefs = [urljoin(index_url, h) for h in hrefs]

    # First pass: any XML/TXT links found directly on the index page
    cands = [u for u in abs_hrefs if re.search(r'\.(xml|txt)$', u, flags=re.I)]

    # Fallback: follow a likely Form 4 HTML document (xslF345/form4 *.htm) and scrape XML/TXT there
    if not cands:
        doc_links = [u for u in abs_hrefs if re.search(r'(xslf345|form4).*\.htm$', u, flags=re.I)]
        if doc_links:
            try:
                doc_html = fetch(doc_links[0], is_html=True)
                doc_hrefs = re.findall(r'href="([^"]+)"', doc_html, flags=re.I)
                cands = [urljoin(doc_links[0], u) for u in doc_hrefs if re.search(r'\.(xml|txt)$', u, flags=re.I)]
            except Exception:
                pass

    def score(u: str) -> int:
        ul = u.lower()
        if 'ownership.xml' in ul: return 100
        if re.search(r'(form4|doc4)\.(xml|txt)$', ul): return 95
        if 'primary_doc.xml' in ul: return 85
        if 'f345' in ul: return 80
        if ul.endswith('.txt'): return 70
        if any(x in ul for x in ['cal.xml','def.xml','lab.xml','pre.xml','xsd']): return 5
        return 50

    seen = {}
    for u in cands:
        s = score(u)
        if u not in seen or s > seen[u]:
            seen[u] = s

    return [u for u,_ in sorted(seen.items(), key=lambda kv: kv[1], reverse=True)]
def parse_form4_xml(xml_text):
    """
    Return purchase transactions (code 'P') as list of dicts:
    [{symbol, owner, shares, price, amount_usd, tenb5, txn_date}]
    """
    out = []
    soup = BeautifulSoup(xml_text, "lxml-xml")

    sym_el = soup.find("issuerTradingSymbol")
    symbol = sym_el.text.strip() if sym_el else None

    # detect 10b5-1 mention anywhere in the XML
    has_10b5 = bool(re.search(r'10b5-?1', xml_text, flags=re.I))

    # list of owners (names)
    owners = []
    for ro in soup.find_all("reportingOwner"):
        n = ro.find("rptOwnerName")
        if n and n.text:
            owners.append(n.text.strip())
    if not owners:
        owners = ["(unknown)"]

    # iterate over non-derivative txns
    for tx in soup.find_all("nonDerivativeTransaction"):
        code = tx.find("transactionCode")
        if not code or code.text.strip().upper() != "P":
            continue  # not a purchase
        shares_node = tx.find("transactionShares")
        price_node  = tx.find("transactionPricePerShare")
        date_node   = tx.find("transactionDate")

        def to_float(el):
            try:
                v = el.find("value").text
                return float(v.replace(",", ""))
            except:
                return 0.0

        shares = to_float(shares_node) if shares_node else 0.0
        price  = to_float(price_node)  if price_node  else 0.0
        amount = shares * price
        txn_dt = ""
        try:
            txn_dt = date_node.find("value").text.strip()
        except:
            pass

        for owner in owners:
            out.append({
                "symbol": symbol,
                "owner": owner,
                "shares": shares,
                "price": price,
                "amount_usd": amount,
                "tenb5": has_10b5,
                "txn_date": txn_dt
            })
    return out

def load_existing_trades():
    cols = ["filing_dt","symbol","owner","shares","price","amount_usd","tenb5","txn_date","filing_url","xml_url"]
    if not os.path.exists(OUT_TRADES_CSV):
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_csv(OUT_TRADES_CSV)
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        return df[cols]
    except:
        return pd.DataFrame(columns=cols)

def append_trades(rows):
    df = load_existing_trades()
    new_df = pd.DataFrame(rows)
    need_cols = ["filing_dt","symbol","owner","shares","price","amount_usd","tenb5","txn_date","filing_url","xml_url"]
    for c in need_cols:
        if c not in new_df.columns:
            new_df[c] = pd.NA
    all_df = pd.concat([df, new_df], ignore_index=True)
    all_df.drop_duplicates(subset=["symbol","owner","txn_date","xml_url"], inplace=True)
    all_df.to_csv(OUT_TRADES_CSV, index=False)
    return all_df

def aggregate_alerts(df, days=7, min_owners=3, min_usd=300000, exclude_10b5=True):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days)

    def parse_dt(x):
        try:
            d = dtp.parse(str(x))
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            else:
                d = d.astimezone(timezone.utc)
            return d
        except:
            return None

    df = df.copy()
    df["_txn_dt"] = df["txn_date"].apply(parse_dt)
    df["_filing_dt"] = df["filing_dt"].apply(parse_dt)
    df["_when"] = df[["_txn_dt","_filing_dt"]].max(axis=1)
    df = df[df["_when"].notna()]
    df = df[df["_when"] >= cutoff]
    if exclude_10b5 and "tenb5" in df.columns:
        df = df[~df["tenb5"].astype(str).str.lower().isin(["true","1"])]

    grouped = (
        df.groupby("symbol", dropna=False)
          .agg(
              owners_count=("owner", "nunique"),
              total_usd=("amount_usd", "sum"),
              last_when=("_when", "max")
          )
          .reset_index()
    )
    alerts = grouped[(grouped["owners_count"] >= min_owners) & (grouped["total_usd"] >= min_usd)]
    return alerts.sort_values(["owners_count","total_usd"], ascending=False)
def main(days=7, min_owners=3, min_usd=300000):
    entries = get_atom_entries()
    collected = []
    for ent in entries:
        idx_url = ent["link"]
        try:
            xml_candidates = find_xml_candidates(idx_url)
            got_any = False
            for xml_url in xml_candidates:
                try:
                    xml_text = fetch(xml_url, is_html=True)
                    # quick reject if it doesn't look like a Form 4 ownership doc
                    if "<nonDerivativeTransaction" not in xml_text and "<issuerTradingSymbol" not in xml_text:
                        continue
                    txs = parse_form4_xml(xml_text)
                    if not txs:
                        continue
                    filing_dt = dtp.parse(ent["updated"]).isoformat() if ent["updated"] else datetime.utcnow().isoformat()
                    for row in txs:
                        row.update({"filing_dt": filing_dt, "filing_url": idx_url, "xml_url": xml_url})
                    collected.extend(txs)
                    got_any = True
                    break  # use first good xml for this filing
                except Exception as ex_xml:
                    # try next candidate
                    continue
            time.sleep(0.4)  # politeness
        except Exception as e:
            print(f"[WARN] {idx_url}: {e}")

    if not collected:
        print("No new Form 4 purchases found.")
        pd.DataFrame(columns=["symbol","owners_count","total_usd","last_when"]).to_csv(OUT_ALERTS_CSV, index=False)
        return

    df_all = append_trades(collected)
    alerts = aggregate_alerts(df_all, days=days, min_owners=min_owners, min_usd=min_usd, exclude_10b5=True)
    if alerts.empty:
        print("No tickers crossed thresholds today.")
        pd.DataFrame(columns=["symbol","owners_count","total_usd","last_when"]).to_csv(OUT_ALERTS_CSV, index=False)
    else:
        alerts.to_csv(OUT_ALERTS_CSV, index=False)
        print("ALERTS:")
        try:
            print(alerts.to_string(index=False))
        except:
            print(alerts)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--min_owners", type=int, default=3)
    ap.add_argument("--min_usd", type=float, default=300000)
    args = ap.parse_args()
    main(days=args.days, min_owners=args.min_owners, min_usd=args.min_usd)

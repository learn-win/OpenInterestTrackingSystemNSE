#!/usr/bin/env python3
"""
nifty.py

Single-run NIFTY/BANKNIFTY pct-COI ranking alert script for PythonAnywhere.
Uses requests with proper headers to bypass NSE restrictions.
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import traceback
import gspread
from google.oauth2.service_account import Credentials
import json
import time
import random

# ---------------- USER CONFIG ----------------
SYMBOL = "NIFTY"                     # "NIFTY" or "BANKNIFTY"
NEAREST_STRIKES = 10                 # nearest strikes to analyze
ALERT_TOP_N = 3                      # top N strikes by pct_coi per side
SPREADSHEET_NAME = "Nifty_OI_Alerts_and_History_pctCOI"
HISTORY_SHEET = "history"
ALERTS_SHEET = "alerts"

SERVICE_ACCOUNT_FILE = "/home/ard/analog-reef-457516-d0-89be45703eea.json"
FALLBACK_CSV = "/home/ard/fallback.csv"
ALLOW_FALLBACK = True

MIN_PREV_OI_DENOM = 1e-9
MIN_POSITIVE_PCT = 0.0001
VERBOSE_FETCH = True

# NSE API Configuration
NSE_BASE_URL = "https://www.nseindia.com"
NSE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Cache-Control': 'max-age=0',
}

# ---------------- NSE Data Fetching with Session ----------------
def create_nse_session():
    """Create a session with NSE website to get cookies"""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    
    try:
        # First visit the homepage to get cookies
        response = session.get(NSE_BASE_URL, timeout=10)
        if response.status_code == 200:
            print("NSE session established successfully")
            return session
        else:
            print(f"NSE homepage returned status: {response.status_code}")
            return session
    except Exception as e:
        print(f"Warning: Could not establish NSE session: {e}")
        return session

def fetch_option_chain_direct(symbol="NIFTY", retries=3):
    """
    Fetch option chain data directly from NSE API using requests.
    This bypasses nsepython library which may have proxy issues.
    """
    
    # Map symbol to NSE format
    symbol_map = {
        "NIFTY": "NIFTY",
        "BANKNIFTY": "BANKNIFTY",
        "FINNIFTY": "FINNIFTY"
    }
    
    nse_symbol = symbol_map.get(symbol.upper(), symbol.upper())
    
    for attempt in range(retries):
        try:
            print(f"Fetch attempt {attempt + 1}/{retries} for {nse_symbol}...")
            
            # Create fresh session for each attempt
            session = create_nse_session()
            time.sleep(random.uniform(1, 3))  # Random delay
            
            # Construct option chain URL
            url = f"{NSE_BASE_URL}/api/option-chain-indices?symbol={nse_symbol}"
            
            # Make the request
            response = session.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'records' in data and 'data' in data['records']:
                    records = data['records']['data']
                    underlying = data['records'].get('underlyingValue')
                    
                    print(f"✓ Successfully fetched {len(records)} records")
                    print(f"✓ Underlying value: {underlying}")
                    
                    return data
                else:
                    print(f"Invalid data structure in response")
            else:
                print(f"HTTP {response.status_code}: {response.reason}")
            
            # Wait before retry
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                
        except requests.exceptions.ProxyError as e:
            print(f"Proxy error on attempt {attempt + 1}: {e}")
            if attempt == retries - 1:
                raise RuntimeError(
                    "PythonAnywhere free tier blocks NSE website. "
                    "Solutions: (1) Upgrade to paid account, (2) Use fallback CSV, "
                    "(3) Deploy to Heroku/Railway/other platform"
                )
        except Exception as e:
            print(f"Error on attempt {attempt + 1}: {e}")
            if VERBOSE_FETCH:
                traceback.print_exc()
            
            if attempt == retries - 1:
                raise
    
    raise RuntimeError(f"Failed to fetch data after {retries} attempts")

# ---------------- Data processing helpers ----------------
def records_to_df(records):
    """Convert option chain records to DataFrame"""
    rows = []
    for rec in records:
        strike = rec.get('strikePrice', 0)
        ce = rec.get('CE', {})
        pe = rec.get('PE', {})
        
        def safe_int(x):
            try:
                return int(float(x)) if x is not None else 0
            except:
                return 0
        
        def safe_float(x):
            try:
                return float(x) if x is not None else 0.0
            except:
                return 0.0
        
        rows.append({
            'strike': strike,
            'CE_OI': safe_int(ce.get('openInterest', 0)),
            'CE_COI': safe_int(ce.get('changeinOpenInterest', 0)),
            'CE_IV': safe_float(ce.get('impliedVolatility', 0)),
            'PE_OI': safe_int(pe.get('openInterest', 0)),
            'PE_COI': safe_int(pe.get('changeinOpenInterest', 0)),
            'PE_IV': safe_float(pe.get('impliedVolatility', 0))
        })
    
    df = pd.DataFrame(rows).drop_duplicates(subset=['strike']).sort_values('strike').reset_index(drop=True)
    return df

def pick_nearest(df, underlying, n=10):
    """Select N nearest strikes to underlying price"""
    df2 = df.copy()
    df2['dist'] = (df2['strike'] - underlying).abs()
    sel = df2.sort_values('dist').head(n).sort_values('strike').reset_index(drop=True)
    return sel

def load_fallback_csv(csv_path):
    """Load and process fallback CSV data"""
    try:
        print(f"Loading fallback CSV: {csv_path}")
        tmp = pd.read_csv(csv_path)
        
        if tmp.empty:
            print("Fallback CSV is empty")
            return None, None
        
        cols_l = {c.lower(): c for c in tmp.columns}
        
        def find_col(patterns):
            for k, orig in cols_l.items():
                for p in patterns:
                    if p in k:
                        return orig
            return None
        
        strike_col = find_col(['strike'])
        if not strike_col:
            print("Fallback CSV missing strike column")
            return None, None
        
        def numeric(col):
            if col:
                return pd.to_numeric(tmp[col], errors='coerce').fillna(0)
            return pd.Series([0] * len(tmp))
        
        df = pd.DataFrame({
            'strike': tmp[strike_col],
            'CE_OI': numeric(find_col(['ce_oi', 'call_oi', 'calls_oi'])).astype(int),
            'CE_COI': numeric(find_col(['ce_coi', 'ce_change', 'call_change'])).astype(int),
            'CE_IV': numeric(find_col(['ce_iv', 'call_iv'])),
            'PE_OI': numeric(find_col(['pe_oi', 'put_oi', 'puts_oi'])).astype(int),
            'PE_COI': numeric(find_col(['pe_coi', 'pe_change', 'put_change'])).astype(int),
            'PE_IV': numeric(find_col(['pe_iv', 'put_iv']))
        }).drop_duplicates(subset=['strike']).sort_values('strike').reset_index(drop=True)
        
        underlying = (df['strike'].min() + df['strike'].max()) / 2
        
        print(f"✓ Loaded fallback CSV: {len(df)} strikes, underlying≈{underlying:.0f}")
        return df, underlying
        
    except Exception as ex:
        print(f"Fallback CSV error: {ex}")
        traceback.print_exc()
        return None, None

# ---------------- Google Sheets helpers ----------------
def sheets_client_from_service_account(json_path):
    """Create Google Sheets client from service account"""
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Service account JSON not found: {json_path}")
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(json_path, scopes=scopes)
    return gspread.authorize(creds)

def open_or_create_spreadsheet(client, name):
    """Open existing spreadsheet or create new one"""
    try:
        sh = client.open(name)
        return sh, False
    except:
        sh = client.create(name)
        return sh, True

def ensure_worksheet(spreadsheet, title, header_row):
    """Ensure worksheet exists with proper headers"""
    try:
        ws = spreadsheet.worksheet(title)
        return ws, False
    except:
        ws = spreadsheet.add_worksheet(title=title, rows="2000", cols=str(len(header_row)+5))
        ws.append_row(header_row)
        return ws, True

# ---------------- Main logic ----------------
def main():
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*60}")
    print(f"[{ts}] Starting single-run for {SYMBOL}")
    print(f"{'='*60}\n")
    
    # 1) Try to fetch option chain data
    data = None
    df = None
    underlying = None
    
    try:
        data = fetch_option_chain_direct(SYMBOL, retries=3)
        
        if data:
            records = data.get('records', {}).get('data', [])
            underlying = data.get('records', {}).get('underlyingValue')
            
            if records and len(records) > 0:
                df = records_to_df(records)
                print(f"✓ Processed {len(df)} strikes; underlying={underlying}")
    except Exception as e:
        print(f"\n✗ Primary fetch failed: {e}\n")
        if VERBOSE_FETCH:
            traceback.print_exc()
    
    # 2) Fallback to CSV if needed
    if (df is None or df.empty) and ALLOW_FALLBACK:
        if os.path.exists(FALLBACK_CSV):
            df, underlying = load_fallback_csv(FALLBACK_CSV)
        else:
            print(f"Fallback CSV not found: {FALLBACK_CSV}")
    
    if df is None or df.empty:
        print("\n✗ No usable data available. Exiting.\n")
        print("SOLUTIONS:")
        print("1. Create a fallback CSV at:", FALLBACK_CSV)
        print("2. Upgrade PythonAnywhere to paid tier")
        print("3. Use alternative hosting (Heroku, Railway, etc.)")
        return
    
    # 3) Select nearest strikes
    sel = pick_nearest(df, underlying, NEAREST_STRIKES)
    print(f"\n{'='*60}")
    print("Selected nearest strikes snapshot:")
    print('='*60)
    print(sel[['strike', 'CE_OI', 'CE_COI', 'PE_OI', 'PE_COI']].to_string(index=False))
    print('='*60 + '\n')
    
    # 4) Connect to Google Sheets
    try:
        client = sheets_client_from_service_account(SERVICE_ACCOUNT_FILE)
        print("✓ Connected to Google Sheets")
    except Exception as e:
        print(f"✗ Failed to create Sheets client: {e}")
        traceback.print_exc()
        return
    
    sh, created = open_or_create_spreadsheet(client, SPREADSHEET_NAME)
    print(f"✓ {'Created' if created else 'Opened'} spreadsheet: {SPREADSHEET_NAME}")
    
    ws_hist, _ = ensure_worksheet(sh, HISTORY_SHEET, 
        ["timestamp", "symbol", "underlying", "strike", "CE_OI", "CE_COI", "CE_IV", "PE_OI", "PE_COI", "PE_IV"])
    ws_alerts, _ = ensure_worksheet(sh, ALERTS_SHEET,
        ["timestamp", "symbol", "underlying", "event_rank", "event_type", "strike", "side", "pct_coi", "details"])
    
    # 5) Read previous snapshot
    prev_sel = None
    try:
        hist_vals = ws_hist.get_all_values()
        if len(hist_vals) > 1:
            df_hist = pd.DataFrame(hist_vals[1:], columns=hist_vals[0])
            if 'timestamp' in df_hist.columns and not df_hist.empty:
                last_ts = df_hist['timestamp'].max()
                last_snapshot = df_hist[(df_hist['timestamp'] == last_ts) & (df_hist['symbol'] == SYMBOL)]
                
                if not last_snapshot.empty:
                    prev_sel = last_snapshot[['strike', 'CE_OI', 'CE_COI', 'CE_IV', 'PE_OI', 'PE_COI', 'PE_IV']].copy()
                    prev_sel['strike'] = prev_sel['strike'].astype(float).astype(int)
                    for c in ['CE_OI', 'CE_COI', 'PE_OI', 'PE_COI']:
                        prev_sel[c] = pd.to_numeric(prev_sel[c], errors='coerce').fillna(0).astype(int)
                    print(f"✓ Found previous snapshot with {len(prev_sel)} strikes")
    except Exception as e:
        print(f"Warning: Could not read history: {e}")
    
    # 6) Prepare history rows
    hist_rows = []
    for _, r in sel.iterrows():
        hist_rows.append([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            SYMBOL, float(underlying), int(r['strike']),
            int(r['CE_OI']), int(r['CE_COI']), float(r['CE_IV']),
            int(r['PE_OI']), int(r['PE_COI']), float(r['PE_IV'])
        ])
    
    if prev_sel is None or prev_sel.empty:
        try:
            ws_hist.append_rows(hist_rows, value_input_option='USER_ENTERED')
            print(f"✓ Appended {len(hist_rows)} history rows")
            print("\nℹ️  No previous snapshot for comparison. Run again later to generate alerts.")
        except Exception as e:
            print(f"✗ Failed to append history: {e}")
        return
    
    # 7) Calculate pct_coi and generate alerts
    prev_map = prev_sel.set_index('strike').to_dict('index')
    
    rows_ce = []
    rows_pe = []
    
    for _, r in sel.iterrows():
        s = int(r['strike'])
        curr_ce_coi = int(r['CE_COI'])
        curr_ce_oi = int(r['CE_OI'])
        curr_pe_coi = int(r['PE_COI'])
        curr_pe_oi = int(r['PE_OI'])
        
        pm = prev_map.get(s)
        if pm:
            prev_ce_oi = int(pm.get('CE_OI', 0))
            prev_pe_oi = int(pm.get('PE_OI', 0))
            pct_ce = (curr_ce_coi / max(prev_ce_oi, MIN_PREV_OI_DENOM)) if curr_ce_coi > 0 else 0.0
            pct_pe = (curr_pe_coi / max(prev_pe_oi, MIN_PREV_OI_DENOM)) if curr_pe_coi > 0 else 0.0
        else:
            pct_ce = 0.0
            pct_pe = 0.0
            prev_ce_oi = 0
            prev_pe_oi = 0
        
        rows_ce.append({'strike': s, 'prev_oi': prev_ce_oi, 'curr_oi': curr_ce_oi, 'coi': curr_ce_coi, 'pct_coi': pct_ce})
        rows_pe.append({'strike': s, 'prev_oi': prev_pe_oi, 'curr_oi': curr_pe_oi, 'coi': curr_pe_coi, 'pct_coi': pct_pe})
    
    df_ce = pd.DataFrame(rows_ce).sort_values('pct_coi', ascending=False).reset_index(drop=True)
    df_pe = pd.DataFrame(rows_pe).sort_values('pct_coi', ascending=False).reset_index(drop=True)
    
    df_ce_alerts = df_ce[df_ce['pct_coi'] > MIN_POSITIVE_PCT].head(ALERT_TOP_N)
    df_pe_alerts = df_pe[df_pe['pct_coi'] > MIN_POSITIVE_PCT].head(ALERT_TOP_N)
    
    # 8) Build alert rows
    alert_rows = []
    
    for rank, (_, a) in enumerate(df_pe_alerts.iterrows(), 1):
        event_type = f'Put-writer spike (pct COI rank {rank})'
        details = f"prev_OI={int(a['prev_oi'])}, curr_COI={int(a['coi'])}, pct_coi={a['pct_coi']:.4f}"
        alert_rows.append([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            SYMBOL, float(underlying), rank, event_type,
            int(a['strike']), 'PE', a['pct_coi'], details
        ])
    
    for rank, (_, a) in enumerate(df_ce_alerts.iterrows(), 1):
        event_type = f'Call-writer spike (pct COI rank {rank})'
        details = f"prev_OI={int(a['prev_oi'])}, curr_COI={int(a['coi'])}, pct_coi={a['pct_coi']:.4f}"
        alert_rows.append([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            SYMBOL, float(underlying), rank, event_type,
            int(a['strike']), 'CE', a['pct_coi'], details
        ])
    
    # 9) Write alerts and history
    print(f"\n{'='*60}")
    if alert_rows:
        try:
            ws_alerts.append_rows(alert_rows, value_input_option='USER_ENTERED')
            print(f"✓ Logged {len(alert_rows)} alert(s) to '{ALERTS_SHEET}':")
            print('='*60)
            for row in alert_rows:
                print(f"  🚨 {row[4]} | Strike ₹{row[5]} | {row[8]}")
        except Exception as e:
            print(f"✗ Failed to write alerts: {e}")
            traceback.print_exc()
    else:
        print("ℹ️  No alerts by pct-COI ranking this run")
    
    print('='*60)
    
    try:
        ws_hist.append_rows(hist_rows, value_input_option='USER_ENTERED')
        print(f"✓ Appended {len(hist_rows)} history rows to '{HISTORY_SHEET}'")
    except Exception as e:
        print(f"✗ Failed to append history: {e}")
        traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("✓ Run completed successfully")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⏹️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Unhandled exception: {e}")
        traceback.print_exc()
        sys.exit(2)

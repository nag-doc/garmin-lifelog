import os
import csv
import json
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- Configuration ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or "1fwxyTH8BamIZlyF-k7DhJWEUDhhd0AteOipsmoo_1Xs"
DATA_DIR = "data/fit"
GID = "674362661"

# Column Mapping (Target Sheet Column Index 1-based)
COLUMN_MAP = {
    "steps": 39,
    "distance_km": 40,
    "calories": 41,
    "active_cal": 42
}

def extract_daily_stats():
    daily_stats = {}
    if not os.path.exists(DATA_DIR):
        return daily_stats
    
    # Iterate through all CSV files in data/fit/ recursively
    for root, dirs, files in os.walk(DATA_DIR):
        for f in files:
            if not f.endswith('.csv'): continue
            path = os.path.join(root, f)
            try:
                df = pd.read_csv(path, low_memory=False)
                if 'timestamp' not in df.columns: continue
                
                df['date_str'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y/%m/%d')
                
                for day_str, day_df in df.groupby('date_str'):
                    if day_str not in daily_stats:
                        daily_stats[day_str] = {"steps": 0, "distance_km": 0, "calories": 0, "active_cal": 0}
                    
                    if 'steps' in day_df.columns:
                        s = pd.to_numeric(day_df['steps'], errors='coerce').max()
                        if not pd.isna(s): daily_stats[day_str]["steps"] = max(daily_stats[day_str]["steps"], s)
                    
                    if 'distance' in day_df.columns:
                        d = pd.to_numeric(day_df['distance'], errors='coerce').max()
                        if not pd.isna(d): daily_stats[day_str]["distance_km"] = max(daily_stats[day_str]["distance_km"], d / 1000.0)
                    
                    if 'active_calories' in day_df.columns:
                        ac = pd.to_numeric(day_df['active_calories'], errors='coerce').max()
                        if not pd.isna(ac): daily_stats[day_str]["active_cal"] = max(daily_stats[day_str]["active_cal"], ac)
            except Exception as e:
                print(f"Error reading {f}: {e}")
    return daily_stats

def main():
    print("Connecting to Google Sheets...")
    json_str = os.getenv("SERVICE_ACCOUNT_JSON")
    if not json_str:
        print("Error: SERVICE_ACCOUNT_JSON missing.")
        return

    try:
        creds_dict = json.loads(json_str)
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.Client(auth=creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        worksheet = next((ws for ws in sh.worksheets() if str(ws.id) == GID), sh.get_worksheet(0))
        print(f"Connected to: {worksheet.title}")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    daily_data = extract_daily_stats()
    if not daily_data:
        print("No CSV data found to process.")
        return

    # Get existing dates in column 1
    existing_vals = worksheet.col_values(1)
    existing_dates = {v: i+1 for i, v in enumerate(existing_vals) if v}

    print(f"Processing {len(daily_data)} days...")
    for date_str, stats in daily_data.items():
        row_num = existing_dates.get(date_str)
        if not row_num:
            print(f"[{date_str}] Row not found. Skipping.")
            continue
        
        updates = []
        for key, col_idx in COLUMN_MAP.items():
            val = stats.get(key)
            if val is not None and val != 0:
                updates.append(gspread.Cell(row_num, col_idx, val))
        
        if updates:
            worksheet.update_cells(updates, value_input_option="USER_ENTERED")
            print(f"[{date_str}] Updated")
        time.sleep(1)

if __name__ == "__main__":
    main()

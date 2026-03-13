#!/usr/bin/env python3
import os
import sys
import json
import base64
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, date, timedelta, timezone
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or "1t-GDPqivzTPchEQt-KS3nj9uaJjX7wc5tcWoIzQS1cE"

def toggl_request(api_token, path, params=None):
    url = f"https://api.track.toggl.com/api/v9/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    
    req = urllib.request.Request(url)
    auth_str = f"{api_token}:api_token"
    auth_b64 = base64.b64encode(auth_str.encode('ascii')).decode('ascii')
    req.add_header('Authorization', f'Basic {auth_b64}')
    req.add_header('Content-Type', 'application/json')
    
    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                return json.loads(response.read().decode('utf-8'))
            else:
                print(f"Error HTTP {response.status}: {response.read()}")
                return None
    except urllib.error.URLError as e:
        print(f"Request Error to {url}: {e}")
        return None

def fetch_toggl_data(api_token, target_date):
    print(f"[{target_date}] Fetching Toggl Track data...", end=" ", flush=True)
    
    # Calculate start and end of the target day in UTC
    # Since we want local day, we need to consider the timezone context.
    # Assuming JST (+09:00).
    jst_zone = timezone(timedelta(hours=9))
    start_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=jst_zone)
    end_dt = start_dt + timedelta(days=1)
    
    start_date_str = start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date_str = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    entries = toggl_request(api_token, "me/time_entries", {
        "start_date": start_date_str,
        "end_date": end_date_str
    })
    
    if entries is None:
        print("Failed to fetch time entries.")
        return []
        
    print(f"Found {len(entries)} entries.", end=" ", flush=True)
    
    # Needs to get project names
    # Collect unique workspace and project IDs
    workspaces_to_fetch = set()
    for entry in entries:
        wid = entry.get("workspace_id")
        pid = entry.get("project_id")
        if wid and pid:
            workspaces_to_fetch.add((wid, pid))
            
    # Fetch projects to map IDs to Names
    project_map = {}
    fetched_workspaces = set()
    for wid, pid in workspaces_to_fetch:
        if wid not in fetched_workspaces:
            projects = toggl_request(api_token, f"workspaces/{wid}/projects")
            if projects:
                for p in projects:
                    project_map[p["id"]] = p["name"]
            fetched_workspaces.add(wid)
    
    # Aggregate data by (project_id, description)
    # Toggl running tasks have negative duration until stopped.
    # We ignore currently running tasks or calculate them using current time.
    aggregated = {}
    
    for entry in entries:
        desc = entry.get("description") or "No Description"
        pid = entry.get("project_id")
        pname = project_map.get(pid, "No Project")
        duration = entry.get("duration", 0)
        
        # If duration is negative, task is currently running
        if duration < 0:
            start_time = datetime.fromisoformat(entry["start"].replace("Z", "+00:00"))
            now_utc = datetime.now(timezone.utc)
            duration = int((now_utc - start_time).total_seconds())
            
        key = (pname, desc)
        if key not in aggregated:
            aggregated[key] = 0
        aggregated[key] += duration
        
    print("Aggregated successfully.")
    
    # Convert aggregated data to format ready for sheets
    # [Date, Project Name, Task Description, Duration (Minutes)]
    results = []
    for (pname, desc), dur_seconds in aggregated.items():
        dur_minutes = round(dur_seconds / 60)
        results.append([
            target_date.isoformat(),
            pname,
            desc,
            dur_minutes
        ])
        
    return results

def write_to_sheet(worksheet, date_str, results):
    if not results:
        print("  -> Skip (No Data)")
        return
        
    try:
        # For Toggl, we will just append rows so we have a log of each project/task per day
        # We check if there are already entries for this date to avoid duplicates if run multiple times
        vals = worksheet.col_values(1)
        existing_rows_for_date = [i + 1 for i, v in enumerate(vals) if v == date_str]
        
        if existing_rows_for_date:
            print(f"  -> Data for {date_str} already exists. Deleting {len(existing_rows_for_date)} old rows before inserting new ones...")
            # Delete from bottom up to avoid shifting index issues
            for row_num in sorted(existing_rows_for_date, reverse=True):
                worksheet.delete_rows(row_num)
                
        # Append new rows
        worksheet.append_rows(results, value_input_option="USER_ENTERED")
        print(f"  -> Appended {len(results)} rows to Toggl sheet")
    except Exception as e:
        print(f"  -> Write Error: {e}")

def main():
    print("--- Starting Toggl Fetch ---")
    
    toggl_token = os.getenv("TOGGL_API_TOKEN")
    if not toggl_token:
        print("Error: TOGGL_API_TOKEN environment variable is missing.")
        sys.exit(1)

    json_str = os.getenv("SERVICE_ACCOUNT_JSON")
    if not json_str:
        print("Error: SERVICE_ACCOUNT_JSON environment variable is missing.")
        sys.exit(1)
    
    try:
        creds_dict = json.loads(json_str)
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        # using the second worksheet (index 1) for Toggl data
        sheet = gc.open_by_key(SPREADSHEET_ID)
        try:
            worksheet = sheet.get_worksheet(1)
            if worksheet is None:
                # If worksheet doesn't exist, create it
                worksheet = sheet.add_worksheet(title="Toggl", rows="1000", cols="4")
                # Add headers
                worksheet.update('A1:D1', [["Date", "Project", "Task", "Duration (Minutes)"]])
        except Exception as e:
            print(f"Error accessing sheet index 1, attempting to create one: {e}")
            worksheet = sheet.add_worksheet(title="Toggl", rows="1000", cols="4")
            worksheet.update('A1:D1', [["Date", "Project", "Task", "Duration (Minutes)"]])

    except Exception as e:
        print(f"Sheets connection failed: {e}")
        sys.exit(1)

    # Run at 23:59 JST, so target is today
    targets = [date.today()]
    
    for d_obj in targets:
        d_str = d_obj.isoformat()
        try:
            results = fetch_toggl_data(toggl_token, d_obj)
            write_to_sheet(worksheet, d_str, results)
        except Exception as e:
            print(f"[{d_str}] Fatal Error: {e}")

    print("--- Done ---")

if __name__ == "__main__":
    main()

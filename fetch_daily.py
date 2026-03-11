#!/usr/bin/env python3
import os
import sys
import time
import json
from datetime import datetime, date, timedelta
from garminconnect import Garmin
import gspread
from google.oauth2.service_account import Credentials

# --- 設定 ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or "1t-GDPqivzTPchEQt-KS3nj9uaJjX7wc5tcWoIzQS1cE"
SLEEP_BETWEEN_API = 2.0

# --- ヘルパー関数 ---
def safe_get(func, *args, default=None, **kwargs):
    try:
        result = func(*args, **kwargs)
        time.sleep(SLEEP_BETWEEN_API)
        return result if result is not None else (default if default is not None else {})
    except Exception as e:
        print(f" (API Error: {e})")
        return default if default is not None else {}

# --- VO2Max取得（堅牢版） ---
def fetch_vo2max_robust(garmin, date_str):
    """
    VO2Maxを複数の方法で試行して取得
    戻り値: (vo2max_run, vo2max_cycling, method_string)
    """
    vo2max_run = 0
    vo2max_cycling = 0
    method_used = "未取得"
    
    # 方法1: get_max_metrics を試す
    try:
        max_metrics = garmin.get_max_metrics(date_str)
        time.sleep(SLEEP_BETWEEN_API)
        
        if isinstance(max_metrics, dict):
            # パターンA: 直接 vo2MaxValue がある
            if "vo2MaxValue" in max_metrics and max_metrics["vo2MaxValue"]:
                vo2max_run = max_metrics["vo2MaxValue"]
                method_used = "get_max_metrics:direct"
            
            # パターンB: generic キー内
            elif "generic" in max_metrics and isinstance(max_metrics["generic"], dict):
                generic = max_metrics["generic"]
                if generic.get("vo2MaxValue"):
                    vo2max_run = generic["vo2MaxValue"]
                    method_used = "get_max_metrics:generic"
            
            # パターンC: running キー内
            if "running" in max_metrics and isinstance(max_metrics["running"], dict):
                running = max_metrics["running"]
                if running.get("vo2MaxValue"):
                    vo2max_run = running["vo2MaxValue"]
                    if method_used == "未取得":
                        method_used = "get_max_metrics:running"
            
            # パターンD: cycling キー内
            if "cycling" in max_metrics and isinstance(max_metrics["cycling"], dict):
                cycling = max_metrics["cycling"]
                if cycling.get("vo2MaxValue"):
                    vo2max_cycling = cycling["vo2MaxValue"]
                    if method_used == "未取得":
                        method_used = "get_max_metrics:cycling"
                        
    except Exception as e:
        pass
    
    # 方法2: 直接APIエンドポイントを叩く（方法1が失敗した場合）
    if not vo2max_run and not vo2max_cycling:
        try:
            url = f"/metrics-service/metrics/maxmetrics/{garmin.display_name}"
            direct_data = garmin.connectapi(url, params={"calendarDate": date_str})
            time.sleep(SLEEP_BETWEEN_API)
            
            if isinstance(direct_data, dict):
                # maxMetrics キーが入れ子の場合
                if "maxMetrics" in direct_data:
                    inner = direct_data["maxMetrics"]
                    if isinstance(inner, dict):
                        vo2max_run = inner.get("vo2MaxValue", inner.get("vo2Max", 0))
                        if vo2max_run:
                            method_used = "direct_api:maxMetrics"
                
                # リスト形式の場合
                elif isinstance(direct_data.get("maxMetrics"), list):
                    for item in direct_data["maxMetrics"]:
                        if isinstance(item, dict):
                            if item.get("key") == "generic" or item.get("type") == "running":
                                vo2max_run = item.get("value", item.get("vo2MaxValue", 0))
                                if vo2max_run:
                                    method_used = "direct_api:list_generic"
                            elif item.get("key") == "cycling":
                                vo2max_cycling = item.get("value", item.get("vo2MaxValue", 0))
                                
        except Exception:
            pass
    
    # 方法3: ユーザープロファイルから取得（最終手段）
    if not vo2max_run:
        try:
            profile = garmin.get_user_profile()
            time.sleep(SLEEP_BETWEEN_API)
            if isinstance(profile, dict):
                if "vo2MaxValue" in profile:
                    vo2max_run = profile["vo2MaxValue"]
                    method_used = "user_profile"
                elif "currentVO2Max" in profile:
                    vo2max_run = profile["currentVO2Max"]
                    method_used = "user_profile:current"
        except:
            pass
    
    # デバッグ出力
    if vo2max_run or vo2max_cycling:
        print(f" [VO2Max={vo2max_run}/{vo2max_cycling} via {method_used}]", end="")
    else:
        print(f" [VO2Max未取得]", end="")
    
    return vo2max_run, vo2max_cycling, method_used

# --- 1日分のデータ取得 ---
def fetch_day_data(garmin, date_str):
    print(f"[{date_str}] Fetching...", end=" ", flush=True)
    
    # 1. Sleep
    wakeup_time = bed_time = ""
    total_score = deep_min = light_min = rem_min = awake_min = 0
    try:
        sleep = garmin.get_sleep_data(date_str)
        time.sleep(SLEEP_BETWEEN_API)
        dto = sleep.get("dailySleepDTO", {}) if isinstance(sleep, dict) else {}
        
        if dto.get("sleepEndTimestampLocal"):
            wakeup_time = datetime.fromtimestamp(dto["sleepEndTimestampLocal"] / 1000).strftime("%H:%M")
        if dto.get("sleepStartTimestampLocal"):
            bed_time = datetime.fromtimestamp(dto["sleepStartTimestampLocal"] / 1000).strftime("%H:%M")
        
        sleep_scores = dto.get("sleepScores", {}) or {}
        overall = sleep_scores.get("overall", {}) or {}
        total_score = overall.get("value", 0) or 0
        
        deep_min = (dto.get("deepSleepSeconds") or 0) // 60
        light_min = (dto.get("lightSleepSeconds") or 0) // 60
        rem_min = (dto.get("remSleepSeconds") or 0) // 60
        awake_min = (dto.get("awakeSleepSeconds") or 0) // 60
    except Exception:
        pass

    # 2. User Summary
    stats = {}
    try:
        url = f"/usersummary-service/usersummary/daily/{garmin.display_name}"
        stats = garmin.connectapi(url, params={"calendarDate": date_str})
        time.sleep(SLEEP_BETWEEN_API)
    except Exception:
        pass
    if not isinstance(stats, dict): stats = {}

    steps = stats.get("totalSteps", 0) or 0
    distance_km = round((stats.get("totalDistanceMeters", 0) or 0) / 1000, 2)
    calories = stats.get("totalKilocalories", 0) or 0
    active_cal = stats.get("activeKilocalories", 0) or 0
    floors = stats.get("floorsClimbed", 0) or 0
    moderate_min = stats.get("moderateIntensityMinutes", 0) or 0
    vigorous_min = stats.get("vigorousIntensityMinutes", 0) or 0
    intensity_min = moderate_min + vigorous_min

    # 3. VO2Max（堅牢版）
    vo2max, vo2max_cycling, _ = fetch_vo2max_robust(garmin, date_str)

    # 4. Heart Rate
    hr = {}
    try:
        url = f"/wellness-service/wellness/dailyHeartRate/{garmin.display_name}"
        hr = garmin.connectapi(url, params={"date": date_str})
        time.sleep(SLEEP_BETWEEN_API)
    except Exception:
        pass
    if not isinstance(hr, dict): hr = {}
    
    resting_hr = hr.get("restingHeartRate", 0) or 0
    max_hr = hr.get("maxHeartRate", 0) or 0
    hr_values = hr.get("heartRateValues", []) or []
    valid_hr = [v[1] for v in hr_values if isinstance(v, (list, tuple)) and len(v) > 1 and v[1] and v[1] > 0]
    avg_hr = round(sum(valid_hr) / len(valid_hr)) if valid_hr else 0

    # 5. Body Battery
    bb_max = bb_min = 0
    try:
        bb_data = garmin.get_body_battery(date_str) or []
        time.sleep(SLEEP_BETWEEN_API)
        bb_values = []
        if isinstance(bb_data, list):
            for day_item in bb_data:
                if isinstance(day_item, dict):
                    vals_array = day_item.get("bodyBatteryValuesArray", []) or []
                    for pair in vals_array:
                        if isinstance(pair, list) and len(pair) > 1 and pair[1] is not None:
                            bb_values.append(pair[1])
        if bb_values:
            bb_max = max(bb_values)
            bb_min = min(bb_values)
    except Exception:
        pass

    # 6. Stress
    stress = safe_get(garmin.get_stress_data, date_str, default={})
    if not isinstance(stress, dict): stress = {}
    stress_avg = stress.get("overallStressLevel", 0) or 0
    stress_max = stress.get("maxStressLevel", 0) or 0

    # 7. SpO2
    spo2 = safe_get(garmin.get_spo2_data, date_str, default={}) or {}
    spo2_avg = 0
    if isinstance(spo2, dict):
        spo2_avg = spo2.get("averageSpO2", 0) or 0
        if not spo2_avg: spo2_avg = spo2.get("lastSevenDaysAvgSPO2", 0) or 0

    # 8. HRV
    hrv = safe_get(garmin.get_hrv_data, date_str, default={}) or {}
    hrv_value = 0
    if isinstance(hrv, dict):
        hrv_summary = hrv.get("hrvSummary", {}) or {}
        hrv_value = hrv_summary.get("lastNightAvg", 0) or 0
        if not hrv_value: hrv_value = hrv_summary.get("weeklyAvg", 0) or 0

    # 9. Respiration
    resp = safe_get(garmin.get_respiration_data, date_str, default={})
    if not isinstance(resp, dict): resp = {}
    resp_avg = resp.get("avgWakingRespirationValue", 0) or 0

    # 10. Body Composition
    body = safe_get(garmin.get_body_composition, date_str, default={}) or {}
    weight_kg = body_fat = 0
    if isinstance(body, dict):
        total_avg = body.get("totalAverage", {}) or {}
        w = total_avg.get("weight", 0) or 0
        weight_kg = round(w / 1000, 1) if w else 0
        body_fat = round(total_avg.get("bodyFat", 0) or 0, 1)
    
    # 結果出力
    print(f" Steps={steps}, Sleep={total_score}, BB={bb_max}")
    
    return {
        "wakeup_time": wakeup_time, "bed_time": bed_time, "total_score": total_score,
        "early_wakeup": 1 if wakeup_time and wakeup_time < "06:00" else 0,
        "deep_min": deep_min, "light_min": light_min, "rem_min": rem_min, "awake_min": awake_min,
        "steps": steps, "distance_km": distance_km, "calories": calories, "active_cal": active_cal,
        "floors": floors, "intensity_min": intensity_min, 
        "vo2max": vo2max, "vo2max_cycling": vo2max_cycling,
        "resting_hr": resting_hr, "max_hr": max_hr, "avg_hr": avg_hr,
        "bb_max": bb_max, "bb_min": bb_min, "stress_avg": stress_avg, "stress_max": stress_max,
        "spo2_avg": spo2_avg, "hrv_value": hrv_value, "resp_avg": resp_avg,
        "weight_kg": weight_kg, "body_fat": body_fat
    }

# --- スプレッドシート書き込み ---
def write_to_sheet(worksheet, date_str, data, existing_dates):
    if data["steps"] == 0 and data["total_score"] == 0 and data["bb_max"] == 0:
        print(" -> Skip (No Data)")
        return

    # 列マッピング（57列目=VO2Max, 58列目=VO2Max Cycling）
    data_map = {
        8: data["wakeup_time"], 9: data["bed_time"], 10: data["total_score"], 11: data["early_wakeup"],
        35: data["deep_min"], 36: data["light_min"], 37: data["rem_min"], 38: data["awake_min"],
        39: data["steps"], 40: data["distance_km"], 41: data["calories"], 42: data["active_cal"],
        43: data["floors"], 44: data["intensity_min"], 45: data["resting_hr"], 46: data["max_hr"],
        47: data["avg_hr"], 48: data["bb_max"], 49: data["bb_min"], 50: data["stress_avg"],
        51: data["stress_max"], 52: data["spo2_avg"], 53: data["hrv_value"], 54: data["resp_avg"],
        55: data["weight_kg"], 56: data["body_fat"], 57: data["vo2max"], 58: data["vo2max_cycling"]
    }

    try:
        if date_str in existing_dates:
            row_num = existing_dates[date_str]
            cells = [gspread.Cell(row_num, col, val) for col, val in data_map.items()]
            worksheet.update_cells(cells)
            print("  -> Updated")
        else:
            row_values = [""] * 58
            row_values[0] = date_str
            for col, val in data_map.items():
                row_values[col-1] = val
            worksheet.append_row(row_values, value_input_option="USER_ENTERED")
            existing_dates[date_str] = len(existing_dates) + 2
            print("  -> Appended")
    except Exception as e:
        print(f"  -> Write Error: {e}")

# --- VO2Maxバックフィル機能 ---
def backfill_vo2max(garmin, worksheet, days_back=30):
    """過去のVO2Maxデータを一括で埋める"""
    print(f"\n=== VO2Maxバックフィル開始（過去{days_back}日分）===\n")
    
    # 既存の日付マップを取得
    vals = worksheet.col_values(1)
    existing = {v: i+1 for i, v in enumerate(vals) if len(v) >= 10}
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    updated_count = 0
    empty_count = 0
    
    current = start_date
    while current <= end_date:
        date_str = current.isoformat()
        
        if date_str not in existing:
            current += timedelta(days=1)
            continue
            
        row_num = existing[date_str]
        
        # 現在のVO2Max値をチェック（57列目）
        try:
            current_val = worksheet.cell(row_num, 57).value
            if current_val and str(current_val) not in ["", "0"]:
                print(f"[{date_str}] 既にVO2Max={current_val} → スキップ")
                current += timedelta(days=1)
                continue
        except:
            pass
        
        # VO2Maxを取得・更新
        try:
            vo2max, vo2max_cycling, method = fetch_vo2max_robust(garmin, date_str)
            
            if vo2max or vo2max_cycling:
                # セルを個別に更新（高速化のためupdate_cellsを使わない）
                if vo2max:
                    worksheet.update_cell(row_num, 57, vo2max)
                if vo2max_cycling:
                    worksheet.update_cell(row_num, 58, vo2max_cycling)
                print(f"  → 更新完了（方法: {method}）")
                updated_count += 1
            else:
                print(f"  → Garmin上にデータなし")
                empty_count += 1
                
        except Exception as e:
            print(f"  → エラー: {e}")
        
        current += timedelta(days=1)
        time.sleep(0.5)  # API負荷軽減
    
    print(f"\n=== 完了 ===")
    print(f"更新した日数: {updated_count}")
    print(f"データなし: {empty_count}")

# --- 欠損データチェック機能 ---
def check_missing_data(worksheet):
    """スプレッドシートの欠損データをチェック"""
    print("=== データ整合性チェック ===\n")
    
    # 全データ取得
    all_values = worksheet.get_all_values()
    if len(all_values) <= 1:
        print("データが見つかりません")
        return
    
    data_rows = all_values[1:]
    print(f"総行数: {len(data_rows)} 日分\n")
    
    # チェック対象列
    columns = {
        57: "VO2Max",
        58: "VO2Max Cycling", 
        10: "Sleep Score",
        39: "Steps",
        48: "Body Battery"
    }
    
    missing_vo2max = []
    
    for i, row in enumerate(data_rows, start=2):
        if len(row) < 2:
            continue
        date_str = row[0] if row[0] else f"Row{i}"
        
        # VO2Maxチェック（57列目）
        if len(row) < 57 or not row[56] or row[56] == "0":
            missing_vo2max.append(date_str)
    
    # レポート
    if missing_vo2max:
        print(f"【VO2Max未入力】{len(missing_vo2max)} 日分")
        print(f"  最新10件: {', '.join(missing_vo2max[-10:])}")
    else:
        print("【VO2Max】すべて入力済み ✅")
    
    # 他の項目も簡易チェック
    for col_num, col_name in columns.items():
        if col_num == 57:
            continue
        missing = sum(1 for row in data_rows if len(row) < col_num or not row[col_num-1] or row[col_num-1] == "0")
        if missing:
            print(f"【{col_name}】未入力: {missing} 日")
        else:
            print(f"【{col_name}】すべて入力済み ✅")

# --- メイン処理 ---
def main():
    mode = os.getenv("RUN_MODE", "daily")  # daily, backfill, check
    print(f"--- モード: {mode} ---")
    
    # 環境変数チェック
    token_str = os.getenv("GARMIN_TOKENS")
    if not token_str:
        print("Error: GARMIN_TOKENS missing")
        sys.exit(1)
    
    json_str = os.getenv("SERVICE_ACCOUNT_JSON")
    if not json_str:
        print("Error: SERVICE_ACCOUNT_JSON missing")
        sys.exit(1)
    
    # Garminログイン
    try:
        garmin = Garmin()
        garmin.login(token_str)
        try:
            profile = garmin.connectapi("/userprofile-service/userprofile/profile")
            if profile and "displayName" in profile:
                garmin.display_name = profile["displayName"]
        except:
            pass
        print(f"Logged in as: {garmin.display_name}")
    except Exception as e:
        print(f"Login failed: {e}")
        sys.exit(1)
    
    # Google Sheets接続
    try:
        creds_dict = json.loads(json_str)
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        worksheet = gc.open_by_key(SPREADSHEET_ID).get_worksheet(0)
    except Exception as e:
        print(f"Sheets connection failed: {e}")
        sys.exit(1)
    
    # モード別処理
    if mode == "backfill":
        # バックフィルモード: 過去のVO2Maxを埋める
        days = int(os.getenv("BACKFILL_DAYS", "30"))
        backfill_vo2max(garmin, worksheet, days)
        
    elif mode == "check":
        # チェックモード: 欠損データを確認
        check_missing_data(worksheet)
        
    else:
        # デイリーモード: 昨日と今日のデータを取得
        vals = worksheet.col_values(1)
        existing = {v: i+1 for i, v in enumerate(vals) if len(v) >= 10}
        
        targets = [date.today() - timedelta(days=1), date.today()]
        
        for d_obj in targets:
            d_str = d_obj.isoformat()
            try:
                dat = fetch_day_data(garmin, d_str)
                write_to_sheet(worksheet, d_str, dat, existing)
            except Exception as e:
                print(f"[{d_str}] Fatal Error: {e}")
    
    print("--- Done ---")

if __name__ == "__main__":
    main()

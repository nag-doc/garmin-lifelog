#!/usr/bin/env python3
import os
import sys
import time
import json
import tempfile
import traceback
from datetime import datetime, date, timedelta
from garminconnect import Garmin
import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or "1t-GDPqivzTPchEQt-KS3nj9uaJjX7wc5tcWoIzQS1cE"
SLEEP_BETWEEN_API = 2.0

def safe_get(func, *args, default=None, **kwargs):
    try:
        result = func(*args, **kwargs)
        time.sleep(SLEEP_BETWEEN_API)
        return result if result is not None else (default if default is not None else {})
    except Exception as e:
        return default if default is not None else {}

def fetch_sleep_robust(garmin, date_str):
    """
    睡眠データを複数の方法で堅牢に取得
    戻り値: (wakeup_time, bed_time, total_score, deep_min, light_min, rem_min, awake_min)
    """
    wakeup_time = bed_time = ""
    total_score = deep_min = light_min = rem_min = awake_min = 0
    
    # 方法1: get_sleep_data を試す
    try:
        sleep = garmin.get_sleep_data(date_str)
        time.sleep(SLEEP_BETWEEN_API)
        
        if isinstance(sleep, dict):
            dto = sleep.get("dailySleepDTO", {})
            
            if isinstance(dto, dict):
                # 起床時間（複数のキーをチェック）
                end_ts = dto.get("sleepEndTimestampLocal") or dto.get("sleepEndTimestamp") or dto.get("endTimestamp")
                if end_ts:
                    # ミリ秒（13桁）と秒（10桁）の混在に対応
                    if len(str(int(end_ts))) == 13:
                        wakeup_time = datetime.fromtimestamp(end_ts / 1000).strftime("%H:%M")
                    else:
                        wakeup_time = datetime.fromtimestamp(end_ts).strftime("%H:%M")
                
                # 就寝時間
                start_ts = dto.get("sleepStartTimestampLocal") or dto.get("sleepStartTimestamp") or dto.get("startTimestamp")
                if start_ts:
                    if len(str(int(start_ts))) == 13:
                        bed_time = datetime.fromtimestamp(start_ts / 1000).strftime("%H:%M")
                    else:
                        bed_time = datetime.fromtimestamp(start_ts).strftime("%H:%M")
                
                # スコア
                sleep_scores = dto.get("sleepScores", {}) or {}
                overall = sleep_scores.get("overall", {}) or {}
                total_score = overall.get("value", 0) or overall.get("score", 0) or 0
                
                # 睡眠段階（秒→分）
                deep_min = (dto.get("deepSleepSeconds") or dto.get("deepSleepDuration", 0)) // 60
                light_min = (dto.get("lightSleepSeconds") or dto.get("lightSleepDuration", 0)) // 60
                rem_min = (dto.get("remSleepSeconds") or dto.get("remSleepDuration", 0)) // 60
                awake_min = (dto.get("awakeSleepSeconds") or dto.get("awakeDuration", 0)) // 60
                
                if wakeup_time or bed_time:
                    return wakeup_time, bed_time, total_score, deep_min, light_min, rem_min, awake_min
    except Exception:
        pass
    
    # 方法2: get_sleep（別エンドポイント）を試す
    try:
        sleep_alt = garmin.get_sleep(date_str)
        time.sleep(SLEEP_BETWEEN_API)
        
        if isinstance(sleep_alt, dict):
            # 開始・終了時刻を探す
            start = sleep_alt.get("startTime") or sleep_alt.get("sleepStart")
            end = sleep_alt.get("endTime") or sleep_alt.get("sleepEnd")
            
            if start:
                try:
                    # ISO形式やUNIXタイムスタンプに対応
                    if isinstance(start, str) and 'T' in start:
                        dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                        bed_time = dt.strftime("%H:%M")
                    else:
                        ts = int(start)
                        if len(str(ts)) == 13:
                            bed_time = datetime.fromtimestamp(ts / 1000).strftime("%H:%M")
                        else:
                            bed_time = datetime.fromtimestamp(ts).strftime("%H:%M")
                except:
                    pass
            
            if end:
                try:
                    if isinstance(end, str) and 'T' in end:
                        dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                        wakeup_time = dt.strftime("%H:%M")
                    else:
                        ts = int(end)
                        if len(str(ts)) == 13:
                            wakeup_time = datetime.fromtimestamp(ts / 1000).strftime("%H:%M")
                        else:
                            wakeup_time = datetime.fromtimestamp(ts).strftime("%H:%M")
                except:
                    pass
            
            # スコアと睡眠段階
            total_score = sleep_alt.get("sleepScore", 0) or sleep_alt.get("score", 0)
            deep_min = (sleep_alt.get("deepDuration", 0)) // 60
            light_min = (sleep_alt.get("lightDuration", 0)) // 60
            rem_min = (sleep_alt.get("remDuration", 0)) // 60
            awake_min = (sleep_alt.get("awakeDuration", 0)) // 60
            
            if wakeup_time or bed_time:
                return wakeup_time, bed_time, total_score, deep_min, light_min, rem_min, awake_min
    except Exception:
        pass
    
    # 方法3: 直接API（wellness-service）
    try:
        url = f"/wellness-service/wellness/dailySleepData/{garmin.display_name}"
        params = {"date": date_str}
        direct_data = garmin.connectapi(url, params=params)
        time.sleep(SLEEP_BETWEEN_API)
        
        if isinstance(direct_data, dict):
            dto = direct_data.get("dailySleepDTO", {})
            if not dto:
                dto = direct_data  # 直接dtoが返る場合もある
            
            if isinstance(dto, dict):
                end_ts = dto.get("sleepEndTimestampLocal")
                start_ts = dto.get("sleepStartTimestampLocal")
                
                if end_ts:
                    if len(str(int(end_ts))) == 13:
                        wakeup_time = datetime.fromtimestamp(end_ts / 1000).strftime("%H:%M")
                    else:
                        wakeup_time = datetime.fromtimestamp(end_ts).strftime("%H:%M")
                
                if start_ts:
                    if len(str(int(start_ts))) == 13:
                        bed_time = datetime.fromtimestamp(start_ts / 1000).strftime("%H:%M")
                    else:
                        bed_time = datetime.fromtimestamp(start_ts).strftime("%H:%M")
                
                sleep_scores = dto.get("sleepScores", {}) or {}
                total_score = sleep_scores.get("overall", {}).get("value", 0) if isinstance(sleep_scores.get("overall"), dict) else 0
                
                deep_min = (dto.get("deepSleepSeconds", 0)) // 60
                light_min = (dto.get("lightSleepSeconds", 0)) // 60
                rem_min = (dto.get("remSleepSeconds", 0)) // 60
                awake_min = (dto.get("awakeSleepSeconds", 0)) // 60
                
                if wakeup_time or bed_time:
                    return wakeup_time, bed_time, total_score, deep_min, light_min, rem_min, awake_min
    except Exception:
        pass
    
    return wakeup_time, bed_time, total_score, deep_min, light_min, rem_min, awake_min

def fetch_vo2max(garmin, date_str):
    vo2max_run = 0
    vo2max_cycling = 0
    try:
        max_metrics = garmin.get_max_metrics(date_str)
        time.sleep(SLEEP_BETWEEN_API)
        if isinstance(max_metrics, list) and len(max_metrics) > 0:
            max_metrics = max_metrics[0]
        if isinstance(max_metrics, dict):
            if "generic" in max_metrics and isinstance(max_metrics["generic"], dict):
                generic = max_metrics["generic"]
                vo2max_run = generic.get("vo2MaxValue") or generic.get("vo2MaxPreciseValue", 0)
            if "cycling" in max_metrics and isinstance(max_metrics["cycling"], dict):
                cycling = max_metrics["cycling"]
                vo2max_cycling = cycling.get("vo2MaxValue") or cycling.get("vo2MaxPreciseValue", 0)
    except Exception:
        pass
    return vo2max_run, vo2max_cycling

def fetch_stress_robust(garmin, date_str):
    stress_avg = 0
    stress_max = 0
    try:
        stress = garmin.get_stress_data(date_str)
        time.sleep(SLEEP_BETWEEN_API)
        if isinstance(stress, dict):
            stress_avg = stress.get("overallStressLevel") or stress.get("averageStressLevel") or stress.get("avgStressLevel", 0)
            stress_max = stress.get("maxStressLevel") or stress.get("peakStressLevel", 0)
            if stress_avg:
                return stress_avg, stress_max
    except Exception:
        pass
    
    try:
        url = f"/wellness-service/wellness/dailyStress/{garmin.display_name}"
        data = garmin.connectapi(url, params={"date": date_str})
        time.sleep(SLEEP_BETWEEN_API)
        if isinstance(data, dict):
            stress_avg = data.get("averageStressLevel") or data.get("overallStressLevel", 0)
            stress_max = data.get("maxStressLevel", 0)
    except Exception:
        pass
    
    return stress_avg, stress_max

def fetch_weight_robust(garmin, date_str):
    weight_kg = 0
    body_fat = 0
    try:
        body = garmin.get_body_composition(date_str)
        time.sleep(SLEEP_BETWEEN_API)
        if isinstance(body, dict):
            if "totalAverage" in body and isinstance(body["totalAverage"], dict):
                total_avg = body["totalAverage"]
                w = total_avg.get("weight", 0)
                if w:
                    weight_kg = round(w / 1000, 1)
                body_fat = round(total_avg.get("bodyFat", 0) or 0, 1)
                if weight_kg:
                    return weight_kg, body_fat
            
            if "latestWeight" in body and isinstance(body["latestWeight"], dict):
                latest = body["latestWeight"]
                w = latest.get("weight", 0)
                if w:
                    weight_kg = round(w / 1000, 1)
                body_fat = round(latest.get("bodyFat", 0) or 0, 1)
                if weight_kg:
                    return weight_kg, body_fat
            
            w = body.get("weight", 0)
            if w and isinstance(w, (int, float)):
                if w > 1000:
                    weight_kg = round(w / 1000, 1)
                else:
                    weight_kg = round(w, 1)
                body_fat = round(body.get("bodyFat", 0) or 0, 1)
    except Exception:
        pass
    
    if not weight_kg:
        try:
            stats = garmin.get_user_summary(date_str)
            time.sleep(SLEEP_BETWEEN_API)
            if isinstance(stats, dict):
                w = stats.get("latestWeight", 0)
                if w:
                    if isinstance(w, dict):
                        weight_val = w.get("weight", 0)
                        if weight_val > 1000:
                            weight_kg = round(weight_val / 1000, 1)
                        else:
                            weight_kg = round(weight_val, 1)
                    else:
                        weight_kg = round(w / 1000 if w > 1000 else w, 1)
        except Exception:
            pass
    
    return weight_kg, body_fat

def fetch_floors_robust(stats, garmin, date_str):
    floors = stats.get("floorsClimbed", 0) or 0
    if not floors:
        try:
            url = f"/usersummary-service/usersummary/daily/{garmin.display_name}"
            data = garmin.connectapi(url, params={"calendarDate": date_str})
            time.sleep(SLEEP_BETWEEN_API)
            if isinstance(data, dict):
                floors = data.get("floorsClimbed") or data.get("totalFloorsClimbed", 0)
        except Exception:
            pass
    
    if not floors:
        try:
            activities = garmin.get_activities_by_date(date_str, date_str)
            time.sleep(SLEEP_BETWEEN_API)
            total_floors = 0
            for act in activities:
                if isinstance(act, dict):
                    total_floors += act.get("floorsClimbed", 0) or 0
            if total_floors:
                floors = total_floors
        except Exception:
            pass
    
    return floors

def fetch_day_data(garmin, date_str):
    print(f"[{date_str}] Fetching...", end=" ", flush=True)
    
    # 睡眠データ（堅牢版）
    wakeup_time, bed_time, total_score, deep_min, light_min, rem_min, awake_min = fetch_sleep_robust(garmin, date_str)
    
    # User Summary
    stats = {}
    try:
        url = f"/usersummary-service/usersummary/daily/{garmin.display_name}"
        stats = garmin.connectapi(url, params={"calendarDate": date_str})
        time.sleep(SLEEP_BETWEEN_API)
    except Exception:
        pass
    if not isinstance(stats, dict): 
        stats = {}

    steps = stats.get("totalSteps", 0) or 0
    distance_km = round((stats.get("totalDistanceMeters", 0) or 0) / 1000, 2)
    calories = stats.get("totalKilocalories", 0) or 0
    active_cal = stats.get("activeKilocalories", 0) or 0
    moderate_min = stats.get("moderateIntensityMinutes", 0) or 0
    vigorous_min = stats.get("vigorousIntensityMinutes", 0) or 0
    intensity_min = moderate_min + vigorous_min

    # VO2Max
    vo2max, vo2max_cycling = fetch_vo2max(garmin, date_str)

    # Heart Rate
    hr = {}
    try:
        url = f"/wellness-service/wellness/dailyHeartRate/{garmin.display_name}"
        hr = garmin.connectapi(url, params={"date": date_str})
        time.sleep(SLEEP_BETWEEN_API)
    except Exception:
        pass
    if not isinstance(hr, dict): 
        hr = {}
    
    resting_hr = hr.get("restingHeartRate", 0) or 0
    max_hr = hr.get("maxHeartRate", 0) or 0
    hr_values = hr.get("heartRateValues", []) or []
    valid_hr = [v[1] for v in hr_values if isinstance(v, (list, tuple)) and len(v) > 1 and v[1] and v[1] > 0]
    avg_hr = round(sum(valid_hr) / len(valid_hr)) if valid_hr else 0

    # Body Battery
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

    # Stress（堅牢版）
    stress_avg, stress_max = fetch_stress_robust(garmin, date_str)

    # SpO2
    spo2 = safe_get(garmin.get_spo2_data, date_str, default={}) or {}
    spo2_avg = 0
    if isinstance(spo2, dict):
        spo2_avg = spo2.get("averageSpO2", 0) or 0
        if not spo2_avg: 
            spo2_avg = spo2.get("lastSevenDaysAvgSPO2", 0) or 0

    # HRV
    hrv = safe_get(garmin.get_hrv_data, date_str, default={}) or {}
    hrv_value = 0
    if isinstance(hrv, dict):
        hrv_summary = hrv.get("hrvSummary", {}) or {}
        hrv_value = hrv_summary.get("lastNightAvg", 0) or 0
        if not hrv_value: 
            hrv_value = hrv_summary.get("weeklyAvg", 0) or 0

    # Respiration
    resp = safe_get(garmin.get_respiration_data, date_str, default={})
    if not isinstance(resp, dict): 
        resp = {}
    resp_avg = resp.get("avgWakingRespirationValue", 0) or 0

    # Weight & Body Fat（堅牢版）
    weight_kg, body_fat = fetch_weight_robust(garmin, date_str)

    # Floors（堅牢版）
    floors = fetch_floors_robust(stats, garmin, date_str)
    
    print(f"Sleep={total_score}, Wake={wakeup_time}, Bed={bed_time}, Steps={steps}, VO2Max={vo2max}, Stress={stress_avg}, Weight={weight_kg}, Fat={body_fat}, Floors={floors}")
    
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

def write_to_sheet(worksheet, date_str, data, existing_dates):
    if data["steps"] == 0 and data["total_score"] == 0 and data["bb_max"] == 0:
        print(" -> Skip (No Data)")
        return

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

def backfill_missing_data(garmin, worksheet, days_back=30):
    """
    欠損データを一括で埋める（Sleep Time, VO2Max, Stress, Weight, BodyFat, Floors）
    """
    print(f"\n=== 欠損データバックフィル開始（過去{days_back}日分）===\n")
    
    vals = worksheet.col_values(1)
    existing = {v: i+1 for i, v in enumerate(vals) if len(v) >= 10}
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    updated_count = 0
    
    current = start_date
    while current <= end_date:
        date_str = current.isoformat()
        
        if date_str not in existing:
            current += timedelta(days=1)
            continue
            
        row_num = existing[date_str]
        
        try:
            # 現在の値を取得
            current_row = worksheet.row_values(row_num)
            
            # 欠損チェック（列8:wakeup, 列9:bedtime, 列50:stress, 列55:weight, 列56:fat, 列43:floors, 列57:vo2max）
            needs_update = False
            missing_items = []
            
            # Sleep Timeチェック（列8, 9）
            if len(current_row) < 9 or not current_row[7] or not current_row[8]:  # wakeup(8列目)またはbed(9列目)
                needs_update = True
                missing_items.append("sleep")
            
            # 他の項目もチェック
            if len(current_row) < 58 or not current_row[56] or str(current_row[56]) in ["", "0"]:  # VO2Max
                needs_update = True
                missing_items.append("vo2max")
            if len(current_row) < 50 or not current_row[49] or str(current_row[49]) in ["", "0"]:  # Stress
                needs_update = True
                missing_items.append("stress")
            if len(current_row) < 56 or not current_row[54] or str(current_row[54]) in ["", "0"]:  # Weight
                needs_update = True
                missing_items.append("weight")
            if len(current_row) < 57 or not current_row[55] or str(current_row[55]) in ["", "0"]:  # BodyFat
                needs_update = True
                missing_items.append("bodyfat")
            if len(current_row) < 44 or not current_row[42] or str(current_row[42]) in ["", "0"]:  # Floors
                needs_update = True
                missing_items.append("floors")
            
            if not needs_update:
                print(f"[{date_str}] 全データ済み -> スキップ")
                current += timedelta(days=1)
                continue
            
            print(f"[{date_str}] 欠損: {', '.join(missing_items)} -> 再取得...", end=" ")
            
            # データを再取得
            dat = fetch_day_data(garmin, date_str)
            
            # 部分的に更新（欠損している項目のみ）
            updates = []
            
            # Sleep Time更新（空欄の場合のみ）
            if "sleep" in missing_items:
                if dat["wakeup_time"]:
                    updates.append(gspread.Cell(row_num, 8, dat["wakeup_time"]))
                if dat["bed_time"]:
                    updates.append(gspread.Cell(row_num, 9, dat["bed_time"]))
                if dat["total_score"]:
                    updates.append(gspread.Cell(row_num, 10, dat["total_score"]))
                    updates.append(gspread.Cell(row_num, 11, 1 if dat["wakeup_time"] and dat["wakeup_time"] < "06:00" else 0))
                if dat["deep_min"]:
                    updates.append(gspread.Cell(row_num, 35, dat["deep_min"]))
                if dat["light_min"]:
                    updates.append(gspread.Cell(row_num, 36, dat["light_min"]))
                if dat["rem_min"]:
                    updates.append(gspread.Cell(row_num, 37, dat["rem_min"]))
                if dat["awake_min"]:
                    updates.append(gspread.Cell(row_num, 38, dat["awake_min"]))
            
            # その他の項目
            if "vo2max" in missing_items and dat["vo2max"]:
                updates.append(gspread.Cell(row_num, 57, dat["vo2max"]))
            if "stress" in missing_items and dat["stress_avg"]:
                updates.append(gspread.Cell(row_num, 50, dat["stress_avg"]))
            if "weight" in missing_items and dat["weight_kg"]:
                updates.append(gspread.Cell(row_num, 55, dat["weight_kg"]))
            if "bodyfat" in missing_items and dat["body_fat"]:
                updates.append(gspread.Cell(row_num, 56, dat["body_fat"]))
            if "floors" in missing_items and dat["floors"]:
                updates.append(gspread.Cell(row_num, 43, dat["floors"]))
            
            if updates:
                worksheet.update_cells(updates)
                print(f"-> 更新完了")
                updated_count += 1
            else:
                print(f"-> 取得できず")
                
        except Exception as e:
            print(f"[{date_str}] エラー: {e}")
        
        current += timedelta(days=1)
        time.sleep(0.5)
    
    print(f"\n=== 完了: 更新{updated_count}件 ===")

def main():
    mode = os.getenv("RUN_MODE", "daily")
    print(f"--- モード: {mode} ---")
    
    token_str = os.getenv("GARMIN_TOKENS")
    if not token_str:
        print("Error: GARMIN_TOKENS missing")
        sys.exit(1)
    
    json_str = os.getenv("SERVICE_ACCOUNT_JSON")
    if not json_str:
        print("Error: SERVICE_ACCOUNT_JSON missing")
        sys.exit(1)
    
    try:
        if len(token_str) > 1000 and token_str.startswith('{'):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(token_str)
                token_file = f.name
            print("Token saved to temp file")
            
            garmin = Garmin()
            garmin.login(token_file)
            os.unlink(token_file)
        else:
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
        traceback.print_exc()
        sys.exit(1)
    
    try:
        creds_dict = json.loads(json_str)
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        worksheet = gc.open_by_key(SPREADSHEET_ID).get_worksheet(0)
    except Exception as e:
        print(f"Sheets connection failed: {e}")
        sys.exit(1)
    
    if mode == "backfill":
        days = int(os.getenv("BACKFILL_DAYS", "30"))
        backfill_missing_data(garmin, worksheet, days)  # 関数名を変更
    else:
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
                traceback.print_exc()
    
    print("--- Done ---")

if __name__ == "__main__":
    main()

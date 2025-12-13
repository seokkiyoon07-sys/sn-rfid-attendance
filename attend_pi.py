# attend_pi.py
import time, uuid, sqlite3, datetime, threading, subprocess, os, json
from urllib import request as urllib_request
from smartcard.System import readers
from smartcard.util import toHexString
from google.oauth2 import service_account
from googleapiclient.discovery import build

DEVICE_ID = "YP_GATE_01"
SQLITE_PATH = "/home/sn-rfid-attendance/attendance.db"
SPREADSHEET_ID = "1Ve4Skq5UV7fpGFWWx-4ri8I2VGk1s8ZVmLWpV2hMemQ"
SHEET_RANGE = "raw_events!A:I"
MAPPING_RANGE = "staff_mapping!A:E"
SERVICE_ACCOUNT_JSON = "/home/sn-rfid-attendance/service_account.json"
SOUND_CHECKIN = "/home/sn-rfid-attendance/sounds/checkin.wav"
SOUND_CHECKOUT = "/home/sn-rfid-attendance/sounds/checkout.wav"
USE_SPEAKER = True
SYNC_INTERVAL = 30
MAPPING_REFRESH_INTERVAL = 300
MAX_SHIFT_HOURS = 16

staff_cache = {}
sheets_service = None

def play_sound(sound_file):
    if not USE_SPEAKER:
        return
    if not os.path.exists(sound_file):
        return
    try:
        subprocess.Popen(["aplay", "-q", sound_file], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except:
        pass

def get_sheets_service():
    global sheets_service
    if sheets_service is None:
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        sheets_service = build("sheets", "v4", credentials=creds)
    return sheets_service

def load_staff_mapping():
    global staff_cache
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=MAPPING_RANGE).execute()
        rows = result.get("values", [])
        new_cache = {}
        for row in rows[1:]:
            if len(row) >= 3:
                webhook_url = row[3].strip() if len(row) >= 4 else ""
                team_webhook_url = row[4].strip() if len(row) >= 5 else ""
                new_cache[row[0].strip().upper()] = {"staff_id": row[1].strip(), "name": row[2].strip(), "webhook_url": webhook_url, "team_webhook_url": team_webhook_url}
        staff_cache = new_cache
        print(f"[MAPPING] loaded {len(staff_cache)} staff members")
    except Exception as e:
        print(f"[MAPPING] error: {e}")

def get_staff_info(card_uid):
    card_uid = card_uid.upper()
    if card_uid in staff_cache:
        return staff_cache[card_uid]
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=MAPPING_RANGE).execute()
        for row in result.get("values", [])[1:]:
            if len(row) >= 3 and row[0].strip().upper() == card_uid:
                webhook_url = row[3].strip() if len(row) >= 4 else ""
                team_webhook_url = row[4].strip() if len(row) >= 5 else ""
                info = {"staff_id": row[1].strip(), "name": row[2].strip(), "webhook_url": webhook_url, "team_webhook_url": team_webhook_url}
                staff_cache[card_uid] = info
                return info
    except:
        pass
    return None

def send_jandi_notification(webhook_url, name, event_type, ts):
    if not webhook_url:
        return
    try:
        dt = datetime.datetime.fromisoformat(ts)
        time_str = dt.strftime("%H:%M:%S")
        if event_type == "CHECK_IN":
            message = f"{name}님이 출근하였습니다. ({time_str})"
            color = "#00C73C"
        else:
            message = f"{name}님이 퇴근하였습니다. ({time_str})"
            color = "#FF6B6B"
        payload = json.dumps({"body": message, "connectColor": color}).encode("utf-8")
        req = urllib_request.Request(webhook_url, data=payload, headers={"Accept": "application/vnd.tosslab.jandi-v2+json", "Content-Type": "application/json"})
        urllib_request.urlopen(req, timeout=5)
        print(f"[JANDI] sent: {message}")
    except Exception as e:
        print(f"[JANDI] error: {e}")

def format_event_row(eid, dev, ts, uid, sid, etype):
    dt = datetime.datetime.fromisoformat(ts)
    return [eid, dev, dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S"), uid, sid or "", etype, ts, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

def upload_to_sheets(eid, dev, ts, uid, sid, etype):
    try:
        get_sheets_service().spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE, valueInputOption="RAW", body={"values": [format_event_row(eid, dev, ts, uid, sid, etype)]}).execute()
        print(f"[SHEETS] uploaded {eid}")
        return True
    except Exception as e:
        print(f"[SHEETS] error: {e}")
        return False

def upload_batch(events):
    try:
        values = [format_event_row(e["id"], e["device_id"], e["ts_iso"], e["card_uid"], e["staff_id"], e["event_type"]) for e in events]
        get_sheets_service().spreadsheets().values().append(spreadsheetId=SPREADSHEET_ID, range=SHEET_RANGE, valueInputOption="RAW", body={"values": values}).execute()
        return True
    except:
        return False

def init_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, device_id TEXT, ts_iso TEXT, card_uid TEXT, staff_id TEXT, event_type TEXT, synced INTEGER DEFAULT 0)")
    conn.commit()
    conn.close()

def get_last_event(uid):
    """마지막 이벤트 조회. (event_type, ts_iso) 또는 None 반환"""
    conn = sqlite3.connect(SQLITE_PATH)
    row = conn.execute("SELECT event_type, ts_iso FROM events WHERE card_uid=? ORDER BY ts_iso DESC LIMIT 1", (uid,)).fetchone()
    conn.close()
    return row

def should_check_in(uid):
    """출근 여부 판단: 마지막 이벤트가 없거나, CHECK_OUT이거나, 12시간 초과면 출근"""
    last = get_last_event(uid)
    if not last:
        return True
    event_type, ts_iso = last
    if event_type == "CHECK_OUT":
        return True
    last_time = datetime.datetime.fromisoformat(ts_iso)
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    hours_diff = (now - last_time).total_seconds() / 3600
    return hours_diff > MAX_SHIFT_HOURS

def insert_event(uid, sid, etype):
    ts = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).isoformat()
    eid = str(uuid.uuid4())
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("INSERT INTO events VALUES(?,?,?,?,?,?,0)", (eid, DEVICE_ID, ts, uid, sid, etype))
    conn.commit()
    conn.close()
    print(f"[LOCAL] {eid} {uid} {sid} {etype}")
    if upload_to_sheets(eid, DEVICE_ID, ts, uid, sid, etype):
        conn = sqlite3.connect(SQLITE_PATH)
        conn.execute("UPDATE events SET synced=1 WHERE id=?", (eid,))
        conn.commit()
        conn.close()
    return eid, ts

def get_unsynced():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM events WHERE synced=0 LIMIT 50").fetchall()]
    conn.close()
    return rows

def sync_worker():
    while True:
        time.sleep(SYNC_INTERVAL)
        uns = get_unsynced()
        if uns and upload_batch(uns):
            conn = sqlite3.connect(SQLITE_PATH)
            conn.executemany("UPDATE events SET synced=1 WHERE id=?", [(e["id"],) for e in uns])
            conn.commit()
            conn.close()

def mapping_worker():
    while True:
        time.sleep(MAPPING_REFRESH_INTERVAL)
        load_staff_mapping()

def read_loop():
    r = readers()
    if not r:
        raise RuntimeError("No readers")
    conn = r[0].createConnection()
    last_uid, last_time = None, 0
    while True:
        try:
            conn.connect()
            data, sw1, sw2 = conn.transmit([0xFF, 0xCA, 0x00, 0x00, 0x00])
            if (sw1, sw2) == (0x90, 0x00):
                uid = toHexString(data).replace(" ", "")
                now = time.time()
                if uid != last_uid or (now - last_time) > 2.0:
                    info = get_staff_info(uid)
                    sid = info["staff_id"] if info else None
                    name = info["name"] if info else "미등록"
                    webhook_url = info["webhook_url"] if info else ""
                    team_webhook_url = info["team_webhook_url"] if info else ""
                    if should_check_in(uid):
                        print(f"[CHECK_IN] {uid} {name}")
                        play_sound(SOUND_CHECKIN)
                        eid, ts = insert_event(uid, sid, "CHECK_IN")
                        send_jandi_notification(webhook_url, name, "CHECK_IN", ts)
                        send_jandi_notification(team_webhook_url, name, "CHECK_IN", ts)
                    else:
                        print(f"[CHECK_OUT] {uid} {name}")
                        play_sound(SOUND_CHECKOUT)
                        eid, ts = insert_event(uid, sid, "CHECK_OUT")
                        send_jandi_notification(webhook_url, name, "CHECK_OUT", ts)
                        send_jandi_notification(team_webhook_url, name, "CHECK_OUT", ts)
                    last_uid, last_time = uid, now
            time.sleep(0.2)
        except:
            time.sleep(0.2)

if __name__ == "__main__":
    init_db()
    print("RFID Attendance System Started")
    load_staff_mapping()
    threading.Thread(target=sync_worker, daemon=True).start()
    threading.Thread(target=mapping_worker, daemon=True).start()
    read_loop()

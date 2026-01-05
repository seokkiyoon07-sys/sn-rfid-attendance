# attend_pi.py - API 연동 버전
import time, sqlite3, datetime, threading, subprocess, os, json, uuid
from urllib import request as urllib_request
from urllib.error import URLError, HTTPError
from smartcard.System import readers
from smartcard.util import toHexString

# ============================================
# 설정
# ============================================
DEVICE_ID = "YP_GATE_01"
SQLITE_PATH = "/home/sn-rfid-attendance/attendance.db"

# API 설정 (Next.js 서버)
API_BASE_URL = "https://your-domain.vercel.app"  # 배포 URL로 변경 필요
API_KEY = "sn-rfid-api-key-2024-secure"  # .env.local의 RFID_API_KEY와 동일하게

# 사운드 설정
SOUND_CHECKIN = "/home/sn-rfid-attendance/sounds/checkin.wav"
SOUND_CHECKOUT = "/home/sn-rfid-attendance/sounds/checkout.wav"
USE_SPEAKER = True

# 동기화 설정
SYNC_INTERVAL = 30  # 오프라인 이벤트 동기화 주기 (초)
MAX_SHIFT_HOURS = 16  # 출근 후 이 시간이 지나면 자동으로 다시 출근 처리

# ============================================
# 사운드 재생
# ============================================
def play_sound(sound_file):
    if not USE_SPEAKER:
        return
    if not os.path.exists(sound_file):
        return
    try:
        subprocess.Popen(["aplay", "-q", sound_file], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except:
        pass

# ============================================
# API 호출 함수
# ============================================
def api_request(method, endpoint, data=None):
    """API 요청을 보내고 응답을 반환"""
    url = f"{API_BASE_URL}{endpoint}"
    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        if method == "GET":
            req = urllib_request.Request(url, headers=headers)
        else:  # POST
            payload = json.dumps(data).encode("utf-8") if data else None
            req = urllib_request.Request(url, data=payload, headers=headers, method=method)

        with urllib_request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        print(f"[API] HTTP Error {e.code}: {e.reason}")
        return None
    except URLError as e:
        print(f"[API] URL Error: {e.reason}")
        return None
    except Exception as e:
        print(f"[API] Error: {e}")
        return None

def get_staff_and_last_event(card_uid):
    """카드 UID로 직원 정보와 마지막 이벤트 조회"""
    result = api_request("GET", f"/api/rfid?cardUid={card_uid}")
    if result:
        return result.get("staff"), result.get("lastEvent")
    return None, None

def send_event_to_api(device_id, card_uid, event_type):
    """출퇴근 이벤트를 API로 전송"""
    data = {
        "deviceId": device_id,
        "cardUid": card_uid,
        "eventType": event_type
    }
    result = api_request("POST", "/api/rfid", data)
    if result and result.get("success"):
        print(f"[API] Event sent successfully: {result.get('staffName')} - {event_type}")
        return True, result
    return False, None

# ============================================
# 잔디 웹훅 알림
# ============================================
def send_jandi_notification(webhook_url, name, event_type, ts):
    if not webhook_url:
        return
    try:
        dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        time_str = dt.strftime("%H:%M:%S")
        if event_type == "CHECK_IN":
            message = f"{name}님이 출근하였습니다. ({time_str})"
            color = "#00C73C"
        else:
            message = f"{name}님이 퇴근하였습니다. ({time_str})"
            color = "#FF6B6B"
        payload = json.dumps({"body": message, "connectColor": color}).encode("utf-8")
        req = urllib_request.Request(webhook_url, data=payload, headers={
            "Accept": "application/vnd.tosslab.jandi-v2+json",
            "Content-Type": "application/json"
        })
        urllib_request.urlopen(req, timeout=5)
        print(f"[JANDI] sent: {message}")
    except Exception as e:
        print(f"[JANDI] error: {e}")

# ============================================
# 로컬 SQLite (오프라인 백업용)
# ============================================
def init_db():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT,
            ts_iso TEXT,
            card_uid TEXT,
            event_type TEXT,
            synced INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def save_local_event(device_id, card_uid, event_type, ts_iso, synced=False):
    """로컬 DB에 이벤트 저장 (오프라인 백업)"""
    conn = sqlite3.connect(SQLITE_PATH)
    event_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO events (id, device_id, ts_iso, card_uid, event_type, synced) VALUES (?, ?, ?, ?, ?, ?)",
        (event_id, device_id, ts_iso, card_uid, event_type, 1 if synced else 0)
    )
    conn.commit()
    conn.close()

def get_last_local_event(card_uid):
    """로컬 DB에서 마지막 이벤트 조회"""
    conn = sqlite3.connect(SQLITE_PATH)
    row = conn.execute(
        "SELECT event_type, ts_iso FROM events WHERE card_uid=? ORDER BY ts_iso DESC LIMIT 1",
        (card_uid,)
    ).fetchone()
    conn.close()
    return row

def get_unsynced_events():
    """동기화되지 않은 이벤트 조회"""
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM events WHERE synced=0 ORDER BY ts_iso ASC LIMIT 50"
    ).fetchall()]
    conn.close()
    return rows

def mark_event_synced(event_id):
    """이벤트를 동기화 완료로 표시"""
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("UPDATE events SET synced=1 WHERE id=?", (event_id,))
    conn.commit()
    conn.close()

# ============================================
# 출퇴근 판단 로직
# ============================================
def should_check_in(card_uid, last_event_from_api):
    """출근 여부 판단"""
    # API에서 마지막 이벤트가 있으면 사용
    if last_event_from_api:
        event_type = last_event_from_api.get("eventType")
        ts_iso = last_event_from_api.get("tsIso")

        if event_type == "CHECK_OUT":
            return True

        # 마지막 출근 후 MAX_SHIFT_HOURS 시간이 지났으면 다시 출근
        try:
            last_time = datetime.datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
            now = datetime.datetime.now(datetime.timezone.utc)
            hours_diff = (now - last_time).total_seconds() / 3600
            if hours_diff > MAX_SHIFT_HOURS:
                return True
        except:
            pass

        return False

    # API 응답이 없으면 로컬 DB 확인
    local_last = get_last_local_event(card_uid)
    if not local_last:
        return True

    event_type, ts_iso = local_last
    if event_type == "CHECK_OUT":
        return True

    try:
        last_time = datetime.datetime.fromisoformat(ts_iso)
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        hours_diff = (now - last_time).total_seconds() / 3600
        return hours_diff > MAX_SHIFT_HOURS
    except:
        return True

# ============================================
# 동기화 워커 (오프라인 이벤트 동기화)
# ============================================
def sync_worker():
    """오프라인 이벤트를 서버에 동기화"""
    while True:
        time.sleep(SYNC_INTERVAL)
        unsynced = get_unsynced_events()
        for event in unsynced:
            success, _ = send_event_to_api(
                event["device_id"],
                event["card_uid"],
                event["event_type"]
            )
            if success:
                mark_event_synced(event["id"])
                print(f"[SYNC] Event {event['id']} synced")

# ============================================
# 메인 RFID 읽기 루프
# ============================================
def read_loop():
    r = readers()
    if not r:
        raise RuntimeError("No RFID readers found")

    print(f"[RFID] Using reader: {r[0]}")
    conn = r[0].createConnection()
    last_uid, last_time = None, 0

    while True:
        try:
            conn.connect()
            # 카드 UID 읽기 (ISO 14443-3A)
            data, sw1, sw2 = conn.transmit([0xFF, 0xCA, 0x00, 0x00, 0x00])

            if (sw1, sw2) == (0x90, 0x00):
                uid = toHexString(data).replace(" ", "").upper()
                now = time.time()

                # 중복 방지 (같은 카드 2초 이내 재태그 무시)
                if uid != last_uid or (now - last_time) > 2.0:
                    print(f"\n[CARD] UID: {uid}")

                    # API에서 직원 정보와 마지막 이벤트 조회
                    staff, last_event = get_staff_and_last_event(uid)
                    name = staff.get("name") if staff else "미등록"
                    webhook_url = staff.get("webhookUrl") if staff else ""
                    team_webhook_url = staff.get("teamWebhookUrl") if staff else ""

                    # 출퇴근 판단
                    if should_check_in(uid, last_event):
                        event_type = "CHECK_IN"
                        print(f"[CHECK_IN] {name}")
                        play_sound(SOUND_CHECKIN)
                    else:
                        event_type = "CHECK_OUT"
                        print(f"[CHECK_OUT] {name}")
                        play_sound(SOUND_CHECKOUT)

                    # API로 이벤트 전송
                    ts_iso = datetime.datetime.now(
                        datetime.timezone(datetime.timedelta(hours=9))
                    ).isoformat()

                    success, result = send_event_to_api(DEVICE_ID, uid, event_type)

                    # 로컬 DB에도 저장 (백업)
                    save_local_event(DEVICE_ID, uid, event_type, ts_iso, synced=success)

                    # 잔디 알림 (API 성공 시 서버에서 처리하지만, 실패 시 로컬에서 전송)
                    if not success:
                        send_jandi_notification(webhook_url, name, event_type, ts_iso)
                        send_jandi_notification(team_webhook_url, name, event_type, ts_iso)

                    last_uid, last_time = uid, now

            time.sleep(0.2)
        except Exception as e:
            # 카드 읽기 실패 (카드 없음 등)는 무시
            time.sleep(0.2)

# ============================================
# 메인
# ============================================
if __name__ == "__main__":
    print("=" * 50)
    print("RFID Attendance System - API Version")
    print(f"Device ID: {DEVICE_ID}")
    print(f"API URL: {API_BASE_URL}")
    print("=" * 50)

    # 로컬 DB 초기화
    init_db()

    # 동기화 워커 시작 (백그라운드)
    threading.Thread(target=sync_worker, daemon=True).start()

    # RFID 읽기 시작
    read_loop()

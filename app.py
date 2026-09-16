import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
import traceback
import secrets
import hashlib
import json
import math
import requests
from flask import Flask, request
from supabase import create_client

app = Flask(__name__)

# Environment Configuration
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = [int(i.strip()) for i in os.environ.get("ADMIN_IDS", "").split(",") if i.strip()]
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        pass

ADMIN_SESSIONS = {}
ADMIN_MENU_MESSAGES = {}  
_ADMIN_MENU_LOCKS = {}
_ADMIN_MENU_LOCKS_GUARD = threading.Lock()
KEY_FLOW_MESSAGES = {}    
PENDING_JOIN_REQUESTS = {}  
VERIFICATION_MESSAGES = {}  
VERIFICATION_LOCKS = {}

_UPDATE_EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="update")
_PUBLIC_EXECUTOR = ThreadPoolExecutor(max_workers=64, thread_name_prefix="public")
_VERIFY_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="verify")
_COMMAND_EXECUTOR = ThreadPoolExecutor(max_workers=64, thread_name_prefix="command")
_ACK_EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="ack")
_PROFILE_EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="profile")

from requests.adapters import HTTPAdapter
_TG_SESSION = requests.Session()
_TG_SESSION.mount("https://", HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0))
_SETTINGS_CACHE = {}
_SETTINGS_CACHE_READY = False
_SETTINGS_CACHE_LOCK = threading.Lock()
_SETTINGS_LOAD_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="settings")

_CLEANUP_EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="cleanup")
_SETTINGS_LOAD_INFLIGHT = False
_SETTINGS_LOAD_GUARD = threading.Lock()
_CHANNELS_CACHE = None
_CHANNELS_CACHE_READY = False
_CHANNELS_CACHE_LOCK = threading.Lock()
USER_PROFILE_CACHE = {}  

# System Defaults
DEFAULT_WELCOME = ">> LOOKING FOR HACKS, ID'S OR ANYTHING ELSE?\nMESSAGE [@lexxiowner](https://t.me/lexxiowner) — WE MIGHT HAVE IT. ⚡"
DEFAULT_ERROR = "⚠️ [INVALID_FILE]\nFile not found or expired.\nContact @lexxiowner"
DEFAULT_FJ = ">> HEY {name} ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T JOINED TO\nOUR CHANNELS YET,"
DEFAULT_STORAGE = "-1004430375220"
DEFAULT_WARN = "⚠️ **IMPORTANT NOTICE:**\n\n_All files will be deleted in {time}.\nPlease forward/save this to your Saved Messages!_"
DEFAULT_UP_BTN = "📟 Update Channel"
DEFAULT_UP_URL = "https://t.me/lexxiowner"
DEFAULT_ANIM_TEXT = "Verifying"
DEFAULT_VERIFY_RETRY = "🔄 **Complete the remaining channels and tap Verify again.**"
DEFAULT_NOTICE_BTN_TEXT = "JOIN NOW"
DEFAULT_NOTICE_BTN_URL = DEFAULT_UP_URL

MONO_TRANS = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    "𝙰𝙱𝙲𝙳𝙴𝙵𝙶𝙷𝙸𝙹𝙺𝙻𝙼𝙽𝙾𝙿𝚀𝚁𝚂𝚃𝚄𝚅𝚆𝚇𝚈𝚉𝚊𝚋𝚌𝚍𝚎𝚏𝚐𝚑𝚒𝚓𝚔𝚕𝚖𝚗𝚘𝚙𝚚𝚛𝚜𝚝𝚞𝚟𝚠𝚡𝚢𝚣"
)

def mono(text):
    return str(text).translate(MONO_TRANS)

def mono_admin_kb(keyboard):
    out = []
    for row in keyboard or []:
        new_row = []
        for btn in row or []:
            b = dict(btn)
            if "text" in b:
                b["text"] = mono(b["text"])
            if "callback_data" in b and "style" not in b:
                cb=str(b.get("callback_data","")); role="back" if ("Back" in b.get("text","") or "BACK" in b.get("text","")) else "admin"
                if any(x in cb for x in ("delete","del","remove","reject")): role="back"
                b["style"]=_ui_button_style(role,"danger" if role=="back" else "primary")
            new_row.append(b)
        out.append(new_row)
    return out

def get_all_apps():
    if not supabase:
        return []
    rows=[]
    start=0
    page_size=1000
    while True:
        try:
            page = (supabase.table("apps").select("*").order("created_at", desc=True)
                    .range(start, start + page_size - 1).execute().data or [])
        except Exception:
            break
        rows.extend(page)
        if len(page) < page_size:
            break
        start += page_size
    return rows

def get_app_page(page=0, page_size=20):
    apps=get_all_apps()
    total=len(apps)
    start=max(0,int(page))*page_size
    return apps[start:start+page_size], total

def cb_suffix(cdata, prefix):
    return cdata[len(prefix):] if cdata.startswith(prefix) else ""

def clean_markdown(name):
    return re.sub(r'([_*\[\]()~`>#+=|{}.!-])', r'\\\1', str(name or "User"))

def tg_call(method, data):
    try:
        payload = dict(data or {})
        admin_chat = False
        try:
            admin_chat = int(payload.get("chat_id")) in {int(x) for x in ADMIN_IDS}
        except Exception:
            admin_chat = False
        if admin_chat:
            payload = _admin_add_back_button(payload, int(payload.get("chat_id")))
            if method == "sendMessage" and int(payload.get("chat_id")) in ADMIN_SESSIONS:
                markup = payload.get("reply_markup")
                if not (isinstance(markup, dict) and isinstance(markup.get("inline_keyboard"), list)):
                    payload["reply_markup"] = _admin_input_markup(int(payload.get("chat_id")))
        resp = _TG_SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
            json=payload, timeout=(1.5, 4)
        )
        try:
            r = resp.json()
        except Exception:
            print(f"TELEGRAM API NONJSON method={method} http={resp.status_code} body={resp.text[:300]!r}", flush=True)
            return {}
        if not r.get("ok"):
            code = r.get("error_code")
            desc = str(r.get("description") or "")
            harmless = (
                method == "deleteMessage" and code == 400 and "message to delete not found" in desc.lower()
            ) or (
                method in {"editMessageText", "editMessageCaption", "editMessageReplyMarkup"}
                and code == 400 and "message is not modified" in desc.lower()
            )
            if harmless:
                return r
            print(f"TELEGRAM API FAIL method={method} http={resp.status_code} code={code} desc={desc}", flush=True)
            if "parse_mode" in payload:
                retry_data = dict(payload); retry_data.pop("parse_mode", None)
                try:
                    rr_resp = _TG_SESSION.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                        json=retry_data, timeout=(1.5, 4)
                    )
                    rr = rr_resp.json()
                    if not rr.get("ok"):
                        print(f"TELEGRAM API RETRY FAIL method={method} http={rr_resp.status_code} code={rr.get('error_code')} desc={rr.get('description')}", flush=True)
                    return rr
                except Exception as retry_exc:
                    print(f"TELEGRAM API RETRY ERROR method={method}: {retry_exc}", flush=True)
        return r
    except Exception as exc:
        print(f"TELEGRAM API ERROR method={method}: {exc}", flush=True)
        return {}

def _load_settings_cache():
    global _SETTINGS_CACHE_READY, _SETTINGS_LOAD_INFLIGHT
    if _SETTINGS_CACHE_READY or not supabase:
        return
    with _SETTINGS_LOAD_GUARD:
        if _SETTINGS_CACHE_READY or _SETTINGS_LOAD_INFLIGHT or not supabase:
            return
        _SETTINGS_LOAD_INFLIGHT = True
    try:
        _SETTINGS_LOAD_EXECUTOR.submit(_load_settings_cache_job)
    except Exception as exc:
        with _SETTINGS_LOAD_GUARD:
            _SETTINGS_LOAD_INFLIGHT = False
        print(f"SETTINGS LOAD QUEUE ERROR: {exc}", flush=True)

def _load_settings_cache_job():
    global _SETTINGS_CACHE_READY, _SETTINGS_LOAD_INFLIGHT
    try:
        rows = supabase.table("settings").select("key,value").execute().data or []
        with _SETTINGS_CACHE_LOCK:
            _SETTINGS_CACHE.clear()
            _SETTINGS_CACHE.update({str(r.get("key")): r.get("value", "") for r in rows if r.get("key") is not None})
            _SETTINGS_CACHE_READY = True
        print(f"SETTINGS CACHE READY entries={len(_SETTINGS_CACHE)}", flush=True)
    except Exception as exc:
        print(f"SETTINGS CACHE LOAD ERROR: {exc}", flush=True)
    finally:
        with _SETTINGS_LOAD_GUARD:
            _SETTINGS_LOAD_INFLIGHT = False

def get_setting(key, default=""):
    if not _SETTINGS_CACHE_READY:
        _load_settings_cache()
    return _SETTINGS_CACHE.get(key, default)

def set_setting(key, value):
    if not supabase:
        return
    try:
        supabase.table("settings").upsert({"key": key, "value": str(value)}).execute()
        _SETTINGS_CACHE[str(key)] = str(value)
    except Exception:
        pass

def invalidate_channels_cache():
    global _CHANNELS_CACHE, _CHANNELS_CACHE_READY
    with _CHANNELS_CACHE_LOCK:
        _CHANNELS_CACHE = None
        _CHANNELS_CACHE_READY = False

def get_channels():
    global _CHANNELS_CACHE, _CHANNELS_CACHE_READY
    if _CHANNELS_CACHE_READY:
        return list(_CHANNELS_CACHE or [])
    if not supabase:
        return []
    with _CHANNELS_CACHE_LOCK:
        if _CHANNELS_CACHE_READY:
            return list(_CHANNELS_CACHE or [])
        try:
            r = supabase.table("force_join_channels").select("*").order("slot").execute()
            _CHANNELS_CACHE = r.data or []
            _CHANNELS_CACHE_READY = True
        except Exception as exc:
            print(f"CHANNELS CACHE LOAD ERROR: {exc}", flush=True)
            _CHANNELS_CACHE = None
            _CHANNELS_CACHE_READY = False
            return None
        return list(_CHANNELS_CACHE or [])

def get_unjoined(user_id):
    channels = get_channels()
    uid = int(user_id)

    if channels is None:
        raise RuntimeError("Unable to load Force Join configuration")

    active_channels = [c for c in channels if c.get("active") and c.get("chat_id") is not None]
    if not active_channels:
        return []

    pending_chat_ids = {str(chat) for (pending_uid, chat), ts in PENDING_JOIN_REQUESTS.items()
                        if pending_uid == uid}
    method = get_setting("verification_method", "both").lower()
    if supabase and method != "joined":
        try:
            rows = (supabase.table("join_requests").select("chat_id")
                    .eq("user_id", uid).execute().data or [])
            for row in rows:
                raw_id=row.get("chat_id")
                if raw_id is not None:
                    pending_chat_ids.add(str(raw_id))
                    try: pending_chat_ids.add(str(int(raw_id)))
                    except (TypeError, ValueError): pass
        except Exception as exc:
            print(f"PENDING REQUEST READ ERROR user={uid}: {exc}", flush=True)

    def check_channel(c):
        chat_id = c.get("chat_id")
        normalized_chat_id = str(chat_id)

        if method != "joined" and normalized_chat_id in pending_chat_ids:
            print(f"VERIFY REQUEST MATCH user={uid} chat={normalized_chat_id}")
            return None
        if method == "request" and normalized_chat_id not in pending_chat_ids:
            return c

        if method == "request":
            return c
        try:
            res = _TG_SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
                json={"chat_id": chat_id, "user_id": uid}, timeout=(2.0, 4)
            ).json()
        except Exception as exc:
            print(f"MEMBERSHIP CHECK ERROR user={uid} chat={normalized_chat_id}: {exc}")
            res = {}

        status = res.get("result", {}).get("status") if res.get("ok") else None
        if res.get("ok") and status not in ["left", "kicked"]:
            return None
        return c

    workers = min(8, max(1, len(active_channels)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(check_channel, active_channels))
    return [c for c in results if c is not None]

def _fj_keyboard(unjoined, code):
    layout = max(1, min(int(get_setting("button_layout", "2")), 4))
    kb, row = [], []
    for c in unjoined:
        btn_txt = c.get("button_text", "Channel").replace("↗", "").replace("↗️", "").strip()
        row.append({"text": btn_txt, "url": c.get("join_url")})
        if len(row) == layout:
            kb.append(row); row = []
    if row: kb.append(row)
    kb.append([{"text": "» VERIFY", "callback_data": f"v:{code}"}])
    return kb

def _track_verification(chat_id, user_id, code, **kwargs):
    key=(chat_id, user_id, code)
    item=VERIFICATION_MESSAGES.setdefault(key, {})
    item.update(kwargs)
    return key

def _delete_verification_messages(chat_id, user_id, code, include_base=True):
    key=(chat_id,user_id,code)
    item=VERIFICATION_MESSAGES.pop(key, {})
    ids=[]
    if include_base: ids.append(item.get("base"))
    ids += [item.get("failed"), item.get("retry"), item.get("anim")]
    for mid in dict.fromkeys(x for x in ids if x):
        _async_delete_message(chat_id, mid)

def cleanup_verification_state(chat_id,user_id,code,include_base=True):
    key=(chat_id,user_id,code)
    item=VERIFICATION_MESSAGES.pop(key,{})
    mapping=[("cleanup_force",item.get("base") if include_base else None),("cleanup_retry",item.get("retry")),("cleanup_anim",item.get("anim"))]
    for setting,mid in mapping:
        if mid and get_setting(setting,"true").lower()=="true":
            tg_call("deleteMessage",{"chat_id":chat_id,"message_id":mid})

def send_fj(chat_id, user_id, code, name):
    unjoined = get_unjoined(user_id)
    if not unjoined:
        return True
    raw = get_setting("force_join_text", DEFAULT_FJ)
    caption = raw.replace("{name}", clean_markdown(name)) if "{name}" in raw else raw
    kb = _fj_keyboard(unjoined, code)
    poster_storage_id = get_setting("poster_storage_id", "")
    storage_id = get_setting("storage_channel_id", DEFAULT_STORAGE)
    if poster_storage_id:
        res=tg_call("copyMessage", {"chat_id": chat_id, "from_chat_id": storage_id, "message_id": int(poster_storage_id), "caption": caption, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        res=tg_call("sendMessage", {"chat_id": chat_id, "text": caption, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    if res.get("ok"):
        _track_verification(chat_id,user_id,code,base=res["result"]["message_id"])
    return False

def auto_del_task(chat_id, mids, delay):
    time.sleep(delay)
    for m in mids:
        if m:
            tg_call("deleteMessage", {"chat_id": chat_id, "message_id": m})

def deliver_file(chat_id, code):
    app_data = None
    if supabase:
        try:
            r = supabase.table("apps").select("*").eq("deep_link_code", code).eq("active", True).execute()
            if r.data:
                app_data = r.data[0]
        except Exception:
            pass

    if not app_data:
        tg_call("sendMessage", {"chat_id": chat_id, "text": get_setting("error_feedback_text", DEFAULT_ERROR), "parse_mode": "Markdown"})
        return

    storage = get_setting("storage_channel_id", DEFAULT_STORAGE)
    sender_mode = app_data.get("sender_mode", "copy")

    if sender_mode == "forward":
        delivery = tg_call("forwardMessage", {"chat_id": chat_id, "from_chat_id": storage, "message_id": app_data["storage_message_id"]})
    else:
        delivery = tg_call("copyMessage", {"chat_id": chat_id, "from_chat_id": storage, "message_id": app_data["storage_message_id"]})

    if not delivery.get("ok"):
        tg_call("sendMessage", {"chat_id": chat_id, "text": get_setting("error_feedback_text", DEFAULT_ERROR), "parse_mode": "Markdown"})
        return

    mids = [delivery.get("result", {}).get("message_id")]

    del_seconds = int(get_setting("auto_delete_time", "120"))
    cleanup_file = get_setting("cleanup_file", "true").lower() == "true"
    cleanup_notice = get_setting("cleanup_notice", "true").lower() == "true"
    if del_seconds > 0 and (cleanup_file or cleanup_notice):
        mins = del_seconds // 60
        secs = del_seconds % 60
        time_str = f"{mins} minute{'s' if mins > 1 else ''}" if mins else f"{secs} seconds"

        notice_enabled = get_setting("notice_enabled", "true").lower() == "true"
        if not notice_enabled:
            warn_msg = ""
        else:
            raw_warn = get_setting("auto_delete_warn_text", DEFAULT_WARN)
            warn_msg = raw_warn.replace("{time}", time_str)

        notice_btn_enabled = notice_enabled and get_setting("notice_btn_enabled", "true").lower() == "true"
        btn_txt = get_setting("notice_btn_text", DEFAULT_NOTICE_BTN_TEXT)
        btn_url = get_setting("notice_btn_url", get_setting("update_btn_url", DEFAULT_NOTICE_BTN_URL))
        kb = [[{"text": btn_txt, "url": btn_url}]] if (notice_btn_enabled and btn_txt and btn_url and btn_txt.lower() != "none") else []
        w = {}
        if warn_msg and cleanup_notice:
            w = tg_call("sendMessage", {
                "chat_id": chat_id,
                "text": warn_msg,
                "parse_mode": "Markdown",
                "reply_markup": {"inline_keyboard": kb} if kb else None
            })
            if w.get("ok"):
                mids.append(w["result"]["message_id"])

        delete_mids = []
        if cleanup_file:
            delete_mids.append(mids[0])
        if cleanup_notice and len(mids) > 1:
            delete_mids.extend(mids[1:])
        if delete_mids:
            threading.Thread(target=auto_del_task, args=(chat_id, delete_mids, del_seconds), daemon=True).start()

def run_verify_process(chat_id, user_id, code, fj_mid):
    key = (chat_id, user_id, code)
    lock = VERIFICATION_LOCKS.setdefault(key, threading.Lock())
    if not lock.acquire(blocking=False):
        return
    try:
        if get_setting("verification_enabled", "true").lower() != "true":
            cleanup_verification_state(chat_id, user_id, code, include_base=True)
            deliver_file(chat_id, code)
            return

        anim_enabled = get_setting("anim_enabled", "true").lower() == "true"
        try:
            anim_speed = max(0.10, min(float(get_setting("anim_speed", "0.50")), 5.0))
        except Exception:
            anim_speed = 0.50
        base = get_setting("anim_text", DEFAULT_ANIM_TEXT).strip() or "Verifying"
        frames = [f"{base}.", f"{base}..", f"{base}..."]
        stop = threading.Event()
        anim_state = {"mid": None}

        def animate():
            if stop.is_set():
                return
            r = tg_call("sendMessage", {"chat_id": chat_id, "text": frames[0]})
            if not r.get("ok") or stop.is_set():
                mid = r.get("result", {}).get("message_id") if r.get("ok") else None
                if mid:
                    _async_delete_message(chat_id, mid)
                return
            anim_state["mid"] = r["result"]["message_id"]
            _track_verification(chat_id, user_id, code, anim=anim_state["mid"])
            idx = 1
            while not stop.wait(anim_speed):
                r = tg_call("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": anim_state["mid"],
                    "text": frames[idx]
                })
                if not r.get("ok"):
                    break
                idx = (idx + 1) % 3

        anim_thread = None
        if anim_enabled:
            anim_thread = threading.Thread(target=animate, name="verify-animation", daemon=True)
            anim_thread.start()

        unjoined = get_unjoined(user_id)
        stop.set()
        if anim_thread:
            anim_thread.join(timeout=0.35)

        anim_mid = anim_state.get("mid") or VERIFICATION_MESSAGES.get(key, {}).get("anim")
        if anim_mid:
            if get_setting("cleanup_anim", "true").lower() == "true":
                tg_call("deleteMessage", {"chat_id": chat_id, "message_id": anim_mid})
            VERIFICATION_MESSAGES.get(key, {}).pop("anim", None)

        if not unjoined:
            cleanup_verification_state(chat_id, user_id, code, include_base=True)
            if code == "__setup__":
                _setup_send(chat_id)
            else:
                deliver_file(chat_id, code)
            return

        state = VERIFICATION_MESSAGES.pop((chat_id, user_id, code), {})
        old_base = state.get("base")
        old_failed = state.get("failed")
        old_retry = state.get("retry")
        old_anim = state.get("anim")
        for old_mid in (old_base, old_failed, old_retry, old_anim):
            if old_mid:
                tg_call("deleteMessage", {"chat_id": chat_id, "message_id": old_mid})

        r = tg_call("sendMessage", {
            "chat_id": chat_id,
            "text": get_setting("verify_retry_text", DEFAULT_VERIFY_RETRY),
            "parse_mode": "Markdown"
        })
        if r.get("ok"):
            _track_verification(chat_id, user_id, code, retry=r["result"]["message_id"])

        send_fj(chat_id, user_id, code, "User")
    finally:
        lock.release()
        VERIFICATION_LOCKS.pop(key, None)

def _admin_menu_lock(chat_id):
    with _ADMIN_MENU_LOCKS_GUARD:
        return _ADMIN_MENU_LOCKS.setdefault(int(chat_id), threading.RLock())

def _delete_admin_menu_ids(chat_id, ids):
    for mid in list(ids or []):
        try:
            tg_call("deleteMessage", {"chat_id": chat_id, "message_id": int(mid)})
        except Exception:
            pass

def _async_delete_message(chat_id, message_id):
    try:
        return _CLEANUP_EXECUTOR.submit(_safe_run, tg_call, "deleteMessage", {"chat_id": chat_id, "message_id": int(message_id)})
    except Exception:
        return None

def _admin_clear_menu(chat_id):
    lock = _admin_menu_lock(chat_id)
    with lock:
        old = ADMIN_MENU_MESSAGES.pop(chat_id, None)
        mids = old if isinstance(old, (set, list, tuple)) else ({old} if old else set())
    for mid in mids:
        if not mid:
            continue
        try:
            _CLEANUP_EXECUTOR.submit(_safe_run, tg_call, "deleteMessage", {"chat_id": chat_id, "message_id": int(mid)})
        except Exception:
            pass

def _admin_session_back_callback(session):
    st=str((session or {}).get("step") or "")
    if st.startswith("UI_BUTTON"):
        return "ui_buttons"
    if st.startswith("K_"):
        pid=(session or {}).get("pid")
        if pid and st not in {"K_ADD_PRODUCT", "K_ADD_KEYS_GLOBAL"}:
            return f"kap:{pid}"
        return "m_keys"
    if st.startswith("SETUP_"):
        return "m_setup"
    if st.startswith("FJ_"):
        return "m_fj"
    if st.startswith("F_"):
        return "m_files"
    if st.startswith("SET_"):
        return "m_sett"
    if st.startswith("WAIT_BC"):
        return "adm_home"
    return "adm_home"

def _admin_add_back_button(data, user_id=None):
    if not isinstance(data, dict):
        return data
    markup=data.get("reply_markup")
    if not isinstance(markup, dict) or not isinstance(markup.get("inline_keyboard"), list):
        return data
    kb=markup.get("inline_keyboard") or []
    has_back=False
    for row in kb:
        for b in row or []:
            b=b or {}
            text=str(b.get("text") or "").lower()
            cb=str(b.get("callback_data") or "")
            if "back" in text or "cancel" in text or cb in {"adm_home","adm_back_session","m_ui","ui_buttons"}:
                has_back=True
                break
        if has_back:
            break
    if not has_back:
        session=ADMIN_SESSIONS.get(int(user_id)) if user_id is not None else None
        cb=_admin_session_back_callback(session) if session else "adm_home"
        kb=[list(row or []) for row in kb if row]
        kb.append([{"text":"⬅️ Back","callback_data":cb}])
        markup=dict(markup)
        markup["inline_keyboard"]=kb
        data=dict(data)
        data["reply_markup"]=markup
    try:
        data=dict(data)
        markup=dict(data.get("reply_markup") or {})
        markup["inline_keyboard"]=_style_inline_keyboard(markup.get("inline_keyboard") or [],"admin")
        data["reply_markup"]=markup
    except Exception as exc:
        print(f"ADMIN MENU STYLE ERROR: {exc}", flush=True)
    return data

def _admin_input_markup(user_id):
    sess=ADMIN_SESSIONS.get(int(user_id)) or {}
    return {"inline_keyboard":[[{"text":"⬅️ Back / Cancel","callback_data":_admin_session_back_callback(sess)}]]}

def _admin_begin_input(chat_id, user_id, session, prompt):
    _admin_clear_menu(chat_id)
    ADMIN_SESSIONS[int(user_id)] = dict(session)
    return tg_call("sendMessage", {"chat_id": chat_id, "text": prompt, "reply_markup": _admin_input_markup(user_id)})

def _admin_store_menu(chat_id, result):
    if isinstance(result, dict) and result.get("ok"):
        mid = result.get("result", {}).get("message_id")
        if mid:
            lock = _admin_menu_lock(chat_id)
            with lock:
                old = ADMIN_MENU_MESSAGES.get(chat_id, set())
                old_mids = old if isinstance(old, (set, list, tuple)) else {old}
                old_mids = {int(x) for x in old_mids if x and str(x) != str(mid)}
                ADMIN_MENU_MESSAGES[chat_id] = {int(mid)}
            for old_mid in old_mids:
                try:
                    _async_delete_message(chat_id, old_mid)
                except Exception:
                    pass
    return result

def _key_send(chat_id, data, track=True):
    r = tg_call("sendMessage", data)
    if track and r.get("ok"):
        KEY_FLOW_MESSAGES.setdefault(chat_id, set()).add(r["result"]["message_id"])
    return r

_UI_BUTTON_REGISTRY = {}
_UI_BUTTON_REGISTRY_LOCK = threading.Lock()

def _button_token(btn):
    raw = "|".join([
        str(btn.get("text") or ""),
        str(btn.get("callback_data") or ""),
        str(btn.get("url") or ""),
    ]) or "button"
    return hashlib.sha1(raw.encode("utf-8", "ignore")).hexdigest()[:12]

def _button_setting(token, field, default=""):
    return get_setting(f"ui_button_{token}_{field}", default)

def _register_ui_button(btn, role, text):
    token = _button_token(btn)
    with _UI_BUTTON_REGISTRY_LOCK:
        _UI_BUTTON_REGISTRY[token] = {
            "token": token, "text": str(text), "role": str(role),
            "callback_data": str(btn.get("callback_data") or ""),
            "url": str(btn.get("url") or ""),
        }
    return token

def _ui_button_style(role="default", fallback="primary", token=None):
    allowed={"primary","success","danger"}
    if token:
        override=str(get_setting(f"ui_button_{token}_style", "") or "").lower()
        if override in allowed:
            return override
    value=str(get_setting(f"ui_btn_{role}",fallback) or fallback).lower()
    return value if value in allowed else fallback

def _style_inline_keyboard(kb, role_default="default"):
    out=[]
    for row in kb or []:
        nr=[]
        for btn in row or []:
            b=dict(btn); cb=str(b.get("callback_data","")); text=str(b.get("text",""))
            role=role_default
            if "Back" in text or "BACK" in text or cb in {"adm_home","m_ui","hide_admin"}: role="back"
            elif any(x in cb for x in ("delete","del","remove","reject")): role="back"
            elif any(x in cb for x in ("k_buy","k_my","k_prod:","k_dur:","klang:","k_home:")): role="buy"
            elif cb.startswith(("m_","set_","ka_","kap","kad","kar","f_","fj_","u_","s_")): role="admin"
            token=_register_ui_button(b,role,text)
            custom_text=str(_button_setting(token,"text","") or "").strip()
            visible=str(_button_setting(token,"visible","true")).lower() != "false"
            if not visible:
                continue
            if custom_text:
                b["text"] = custom_text
            if "style" not in b:
                b["style"]=_ui_button_style(role,"danger" if role=="back" else ("success" if role=="buy" else "primary"),token=token)
            nr.append(b)
        if nr:
            out.append(nr)
    return out

def _button_manager_items():
    with _UI_BUTTON_REGISTRY_LOCK:
        return list(_UI_BUTTON_REGISTRY.values())

def show_button_manager(chat_id, mid=None):
    items=_button_manager_items()
    if not items:
        txt="🎛 **BUTTON MANAGER**\n\nOpen the Admin Panel menus once so their buttons can be registered here.\n\nTelegram supports only Primary/Success/Danger native button colors."
        kb=[[{"text":"⬅️ Back","callback_data":"m_ui"}]]
    else:
        items=items[-40:]
        rows=[]
        for it in items:
            token=it["token"]
            style=_ui_button_style(it.get("role","admin"),"primary",token=token)
            label=it.get("text","Button")[:26]
            rows.append([{"text":f"⚙️ {label} · {style.title()}","callback_data":f"uib:{token}"}])
        rows.append([{"text":"⬅️ Back","callback_data":"m_ui"}])
        txt="🎛 **BUTTON MANAGER**\n\nSelect any registered button to edit its individual settings.\n\n• Native color: Primary / Success / Danger\n• Custom label / emoji\n• Show / Hide\n• Per-button override"
        kb=rows
    if mid: tg_call("deleteMessage",{"chat_id":chat_id,"message_id":mid})
    else: _admin_clear_menu(chat_id)
    return _admin_store_menu(chat_id,tg_call("sendMessage",{"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":_style_inline_keyboard(kb,"admin")}}))

def show_button_editor(chat_id, mid, token):
    with _UI_BUTTON_REGISTRY_LOCK:
        it=dict(_UI_BUTTON_REGISTRY.get(token) or {})
    if not it:
        show_button_manager(chat_id,mid); return
    role=it.get("role","admin")
    current=_ui_button_style(role,"primary",token=token)
    visible=str(_button_setting(token,"visible","true")).lower() != "false"
    custom=str(_button_setting(token,"text","") or "")
    base=it.get("text","Button")
    title=custom or base
    kb=[
        [{"text":("✅ " if current=="primary" else "")+"🔵 Primary","callback_data":f"uibset:{token}:style:primary"}],
        [{"text":("✅ " if current=="success" else "")+"🟢 Success","callback_data":f"uibset:{token}:style:success"}],
        [{"text":("✅ " if current=="danger" else "")+"🔴 Danger","callback_data":f"uibset:{token}:style:danger"}],
        [{"text":"✏️ Edit Label / Emoji","callback_data":f"uibedit:{token}:text"}],
        [{"text":"👁 Hide Button" if visible else "👁 Show Button","callback_data":f"uibset:{token}:visible:{str(not visible).lower()}"}],
        [{"text":"♻️ Reset This Button","callback_data":f"uibset:{token}:reset:1"}],
        [{"text":"⬅️ Back","callback_data":"ui_buttons"}]
    ]
    txt=f"⚙️ **BUTTON SETTINGS**\n\nButton: `{title}`\nCallback: `{it.get('callback_data') or 'URL button'}`\nCurrent color: `{current}`\nVisibility: `{('Visible' if visible else 'Hidden')}`\n\n**Controls:** native color, label/emoji, visibility and per-button override.\n\n⚠️ Telegram does not expose arbitrary HEX/gradient colors for bot buttons; only the three native styles are supported."
    tg_call("editMessageText",{"chat_id":chat_id,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":_style_inline_keyboard(kb,"admin")}})

def _show_ui_studio_new(chat_id,mid=None):
    admin=_ui_button_style("admin","primary"); buy=_ui_button_style("buy","success"); back=_ui_button_style("back","danger")
    kb=[[{"text":f"🛠 Admin Buttons: {admin.title()}","callback_data":"ui_pick:admin"}],
        [{"text":f"🛒 Buy/Action Buttons: {buy.title()}","callback_data":"ui_pick:buy"}],
        [{"text":f"↩️ Back/Delete Buttons: {back.title()}","callback_data":"ui_pick:back"}],
        [{"text":"🎛 Button Manager","callback_data":"ui_buttons"}],
        [{"text":"⬅️ Back","callback_data":"adm_home"}]]
    if mid: tg_call("deleteMessage",{"chat_id":chat_id,"message_id":mid})
    else: _admin_clear_menu(chat_id)
    txt="🎨 **UI STUDIO**\n\nGlobal role colors + individual Button Manager.\n\n🔵 Primary • 🟢 Success • 🔴 Danger\n\nTelegram bots do not receive a native long-press/hold event, so individual settings are opened from Button Manager."
    return _admin_store_menu(chat_id,tg_call("sendMessage",{"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":_style_inline_keyboard(kb,"admin")}}))

def _ui_button_style_legacy(role="default", fallback="primary"):
    return _ui_button_style(role,fallback)

def _key_inline_send(chat_id,text,rows,actions,track=True):
    _clear_key_flow(chat_id)
    kb=[]
    for row in rows or []:
        nr=[]
        for label in row or []:
            label=str(label); action=str((actions or {}).get(label,"")); b={"text":label}
            if action.startswith("PAYURL:"):
                b["url"]=action[7:].strip(); b["style"]=_ui_button_style("buy","success")
            else:
                b["callback_data"]=action[:64]
                back="BACK" in label.upper()
                b["style"]=_ui_button_style("back" if back else "buy","danger" if back else "success")
            nr.append(b)
        kb.append(nr)
    return _key_send(chat_id,{"chat_id":chat_id,"text":text,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}},track=track)

def _clear_key_flow(chat_id):
    mids = list(KEY_FLOW_MESSAGES.pop(chat_id, set()))
    if not mids:
        return
    for mid in mids:
        try:
            _async_delete_message(chat_id, mid)
        except Exception:
            pass

def show_admin_home(chat_id, mid=None):
    kb=[[{"text":"📦 Files","callback_data":"m_files"},{"text":"📢 Force Join","callback_data":"m_fj"}],
        [{"text":"📣 Broadcast","callback_data":"m_bc"},{"text":"👥 Users","callback_data":"m_users"}],
        [{"text":"⚙️ Settings","callback_data":"m_sett"},{"text":"🔑 Keys","callback_data":"m_keys"}],
        [{"text":"⚙️ /setup Content","callback_data":"m_setup"},{"text":"📊 Statistics","callback_data":"m_stats"}],
        [{"text":"🎨 UI Studio","callback_data":"m_ui"}],
        [{"text":"🚫 Hide Admin Panel","callback_data":"hide_admin"}]]
    txt="⚡ **ADMIN CONTROL**\n\nManage files, Force Join, broadcasts, users, keys and /setup.\n\n🟢 System Online"
    if mid: tg_call("deleteMessage",{"chat_id":chat_id,"message_id":mid})
    else: _admin_clear_menu(chat_id)
    return _admin_store_menu(chat_id,tg_call("sendMessage",{"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":_style_inline_keyboard(mono_admin_kb(kb),"admin")}}))

def show_ui_studio(chat_id,mid=None):
    return _show_ui_studio_new(chat_id,mid)

def show_ui_picker(chat_id,mid,role):
    role=role if role in {"admin","buy","back"} else "admin"
    current=_ui_button_style(role,{"admin":"primary","buy":"success","back":"danger"}[role])
    title={"admin":"Admin Buttons","buy":"Buy/Action Buttons","back":"Back/Delete Buttons"}[role]
    kb=[]
    for val,label in [("primary","🔵 Primary"),("success","🟢 Success"),("danger","🔴 Danger")]:
        kb.append([{"text":("✅ " if current==val else "")+label,"callback_data":f"ui_set:{role}:{val}"}])
    kb.append([{"text":"⬅️ Back","callback_data":"m_ui"}])
    tg_call("editMessageText",{"chat_id":chat_id,"message_id":mid,"text":f"🎨 **{title}**\n\nCurrent: `{current}`\nChoose a button color:","parse_mode":"Markdown","reply_markup":{"inline_keyboard":_style_inline_keyboard(kb,"admin")}})

def show_files_menu(chat_id, mid=None):
    kb = [
        [{"text": "➕ "+mono("Add File"), "callback_data": "f_add"}, {"text": "🗑 "+mono("Remove File"), "callback_data": "f_del"}],
        [{"text": "✏️ "+mono("Edit File"), "callback_data": "f_edit"}, {"text": "📁 "+mono("List Files"), "callback_data": "f_list"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = f"📦 **Files Management:**"
    if mid:
        _async_delete_message(chat_id, mid)
    else:
        _admin_clear_menu(chat_id)
    return _admin_store_menu(chat_id, tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}}))

def show_fj_menu(chat_id, mid=None):
    kb = [
        [{"text": "📝 "+mono("Caption"), "callback_data": "fj_caption"}, {"text": "🖼 "+mono("Poster (Add/Remove)"), "callback_data": "fj_poster"}],
        [{"text": "➕ "+mono("ADD (Slot)"), "callback_data": "fj_add"}, {"text": "✏️ "+mono("EDIT (Slot)"), "callback_data": "fj_edit"}],
        [{"text": "🔘 "+mono("ON/OFF (Slot)"), "callback_data": "fj_onoff"}, {"text": "🗑 "+mono("REMOVE (Slot)"), "callback_data": "fj_rem"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = f"📢 **Force Join Configuration:**"
    if mid:
        _async_delete_message(chat_id, mid)
    else:
        _admin_clear_menu(chat_id)
    return _admin_store_menu(chat_id, tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}}))

def _settings_button(text, callback, font="mono"):
    fonts = {"mono": mono, "bold": lambda x: str(x).translate(str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", "𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇")),
             "serif": lambda x: str(x).translate(str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", "𝑨𝑩𝑪𝑫𝑬𝑭𝑮𝑯𝑰𝑱𝑲𝑳𝑴𝑵𝑶𝑷𝑸𝑹𝑺𝑻𝑼𝑽𝑾𝑿𝒀𝒁𝒂𝒃𝒄𝒅𝒆𝒇𝒈𝒉𝒊𝒋𝒌𝒍𝒎𝒏𝒐𝒑𝒒𝒓𝒔𝒕𝒖𝒗𝒘𝒙𝒚𝒛"))}
    return {"text": fonts.get(font, mono)(text), "callback_data": callback}

def show_settings_menu(chat_id, mid=None):
    txt="⚙️ **SETTINGS HUB**\n\nOrganized controls for every major part of the bot.\nChoose a section to configure it."
    kb=[
        [_settings_button("📢 Force Join & Media","set_grp_fj","mono")],
        [_settings_button("✅ Verification & Cleanup","set_grp_verify","bold")],
        [_settings_button("📝 Messages & User Experience","set_grp_msg","serif")],
        [_settings_button("📦 Delivery & Links","set_grp_delivery","mono")],
        [_settings_button("🔘 Buttons & Layout","set_grp_buttons","bold")],
        [_settings_button("🎞 Animation & System","set_grp_system","serif")],
        [_settings_button("⬅️ Back","adm_home","mono")]
    ]
    if mid:
        _async_delete_message(chat_id, mid)
    else:
        _admin_clear_menu(chat_id)
    data={"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}}
    return _admin_store_menu(chat_id, tg_call("sendMessage",data))

def show_settings_group(cid, mid, group):
    fonts = {
        "fj": ("📢 FORCE JOIN & MEDIA", "Manage channels, poster and Force Join presentation."),
        "verify": ("✅ VERIFICATION & CLEANUP", "Control verification rules and temporary-message cleanup."),
        "msg": ("📝 MESSAGES & USER EXPERIENCE", "Customize what users see throughout the bot."),
        "delivery": ("📦 DELIVERY & LINKS", "Control file expiry, notices and destination links."),
        "buttons": ("🔘 BUTTONS & LAYOUT", "Control button arrangement and presentation."),
        "system": ("🎞 ANIMATION & SYSTEM", "Animation controls plus core system information."),
    }
    title, desc = fonts.get(group, ("⚙️ SETTINGS", "Configure the bot."))
    if group == "fj":
        kb=[[{"text":"📢 "+mono("Channels"),"callback_data":"m_fj"}],
            [{"text":"🖼 "+mono("Poster"),"callback_data":"fj_poster"},{"text":"📝 "+mono("Caption"),"callback_data":"fj_caption"}],
            [{"text":"🔘 "+mono("Channel Buttons"),"callback_data":"s_layout"}]]
    elif group == "verify":
        kb=[[{"text":"✅ "+mono("Verification"),"callback_data":"set_verify"}],
            [{"text":"🗑 "+mono("Message Cleanup"),"callback_data":"set_cleanup"}],
            [{"text":"🔄 "+mono("Retry Message"),"callback_data":"msg_retry"}]]
    elif group == "msg":
        kb=[[{"text":"👋 "+mono("Welcome"),"callback_data":"s_wel"},{"text":"⚠️ "+mono("Error"),"callback_data":"s_err"}],
            [{"text":"🔄 "+mono("Retry"),"callback_data":"msg_retry"}],
            [{"text":"⚠️ "+mono("Important Notice"),"callback_data":"s_warn"}]]
    elif group == "delivery":
        kb=[[{"text":"⏱ "+mono("Delete Timer"),"callback_data":"s_timer"},{"text":"📟 "+mono("Update Button"),"callback_data":"s_upbtn"}],
            [{"text":"⚠️ "+mono("Important Notice"),"callback_data":"s_warn"}],
            [{"text":"🔗 "+mono("JOIN NOW Link"),"callback_data":"notice_btn_edit"}]]
    elif group == "buttons":
        kb=[[{"text":"🔘 "+mono("Force Join Layout"),"callback_data":"s_layout"}],
            [{"text":"👁 "+mono("Preview Layout"),"callback_data":"anim_preview"}]]
    else:
        kb=[[{"text":"🎞 "+mono("Animation"),"callback_data":"set_anim"}],
            [{"text":"🛠 "+mono("System Info"),"callback_data":"set_system"}]]
    kb.append([{"text":"⬅️ "+mono("Back to Settings"),"callback_data":"m_sett"}])
    if mid: _async_delete_message(cid, mid)
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage", {"chat_id":cid,"text":f"{title}\n\n{desc}","parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}}))

def settings_submenu(cid,mid,kind):
    def status(k,default="true"): return "ON" if get_setting(k,default).lower()=="true" else "OFF"
    if kind=="verify":
        txt=f"✅ **VERIFICATION**\n\nStatus: `{status('verification_enabled')}`\nMethod: `BOTH` — Joined + Join Request\n\nBoth methods are accepted. Request approval is not required."
        kb=[[{"text":"🟢 ON","callback_data":"verify_on"},{"text":"🔴 OFF","callback_data":"verify_off"}],
            [{"text":"🔍 "+mono("Verification Method"),"callback_data":"verify_method"}],
            [{"text":"🔄 "+mono("Retry Message"),"callback_data":"msg_retry"}],
            [{"text":"🗑 "+mono("Cleanup"),"callback_data":"set_cleanup"}],
            [{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    elif kind=="anim":
        txt=f"🎞 **ANIMATION**\n\nStatus: `{status('anim_enabled')}`\nSpeed: `{get_setting('anim_speed','0.50')}s`\nText: `{get_setting('anim_text','Verifying')}`"
        kb=[[{"text":"🟢 ON","callback_data":"anim_on"},{"text":"🔴 OFF","callback_data":"anim_off"}],
            [{"text":"⚡ "+mono("Speed"),"callback_data":"anim_speed"},{"text":"✏️ "+mono("Base Text"),"callback_data":"anim_text"}],
            [{"text":"👁 "+mono("Preview"),"callback_data":"anim_preview"}],
            [{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    elif kind=="cleanup":
        txt="🗑 **MESSAGE CLEANUP**\n\nChoose exactly which temporary messages are removed automatically."
        items=[("Force Join","cleanup_force"),("Retry Message","cleanup_retry"),("Animation","cleanup_anim"),("Delivered File","cleanup_file"),("Important Notice","cleanup_notice")]
        kb=[]
        for label,key in items:
            kb.append([{"text":f"{label}: {'🟢 ON' if status(key) == 'ON' else '🔴 OFF'}","callback_data":key}])
        kb += [[{"text":"⏱ "+mono("Delivery Timer"),"callback_data":"s_timer"}],[{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    elif kind=="msg":
        txt="📝 **MESSAGES**\n\nEdit every user-facing message without touching the code."
        kb=[[{"text":"👋 "+mono("Welcome"),"callback_data":"s_wel"},{"text":"⚠️ "+mono("Error"),"callback_data":"s_err"}],
            [{"text":"⚠️ "+mono("Important Notice"),"callback_data":"s_warn"}],
            [{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    else:
        txt=f"{kind.replace('_',' ').title()} Settings"
        kb=[[{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    if mid: _async_delete_message(cid, mid)
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}}))

def show_users_menu(chat_id, mid=None):
    cnt = len(supabase.table("users").select("telegram_user_id").execute().data or []) if supabase else 0
    kb = [
        [{"text": "📜 View Serial List", "callback_data": "u_list"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = f"👥 **Users Overview:**\n\n• Total Active Users: `{cnt}`"
    if mid:
        _async_delete_message(chat_id, mid)
    else:
        _admin_clear_menu(chat_id)
    return _admin_store_menu(chat_id, tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}}))

@app.route("/", methods=["GET"])
def home():
    return "Bot Core Online 🚀"

@app.route("/healthz", methods=["GET"])
def healthz():
    return {"status": "ok", "telegram_configured": bool(BOT_TOKEN), "supabase_configured": bool(supabase)}, 200

def _original_process_callback_query(cq):
    uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")

    if uid in ADMIN_IDS:
        if cdata == "adm_home":
            ADMIN_SESSIONS.pop(uid, None)
            show_admin_home(cid, mid)
        elif cdata == "m_files":
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid, mid)
        elif cdata == "m_fj":
            ADMIN_SESSIONS.pop(uid, None)
            show_fj_menu(cid, mid)
        elif cdata == "m_bc":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "WAIT_BC_CONTENT"}
            tg_call("sendMessage", {"chat_id": cid, "text": "📣 Please forward or send the message to broadcast:"})
        elif cdata == "m_users":
            ADMIN_SESSIONS.pop(uid, None)
            show_users_menu(cid, mid)
        elif cdata == "m_sett":
            ADMIN_SESSIONS.pop(uid, None)
            show_settings_menu(cid, mid)
        elif cdata == "m_stats":
            users=len(supabase.table("users").select("telegram_user_id").execute().data or []) if supabase else 0
            files=len(get_all_apps())
            active=len([c for c in get_channels() if c.get("active")])
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"📊 **STATISTICS**\n\n👥 Users: `{users}`\n📦 Files: `{files}`\n📢 Active Channels: `{active}`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"⬅️ Back","callback_data":"adm_home"}]])}})
        elif cdata == "set_grp_fj":
            show_settings_group(cid,mid,"fj")
        elif cdata == "set_grp_verify":
            show_settings_group(cid,mid,"verify")
        elif cdata == "set_grp_msg":
            show_settings_group(cid,mid,"msg")
        elif cdata == "set_grp_delivery":
            show_settings_group(cid,mid,"delivery")
        elif cdata == "set_grp_buttons":
            show_settings_group(cid,mid,"buttons")
        elif cdata == "set_grp_system":
            show_settings_group(cid,mid,"system")
        elif cdata == "set_user":
            show_settings_group(cid,mid,"msg")
        elif cdata == "set_fj":
            show_settings_group(cid,mid,"fj")
        elif cdata == "set_delivery":
            kb=[[{"text":"⏱ "+mono("Delete Timer"),"callback_data":"s_timer"},{"text":"📟 "+mono("Update Button"),"callback_data":"s_upbtn"}], [{"text":"⚠️ "+mono("Important Notice"),"callback_data":"s_warn"}], [{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":"📦 **FILE DELIVERY**\n\nControl delivery mode, expiry timer and post-delivery notice.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
        elif cdata == "set_buttons":
            kb=[[{"text":"🔘 "+mono("Force Join Layout"),"callback_data":"s_layout"}], [{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":"🔘 **BUTTONS**\n\nConfigure button layout and presentation.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
        elif cdata == "set_media":
            show_fj_menu(cid,mid)
        elif cdata == "set_links":
            kb=[[{"text":"📟 "+mono("Update Button"),"callback_data":"s_upbtn"},{"text":"⚠️ "+mono("JOIN NOW"),"callback_data":"s_warn"}], [{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":"🔗 **LINKS**\n\nManage update and JOIN NOW destinations.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
        elif cdata == "set_system":
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":"🛠 **SYSTEM**\n\n⚡ Webhook-first processing\n⚡ Parallel channel verification\n⚡ Cached settings and channels\n⚡ Dynamic channel count","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"⬅️ Back","callback_data":"m_sett"}]])}})
        elif cdata in {"set_verify","set_anim","set_cleanup","set_msg"}:
            settings_submenu(cid,mid,cdata[4:])
        elif cdata == "verify_on":
            set_setting("verification_enabled","true"); settings_submenu(cid,mid,"verify")
        elif cdata == "verify_off":
            set_setting("verification_enabled","false"); settings_submenu(cid,mid,"verify")
        elif cdata == "verify_method":
            kb=[[{"text":"⭐ BOTH — Joined + Request","callback_data":"vm_both"}],[{"text":"👤 Joined Only","callback_data":"vm_join"}],[{"text":"📨 Request Only","callback_data":"vm_req"}],[{"text":"⬅️ Back","callback_data":"set_verify"}]]
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":"🔍 **VERIFICATION METHOD**\n\nRecommended: BOTH","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
        elif cdata.startswith("vm_"):
            set_setting("verification_method",{"vm_both":"both","vm_join":"joined","vm_req":"request"}.get(cdata,"both")); settings_submenu(cid,mid,"verify")
        elif cdata == "msg_retry":
            cur=get_setting("verify_retry_text", DEFAULT_VERIFY_RETRY); _admin_begin_input(cid,uid,{"step":"SET_VERIFY_RETRY"},f"📝 CURRENT MESSAGE:\n\n{cur}\n\n✏️ Send the new Retry message:")
        elif cdata.startswith("cleanup_"):
            k=cdata; cur=get_setting(k,"true").lower()=="true"; set_setting(k,"false" if cur else "true"); settings_submenu(cid,mid,"cleanup")
        elif cdata == "anim_preview":
            tg_call("sendMessage",{"chat_id":cid,"text":"Verifying.\nVerifying..\nVerifying..."})

        # --- Files Sub-Module ---
        elif cdata == "f_add":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "F_TITLE"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter file title:"})

        elif cdata.startswith("fmode_"):
            mode = cdata.split("_")[1]
            sess = ADMIN_SESSIONS.get(uid, {})
            if supabase and "t" in sess and "c" in sess and "fid" in sess:
                payload = {
                    "title": sess["t"],
                    "deep_link_code": sess["c"],
                    "storage_message_id": sess["fid"],
                    "sender_mode": mode,
                    "active": True
                }
                try:
                    supabase.table("apps").upsert(payload, on_conflict="deep_link_code").execute()
                except Exception as exc:
                    print(f"APPS UPSERT sender_mode fallback: {exc}", flush=True)
                    payload.pop("sender_mode", None)
                    supabase.table("apps").upsert(payload, on_conflict="deep_link_code").execute()
                tg_call("sendMessage", {"chat_id": cid, "text": f"Success\nLink: https://t.me/LexxiAdminBot?start={sess['c']}"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid)

        elif cdata == "f_del" or cdata.startswith("f_del_page_"):
            page = int(cdata.split("_")[-1]) if cdata.startswith("f_del_page_") else 0
            apps, total = get_app_page(page)
            if not apps:
                tg_call("sendMessage", {"chat_id": cid, "text": "No files found."})
                show_files_menu(cid)
            else:
                btns = [[{"text": f"🗑 {a['title']} ({a['deep_link_code']})", "callback_data": f"cf_fdel_{a['deep_link_code']}"}] for a in apps]
                nav=[]
                if page>0: nav.append({"text":"◀️ Previous","callback_data":f"f_del_page_{page-1}"})
                if (page+1)*20<total: nav.append({"text":"Next ▶️","callback_data":f"f_del_page_{page+1}"})
                if nav: btns.append(nav)
                btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "m_files"}])
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Select file to remove (Page {page+1}/{max(1,(total+19)//20)}):", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("cf_fdel_"):
            f_code = cb_suffix(cdata, "cf_fdel_")
            kb = [[{"text": "🗑 Confirm Delete", "callback_data": f"act_fdel_{f_code}"}], [{"text": "❌ Cancel", "callback_data": "m_files"}]]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Permanently delete `{f_code}`?", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("act_fdel_"):
            f_code = cb_suffix(cdata, "act_fdel_")
            if supabase:
                supabase.table("apps").delete().eq("deep_link_code", f_code).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Deleted"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            show_files_menu(cid)

        elif cdata == "f_edit" or cdata.startswith("f_edit_page_"):
            page = int(cdata.split("_")[-1]) if cdata.startswith("f_edit_page_") else 0
            apps, total = get_app_page(page)
            if not apps:
                tg_call("sendMessage", {"chat_id": cid, "text": "No files found."})
                show_files_menu(cid)
            else:
                btns = [[{"text": f"✏️ {a['title']} ({a['deep_link_code']})", "callback_data": f"edopt_{a['deep_link_code']}"}] for a in apps]
                nav=[]
                if page>0: nav.append({"text":"◀️ Previous","callback_data":f"f_edit_page_{page-1}"})
                if (page+1)*20<total: nav.append({"text":"Next ▶️","callback_data":f"f_edit_page_{page+1}"})
                if nav: btns.append(nav)
                btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "m_files"}])
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Select file to edit (Page {page+1}/{max(1,(total+19)//20)}):", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("edopt_"):
            f_code = cb_suffix(cdata, "edopt_")
            kb = [
                [{"text": "Title", "callback_data": f"ef_t_{f_code}"}, {"text": "Code", "callback_data": f"ef_c_{f_code}"}],
                [{"text": "File Post", "callback_data": f"ef_p_{f_code}"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "f_edit"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Edit component for `{f_code}`:", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("ef_t_"):
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_TITLE", "code": cb_suffix(cdata, "ef_t_")}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter new title:"})
        elif cdata.startswith("ef_c_"):
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_CODE", "old_code": cb_suffix(cdata, "ef_c_")}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter new short code:"})
        elif cdata.startswith("ef_p_"):
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_FILE", "code": cb_suffix(cdata, "ef_p_")}
            tg_call("sendMessage", {"chat_id": cid, "text": "Forward new post from Storage Channel:"})

        elif cdata == "f_list" or cdata.startswith("f_list_page_"):
            page = int(cdata.split("_")[-1]) if cdata.startswith("f_list_page_") else 0
            apps, total = get_app_page(page)
            if not apps:
                tg_call("sendMessage", {"chat_id": cid, "text": "📁 No files registered yet."})
            else:
                for a in apps:
                    code=a.get("deep_link_code")
                    title=a.get("title")
                    link=f"https://t.me/LexxiAdminBot?start={code}"
                    tg_call("sendMessage", {"chat_id": cid, "text": f"📦 **{title}**\n• Code: `{code}`\n• Link: {link}"})
                nav=[]
                if page>0: nav.append({"text":"◀️ Previous","callback_data":f"f_list_page_{page-1}"})
                if (page+1)*20<total: nav.append({"text":"Next ▶️","callback_data":f"f_list_page_{page+1}"})
                if nav: tg_call("sendMessage", {"chat_id": cid, "text": f"Files Page {page+1}/{max(1,(total+19)//20)}", "reply_markup":{"inline_keyboard":[nav]}})
            show_files_menu(cid)

        # --- Force Join Sub-Module ---
        elif cdata == "fj_caption":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_FJ_CAPTION"}
            cur = get_setting("force_join_text", DEFAULT_FJ)
            tg_call("sendMessage", {"chat_id": cid, "text": f"📝 CURRENT CAPTION:\n\n{cur}\n\n✏️ Send the new Force Join caption.\nUse {{name}} for the dynamic user name:"})

        elif cdata == "fj_poster":
            kb = [
                [{"text": "➕ Set / Replace Poster", "callback_data": "fjp_add"}],
                [{"text": "🗑 Remove Poster", "callback_data": "fjp_rem"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "🖼 Poster Management:", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata == "fjp_add":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_POSTER"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Forward image/post directly from Storage Channel:"})

        elif cdata == "fjp_rem":
            set_setting("poster_storage_id", "")
            tg_call("sendMessage", {"chat_id": cid, "text": "Deleted"})
            show_fj_menu(cid)

        elif cdata == "fj_add":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "FJ_ADD_TITLE"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter channel button text (title):"})

        elif cdata == "fj_edit":
            chs = get_channels()
            if not chs:
                tg_call("sendMessage", {"chat_id": cid, "text": "No channels found."})
                show_fj_menu(cid)
            else:
                btns = [[{"text": f"Slot {c['slot']}: {c.get('button_text')}", "callback_data": f"fje_{c['slot']}"}] for c in chs]
                btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}])
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select slot to edit:", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("fje_"):
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "FJ_E_TITLE", "slot": int(cdata.split("_")[1])}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter updated channel title:"})

        elif cdata == "fj_onoff":
            kb = [
                [{"text": "🟢 Turn ON Channels", "callback_data": "fj_list_to_on"}],
                [{"text": "🔴 Turn OFF Channels", "callback_data": "fj_list_to_off"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select toggle operation:", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata == "fj_list_to_on":
            chs = [c for c in get_channels() if not c.get("active")]
            if not chs:
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "No inactive channels found.", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ "+mono("Back"), "callback_data": "fj_onoff"}]]}})
            else:
                btns = [[{"text": f"Turn ON: Slot {c['slot']} ({c.get('button_text')})", "callback_data": f"cf_on_{c['slot']}"}] for c in chs]
                btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "fj_onoff"}])
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select channel to enable:", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata == "fj_list_to_off":
            chs = [c for c in get_channels() if c.get("active")]
            if not chs:
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "No active channels found.", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ "+mono("Back"), "callback_data": "fj_onoff"}]]}})
            else:
                btns = [[{"text": f"Turn OFF: Slot {c['slot']} ({c.get('button_text')})", "callback_data": f"cf_off_{c['slot']}"}] for c in chs]
                btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "fj_onoff"}])
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select channel to disable:", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("cf_on_"):
            s = int(cdata.split("_")[2])
            kb = [[{"text": "✅ Confirm Turn ON", "callback_data": f"act_on_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_onoff"}]]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Enable Slot {s}?", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("cf_off_"):
            s = int(cdata.split("_")[2])
            kb = [[{"text": "✅ Confirm Turn OFF", "callback_data": f"act_off_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_onoff"}]]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Disable Slot {s}?", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("act_on_"):
            s = int(cdata.split("_")[2])
            if supabase:
                supabase.table("force_join_channels").update({"active": True}).eq("slot", s).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            show_fj_menu(cid)

        elif cdata.startswith("act_off_"):
            s = int(cdata.split("_")[2])
            if supabase:
                supabase.table("force_join_channels").update({"active": False}).eq("slot", s).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            show_fj_menu(cid)

        elif cdata == "fj_rem":
            chs = get_channels()
            if not chs:
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "No channels available to remove.", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}]]}})
            else:
                btns = [[{"text": f"Remove Slot {c['slot']} ({c.get('button_text')})", "callback_data": f"cf_del_{c['slot']}"}] for c in chs]
                btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}])
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select channel to remove:", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("cf_del_"):
            s = int(cdata.split("_")[2])
            kb = [[{"text": "🗑 Confirm Delete", "callback_data": f"act_del_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_rem"}]]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Permanently delete Slot {s}?", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("act_del_"):
            s = int(cdata.split("_")[2])
            if supabase:
                supabase.table("force_join_channels").delete().eq("slot", s).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Deleted"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            show_fj_menu(cid)

        # --- Broadcast Sub-Module ---
        elif cdata == "send_confirmed_bc":
            bc_data = ADMIN_SESSIONS.get(uid, {}).get("bc_payload")
            if not bc_data:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
                show_admin_home(cid)
                return "OK", 200
            ADMIN_SESSIONS.pop(uid, None)
            users = supabase.table("users").select("telegram_user_id").execute().data or [] if supabase else []
            for u in users:
                tg_call("copyMessage", {"chat_id": u["telegram_user_id"], "from_chat_id": bc_data["from_chat"], "message_id": bc_data["mid"]})
                time.sleep(0.05)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_admin_home(cid)

        # --- Users Sub-Module ---
        elif cdata == "u_list":
            users = supabase.table("users").select("*").limit(100).execute().data or [] if supabase else []
            if not users:
                tg_call("sendMessage", {"chat_id": cid, "text": "User database is empty."})
            else:
                lines = ["👥 **Users List:**\n"]
                for idx, u in enumerate(users, start=1):
                    uname_str = f"(@{u.get('username')})" if u.get('username') else ""
                    lines.append(f"{idx}. {u.get('first_name', 'User')} {uname_str} — `{u.get('telegram_user_id')}`")
                tg_call("sendMessage", {"chat_id": cid, "text": "\n".join(lines), "parse_mode": "Markdown"})
            show_users_menu(cid)

        # --- Settings Sub-Module ---
        elif cdata == "s_wel":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_W"}
            cur = get_setting("welcome_text", DEFAULT_WELCOME)
            tg_call("sendMessage", {"chat_id": cid, "text": f"📝 CURRENT WELCOME MESSAGE:\n\n{cur}\n\n✏️ Send the new welcome message:"})
        elif cdata == "s_err":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_E"}
            cur = get_setting("error_feedback_text", DEFAULT_ERROR)
            tg_call("sendMessage", {"chat_id": cid, "text": f"📝 CURRENT ERROR MESSAGE:\n\n{cur}\n\n✏️ Send the new error message:"})
        elif cdata == "s_warn":
            notice_state = "ON" if get_setting("notice_enabled", "true").lower() == "true" else "OFF"
            btn_state = "ON" if get_setting("notice_btn_enabled", "true").lower() == "true" else "OFF"
            kb = [
                [{"text": "🟢 "+mono("Notice ON"), "callback_data": "notice_on"}, {"text": "🔴 "+mono("Notice OFF"), "callback_data": "notice_off"}],
                [{"text": "✏️ "+mono("Edit Notice Text"), "callback_data": "notice_text"}],
                [{"text": "🟢 "+mono("JOIN NOW ON"), "callback_data": "notice_btn_on"}, {"text": "🔴 "+mono("JOIN NOW OFF"), "callback_data": "notice_btn_off"}],
                [{"text": "✏️ "+mono("Edit JOIN NOW Link"), "callback_data": "notice_btn_edit"}, {"text": "🗑 "+mono("REMOVE JOIN NOW"), "callback_data": "notice_btn_remove"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_sett"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"⚠️ **Important Notice**\n\nNotice: `{notice_state}`\nJOIN NOW: `{btn_state}`", "reply_markup": {"inline_keyboard": kb}})
        elif cdata == "notice_on":
            set_setting("notice_enabled", "true")
            tg_call("sendMessage", {"chat_id": cid, "text": "Success!"})
            show_settings_menu(cid)
        elif cdata == "notice_off":
            set_setting("notice_enabled", "false")
            tg_call("sendMessage", {"chat_id": cid, "text": "Success!"})
            show_settings_menu(cid)
        elif cdata == "notice_text":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_WARN"}
            cur = get_setting("auto_delete_warn_text", DEFAULT_WARN)
            tg_call("sendMessage", {"chat_id": cid, "text": f"📝 CURRENT IMPORTANT NOTICE:\n\n{cur}\n\n✏️ Send the new Important Notice text.\nUse {{time}} for the dynamic duration:"})
        elif cdata == "notice_btn_on":
            set_setting("notice_btn_enabled", "true")
            tg_call("sendMessage", {"chat_id": cid, "text": "Success!"})
            show_settings_menu(cid)
        elif cdata == "notice_btn_off":
            set_setting("notice_btn_enabled", "false")
            tg_call("sendMessage", {"chat_id": cid, "text": "Success!"})
            show_settings_menu(cid)
        elif cdata == "notice_btn_remove":
            set_setting("notice_btn_enabled", "false")
            set_setting("notice_btn_text", "none")
            tg_call("sendMessage", {"chat_id": cid, "text": "Removed!"})
            show_settings_menu(cid)
        elif cdata == "notice_btn_edit":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_NOTICE_URL"}
            cur = get_setting("notice_btn_url", DEFAULT_NOTICE_BTN_URL)
            tg_call("sendMessage", {"chat_id": cid, "text": f"🔗 CURRENT JOIN NOW LINK:\n\n{cur}\n\n✏️ Send the new JOIN NOW link:"})
        elif cdata == "s_timer":
            kb = [
                [{"text": "30s", "callback_data": "st_30"}, {"text": "1 min", "callback_data": "st_60"}],
                [{"text": "2 min (Default)", "callback_data": "st_120"}, {"text": "3 min", "callback_data": "st_180"}],
                [{"text": "5 min", "callback_data": "st_300"}, {"text": "Custom", "callback_data": "st_custom"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_sett"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select auto-delete timer:", "reply_markup": {"inline_keyboard": kb}})
        elif cdata.startswith("st_"):
            sec = cdata.split("_")[1]
            if sec == "custom":
                _admin_clear_menu(cid)
                ADMIN_SESSIONS[uid] = {"step": "SET_CUSTOM_TIMER"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter time in seconds:"})
            else:
                set_setting("auto_delete_time", sec)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "s_upbtn":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_UP_TITLE"}
            cur = get_setting("update_btn_text", DEFAULT_UP_BTN)
            tg_call("sendMessage", {"chat_id": cid, "text": f"🔘 CURRENT UPDATE BUTTON LABEL:\n\n{cur}\n\n✏️ Send the new label, or send `none` to disable:"})
        elif cdata == "s_layout":
            kb = [
                [{"text": mono("1 Button Per Row"), "callback_data": "setlay_1"}],
                [{"text": mono("2 Buttons Per Row"), "callback_data": "setlay_2"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_sett"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select Force Join button layout:", "reply_markup": {"inline_keyboard": kb}})
        elif cdata.startswith("setlay_"):
            set_setting("button_layout", cdata.split("_")[1])
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif cdata == "s_anim":
            kb = [
                [{"text": "🟢 "+mono("Turn ON"), "callback_data": "anim_on"}, {"text": "🔴 "+mono("Turn OFF"), "callback_data": "anim_off"}],
                [{"text": "⚡ "+mono("Animation Speed"), "callback_data": "anim_speed"}],
                [{"text": "✏️ "+mono("Change Base Text"), "callback_data": "anim_text"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_sett"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "⏳ **Waiting Animation Settings:**", "reply_markup": {"inline_keyboard": kb}})
        elif cdata == "anim_on":
            set_setting("anim_enabled", "true")
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif cdata == "anim_off":
            set_setting("anim_enabled", "false")
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif cdata == "anim_speed":
            current_speed = get_setting("anim_speed", "0.75")
            kb = [
                [{"text": "⚡ 0.10s (Ultra Fast)", "callback_data": "aspeed_0.10"}, {"text": "🚀 0.25s (Fast)", "callback_data": "aspeed_0.25"}],
                [{"text": "🟢 0.50s (Default)", "callback_data": "aspeed_0.50"}, {"text": "🐢 1.00s (Slow)", "callback_data": "aspeed_1.00"}],
                [{"text": "⏱️ "+mono("Custom"), "callback_data": "aspeed_custom"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "s_anim"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"⚡ **Animation Speed:** `{current_speed}s`\n\nChoose how often the dots update:", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
        elif cdata.startswith("aspeed_"):
            speed_value = cdata.split("_", 1)[1]
            if speed_value == "custom":
                _admin_clear_menu(cid)
                ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_SPEED"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter animation speed in seconds (0.10 - 5.00):"})
            else:
                set_setting("anim_speed", speed_value)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "anim_text":
            _admin_clear_menu(cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_TEXT"}
            cur = get_setting("anim_text", DEFAULT_ANIM_TEXT)
            tg_call("sendMessage", {"chat_id": cid, "text": f"🎞 CURRENT ANIMATION TEXT:\n\n{cur}\n\n✏️ Send the new base text:"})

    return "OK", 200

# ========================= KEY STORE / BUYKEYS SYSTEM =========================
KEY_DATA_LOCK = threading.RLock()
KEY_DATA_KEYS = {
    "products": "keys_products_v1",
    "orders": "keys_orders_v1",
    "users": "keys_users_v1",
    "setup": "setup_global_v1",
}

USER_LANGUAGE_CACHE = {}
USER_LANGUAGE_LOCK = threading.Lock()

LANGUAGE_TEXTS = {
    "en": {
        "key_lang": "🌐 **Select Language**\n\nChoose your preferred language:",
        "key_home": "🔑 **KEY CENTER**\n\nChoose an option below.",
        "buy_keys": "🛒 **BUY KEYS**",
        "my_keys": "🔑 **MY KEYS**",
        "main_menu": "🏠 **MAIN MENU**",
        "select_product": "Select a product:",
        "no_products": "No products are available yet.",
        "select_duration": "Select your duration:",
        "duration_unavailable": "⚠️ This duration is currently unavailable.",
        "payment_disabled": "💳 **PAYMENT TEMPORARILY DISABLED**\n\nPayment verification/gateway has been removed for now. Please try again after a new payment provider is added.",
        "my_keys_empty": "🔑 **MY KEYS**\n\nYou don't have any active keys yet.",
        "my_keys_list": "🔑 **MY KEYS**\n\nLatest purchase is shown first. Select a key:",
        "key_not_found": "⚠️ Key not found.",
        "reset_request": "🔄 Reset request sent to admin.",
        "delete_request": "🗑 Delete request sent to admin.",
        "key_details": "🔐 **KEY DETAILS**\n\nProduct: {product}\nDuration: {duration}\nPrice: ₹{price}\nPurchased: {purchased}\nExpires: {expires}\nResets: {resets}/{max_resets}\n\n🔑 Key:\n`{key}`",
        "reset_success": "✅ **Key Reset Successful**",
        "delete_success": "🗑 **Key Deleted Successfully**",
    },
    "hi": {
        "key_lang": "🌐 **भाषा चुनें**\n\nअपनी पसंदीदा भाषा चुनें:",
        "key_home": "🔑 **की सेंटर**\n\nनीचे से एक विकल्प चुनें।",
        "buy_keys": "🛒 **की खरीदें**",
        "my_keys": "🔑 **मेरी कीज़**",
        "main_menu": "🏠 **मुख्य मेनू**",
        "select_product": "एक प्रोडक्ट चुनें:",
        "no_products": "अभी कोई प्रोडक्ट उपलब्ध नहीं है।",
        "select_duration": "अपनी अवधि चुनें:",
        "duration_unavailable": "⚠️ यह अवधि अभी उपलब्ध नहीं है।",
        "payment_disabled": "💳 **पेमेंट फिलहाल बंद है**\n\nपेमेंट वेरिफिकेशन/गेटवे अभी हटा दिया गया है। नया पेमेंट प्रोवाइडर जोड़ने के बाद फिर कोशिश करें।",
        "my_keys_empty": "🔑 **मेरी कीज़**\n\nआपके पास अभी कोई एक्टिव की नहीं है।",
        "my_keys_list": "🔑 **मेरी कीज़**\n\nसबसे नई खरीद पहले दिखाई गई है। की चुनें:",
        "key_not_found": "⚠️ की नहीं मिली।",
        "reset_request": "🔄 रीसेट रिक्वेस्ट एडमिन को भेज दी गई है।",
        "delete_request": "🗑 डिलीट रिक्वेस्ट एडमिन को भेज दी गई है।",
        "key_details": "🔐 **की की जानकारी**\n\nप्रोडक्ट: {product}\nअवधि: {duration}\nकीमत: ₹{price}\nखरीदा: {purchased}\nसमाप्त: {expires}\nरीसेट: {resets}/{max_resets}\n\n🔑 की:\n`{key}`",
        "reset_success": "✅ **की रीसेट सफल रहा**",
        "delete_success": "🗑 **की सफलतापूर्वक डिलीट हुई**",
    },
}

def _user_lang(user_id):
    uid=str(user_id)
    with USER_LANGUAGE_LOCK:
        lang=USER_LANGUAGE_CACHE.get(uid)
    if lang in ("en","hi"):
        return lang
    stored=str(get_setting(f"user_lang_{uid}", "") or "").lower()
    lang=stored if stored in ("en","hi") else "en"
    with USER_LANGUAGE_LOCK:
        USER_LANGUAGE_CACHE[uid]=lang
    return lang

def _has_user_lang(user_id):
    uid=str(user_id)
    with USER_LANGUAGE_LOCK:
        if USER_LANGUAGE_CACHE.get(uid) in ("en","hi"):
            return True
    stored=str(get_setting(f"user_lang_{uid}", "") or "").lower()
    return stored in ("en","hi")

def _set_user_lang(user_id, lang):
    lang=lang if lang in ("en","hi") else "en"
    uid=str(user_id)
    with USER_LANGUAGE_LOCK:
        USER_LANGUAGE_CACHE[uid]=lang
    try:
        _PROFILE_EXECUTOR.submit(_safe_run, set_setting, f"user_lang_{uid}", lang)
    except Exception:
        pass
    return lang

def _lt(lang, key, **kwargs):
    value=LANGUAGE_TEXTS.get(lang, LANGUAGE_TEXTS["en"]).get(key, LANGUAGE_TEXTS["en"].get(key, key))
    try:
        return value.format(**kwargs)
    except Exception:
        return value

DEFAULT_KEY_TEXTS = {
    "key_lang": LANGUAGE_TEXTS["en"]["key_lang"],
    "key_home_en": LANGUAGE_TEXTS["en"]["key_home"],
    "key_home_hi": LANGUAGE_TEXTS["hi"]["key_home"],
    "product_text": "🛒 **{product}**\n\nSelect your duration:",
    "duration_text": "📅 **{duration}**\n💰 Price: ₹{price}",
    "key_details": LANGUAGE_TEXTS["en"]["key_details"],
    "no_keys": LANGUAGE_TEXTS["en"]["my_keys_empty"],
    "sold_admin": "💰 **1 Key Sold**\n\nProduct: {product}\nDuration: {duration}\nUser ID: `{user_id}`",
    "stock_zero_admin": "⚠️ **Stock Empty**\n\n{product} → {duration} has reached 0 keys. The duration is now hidden from users.",
    "reset_success": LANGUAGE_TEXTS["en"]["reset_success"],
    "delete_success": LANGUAGE_TEXTS["en"]["delete_success"],
}

def _json_setting(key, default):
    raw = get_setting(key, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default

def _save_json_setting(key, value):
    set_setting(key, json.dumps(value, ensure_ascii=False, separators=(',', ':')))

def key_products():
    return _json_setting(KEY_DATA_KEYS["products"], [])

def key_orders():
    return _json_setting(KEY_DATA_KEYS["orders"], [])

def key_setup():
    return _json_setting(KEY_DATA_KEYS["setup"], [])

def _key_text(name, default=None, lang="en", **kwargs):
    lang=lang if lang in ("en","hi") else "en"
    custom_key = f"keymsg_hi_{name}" if lang=="hi" else f"keymsg_{name}"
    fallback = DEFAULT_KEY_TEXTS.get(name, default or name)
    if lang=="hi":
        hi_defaults={
            "key_lang": LANGUAGE_TEXTS["hi"]["key_lang"],
            "key_home_en": LANGUAGE_TEXTS["hi"]["key_home"],
            "key_home_hi": LANGUAGE_TEXTS["hi"]["key_home"],
            "product_text": "🛒 **{product}**\n\nअपनी अवधि चुनें:",
            "duration_text": "📅 **{duration}**\n💰 कीमत: ₹{price}",
            "key_details": LANGUAGE_TEXTS["hi"]["key_details"],
            "no_keys": LANGUAGE_TEXTS["hi"]["my_keys_empty"],
            "reset_success": LANGUAGE_TEXTS["hi"]["reset_success"],
            "delete_success": LANGUAGE_TEXTS["hi"]["delete_success"],
        }
        fallback=hi_defaults.get(name,fallback)
    value=get_setting(custom_key, "") or fallback
    try:
        return value.format(**kwargs)
    except Exception:
        return value

def _find_product(products, pid):
    return next((p for p in products if str(p.get("id")) == str(pid)), None)

def _find_duration(product, did):
    return next((d for d in product.get("durations", []) if str(d.get("id")) == str(did)), None) if product else None

def _available_keys(duration):
    now = time.time()
    return [k for k in duration.get("keys", []) if k.get("status", "available") == "available" and not k.get("disabled") and not k.get("assigned_to") and (not k.get("expires_at") or float(k.get("expires_at", 0)) > now)]

def _duration_days(name):
    m = re.search(r"(\d+(?:\.\d+)?)\s*(day|days|d)", str(name).lower())
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+(?:\.\d+)?)\s*(hour|hours|h)", str(name).lower())
    if m:
        return float(m.group(1)) / 24.0
    return None

def _new_id(prefix):
    return f"{prefix}{time.time_ns()}{secrets.token_hex(6)}"

def _save_products(products):
    with KEY_DATA_LOCK:
        _save_json_setting(KEY_DATA_KEYS["products"], products)

def _save_orders(orders):
    with KEY_DATA_LOCK:
        _save_json_setting(KEY_DATA_KEYS["orders"], orders)

def _save_setup(items):
    with KEY_DATA_LOCK:
        _save_json_setting(KEY_DATA_KEYS["setup"], items)

def _ensure_key_defaults():
    for name, value in DEFAULT_KEY_TEXTS.items():
        if not get_setting("keymsg_" + name, ""):
            set_setting("keymsg_" + name, value)

def _active_user_key(user_id):
    now = time.time()
    products = key_products()
    found = []
    for p in products:
        for d in p.get("durations", []):
            for k in d.get("keys", []):
                if str(k.get("assigned_to")) == str(user_id) and k.get("status") == "active":
                    if float(k.get("expires_at", 0) or 0) > now:
                        found.append((p, d, k))
    return found

def _format_dt(ts):
    if not ts:
        return "—"
    return time.strftime("%d %b %Y, %I:%M %p", time.localtime(float(ts)))

def _key_detail_text(p, d, k, lang="en"):
    max_resets = int(get_setting("key_max_resets", "3"))
    return _key_text("key_details", lang=lang, product=p.get("name",""), duration=d.get("name",""), price=d.get("price",0), purchased=_format_dt(k.get("purchased_at")), expires=_format_dt(k.get("expires_at")), resets=int(k.get("resets",0)), max_resets=max_resets, key=k.get("key",""))

def _send_user_key_message(chat_id, p, d, k, pin=True, lang="en"):
    text = _key_detail_text(p, d, k, lang=lang)
    r = tg_call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    if r.get("ok") and pin:
        tg_call("pinChatMessage", {"chat_id": chat_id, "message_id": r["result"]["message_id"], "disable_notification": True})
    return r

def _admin_notice(text):
    for aid in ADMIN_IDS:
        tg_call("sendMessage", {"chat_id": aid, "text": text, "parse_mode": "Markdown"})

def _key_lang_menu(chat_id):
    rows=[["🇬🇧 English","🇮🇳 हिंदी"]]
    return _key_inline_send(chat_id,LANGUAGE_TEXTS["en"]["key_lang"],rows,{rows[0][0]:"klang:en",rows[0][1]:"klang:hi"})

def _key_home(chat_id, lang="en"):
    lang=lang if lang in ("en","hi") else "en"
    rows=[["🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦" if lang=="en" else "🛒 कीज़ खरीदें"],
          ["🔑 𝗠𝗬 𝗞𝗘𝗬𝗦" if lang=="en" else "🔑 मेरी कीज़"],
          ["🏠 𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨" if lang=="en" else "🏠 मुख्य मेनू"]]
    return _key_inline_send(chat_id,_lt(lang,"key_home"),rows,{rows[0][0]:"k_buy",rows[1][0]:"k_my",rows[2][0]:f"k_home:{lang}"})

def _product_menu(chat_id, lang="en"):
    products=[p for p in key_products() if p.get("enabled",True)]
    rows=[]; actions={}
    for p in products:
        label=f"💎 {p.get('emoji','📦')} {p.get('name','Product')}"
        rows.append([label]); actions[label]="k_prod:"+str(p.get("id"))
    back="🔙 𝗕𝗔𝗖𝗞" if lang=="en" else "🔙 वापस"
    rows.append([back]); actions[back]=f"k_home:{lang}"
    text=_lt(lang,"buy_keys")+"\n\n"+(_lt(lang,"select_product") if products else _lt(lang,"no_products"))
    return _key_inline_send(chat_id,text,rows,actions)

def _duration_menu(chat_id,pid,lang="en"):
    products=key_products(); p=_find_product(products,pid)
    if not p:return
    rows=[]; actions={}
    for d in p.get("durations",[]):
        if not d.get("enabled",True) or not _available_keys(d): continue
        label=f"📅 {d.get('name','1 Day')}"
        rows.append([label]); actions[label]=f"k_dur:{pid}:{d.get('id')}"
    back="🔙 𝗕𝗔𝗖𝗞" if lang=="en" else "🔙 वापस"
    rows.append([back]); actions[back]="k_buy"
    return _key_inline_send(chat_id,_key_text("product_text",lang=lang,product=p.get("name","")),rows,actions)

def _duration_details(chat_id,pid,did,lang="en"):
    products=key_products(); p=_find_product(products,pid); d=_find_duration(p,did)
    if not p or not d or not d.get("enabled",True) or not _available_keys(d):
        tg_call("sendMessage",{"chat_id":chat_id,"text":_lt(lang,"duration_unavailable")})
        return
    back="🔙 𝗕𝗔𝗖𝗞" if lang=="en" else "🔙 वापस"
    rows=[["💳 𝗣𝗔𝗬𝗠𝗘𝗡𝗧 𝗨𝗡𝗔𝗩𝗔𝗜𝗟𝗔𝗕𝗟𝗘" if lang=="en" else "💳 भुगतान उपलब्ध नहीं"],[back]]
    return _key_inline_send(chat_id,_key_text("duration_text",lang=lang,duration=d.get("name"),price=d.get("price"))+"\n\n"+_lt(lang,"payment_disabled"),rows,{rows[0][0]:"noop",rows[1][0]:f"k_prod:{pid}"})

def _my_keys(chat_id,user_id):
    lang=_user_lang(user_id)
    active=_active_user_key(user_id)
    if not active:
        rows=[["🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦" if lang=="en" else "🛒 कीज़ खरीदें"],
              ["🏠 𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨" if lang=="en" else "🏠 मुख्य मेनू"]]
        return _key_inline_send(chat_id,_lt(lang,"my_keys_empty"),rows,{rows[0][0]:"k_buy",rows[1][0]:f"k_home:{lang}"})
    active.sort(key=lambda x:float(x[2].get("purchased_at",0)),reverse=True)
    if len(active)==1:
        p,d,k=active[0]; return _send_user_key_detail_menu(chat_id,user_id,p,d,k,lang=lang)
    rows=[]; actions={}
    for p,d,k in active:
        label=f"🔑 {d.get('name','Key')} • {_format_dt(k.get('purchased_at'))}"
        rows.append([label]); actions[label]="k_view:"+str(k.get("id"))
    buy="🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦" if lang=="en" else "🛒 कीज़ खरीदें"
    home="🏠 𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨" if lang=="en" else "🏠 मुख्य मेनू"
    rows += [[buy],[home]]; actions.update({buy:"k_buy",home:f"k_home:{lang}"})
    return _key_inline_send(chat_id,_lt(lang,"my_keys_list"),rows,actions)

def _send_user_key_detail_menu(chat_id,user_id,p,d,k,lang="en"):
    days=_duration_days(d.get("name","")); can_reset=(days is not None and days>=7) or float(d.get("days",0) or 0)>=7
    resets=int(k.get("resets",0)); max_resets=int(get_setting("key_max_resets","3"))
    rows=[]; actions={}
    if can_reset and resets<max_resets:
        label="🔄 𝗥𝗘𝗦𝗘𝗧 𝗗𝗘𝗩𝗜𝗖𝗘" if lang=="en" else "🔄 डिवाइस रीसेट"
        rows.append([label]); actions[label]="k_reset:"+str(k.get("id"))
    label="🗑 𝗗𝗘𝗟𝗘𝗧𝗘 𝗞𝗘𝗬" if lang=="en" else "🗑 की डिलीट करें"
    rows.append([label]); actions[label]="k_delete:"+str(k.get("id"))
    my="🔙 𝗠𝗬 𝗞𝗘𝗬𝗦" if lang=="en" else "🔙 मेरी कीज़"
    buy="🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦" if lang=="en" else "🛒 कीज़ खरीदें"
    rows += [[my],[buy]]; actions.update({my:"k_my",buy:"k_buy"})
    return _key_inline_send(chat_id,_key_detail_text(p,d,k,lang=lang),rows,actions,track=True)

def _setup_send(chat_id):
    items=key_setup()
    if not items:
        return False
    for item in items:
        try:
            if item.get("kind") == "message" and item.get("text") is not None:
                data={"chat_id":chat_id,"text":item["text"]}
                if item.get("entities"): data["entities"]=item["entities"]
                tg_call("sendMessage",data)
            elif item.get("from_chat_id") and item.get("message_id"):
                tg_call("copyMessage",{"chat_id":chat_id,"from_chat_id":item["from_chat_id"],"message_id":item["message_id"]})
        except Exception as exc:
            print(f"SETUP DELIVERY ERROR: {exc}",flush=True)
    return True

def _start_main_menu(chat_id, user_id=None):
    if user_id is None or not _has_user_lang(user_id):
        return _key_lang_menu(chat_id)
    return _key_home(chat_id,_user_lang(user_id))

def _start_buykeys(chat_id, user_id=None):
    if user_id is None or not _has_user_lang(user_id):
        _key_lang_menu(chat_id)
        return
    _key_home(chat_id,_user_lang(user_id))

def _handle_key_callback(cq):
    uid=cq["from"]["id"]; cid=cq["message"]["chat"]["id"]; cdata=cq.get("data","")
    lang=_user_lang(uid)
    if cdata.startswith("klang:"):
        lang=_set_user_lang(uid,cdata.split(":",1)[1])
        _key_home(cid,lang); return True
    if cdata.startswith("k_home:"):
        lang=cdata.split(":",1)[1] if cdata.split(":",1)[1] in ("en","hi") else lang
        _set_user_lang(uid,lang)
        _key_home(cid,lang); return True
    if cdata=="k_buy": _product_menu(cid,lang); return True
    if cdata=="k_my": _my_keys(cid,uid); return True
    if cdata.startswith("k_prod:"):
        _duration_menu(cid,cdata.split(":",1)[1],lang); return True
    if cdata.startswith("k_dur:"):
        _,pid,did=cdata.split(":",2); _duration_details(cid,pid,did,lang); return True
    if cdata=="noop":
        return True
    if cdata.startswith("k_view:"):
        kid=cdata.split(":",1)[1]
        for p,d,k in _active_user_key(uid):
            if str(k.get("id"))==kid: _send_user_key_detail_menu(cid,uid,p,d,k,lang=lang); break
        return True
    if cdata.startswith("k_reset:") or cdata.startswith("k_delete:"):
        action="reset" if cdata.startswith("k_reset:") else "delete"; kid=cdata.split(":",1)[1]
        products=key_products(); target=None
        for p in products:
            for d in p.get("durations",[]):
                for k in d.get("keys",[]):
                    if str(k.get("id"))==kid and str(k.get("assigned_to"))==str(uid): target=(p,d,k); break
                if target: break
            if target: break
        if not target:
            tg_call("sendMessage",{"chat_id":cid,"text":_lt(lang,"key_not_found")}); return True
        p,d,k=target
        reqs=_json_setting("key_requests_v1",[])
        reqs.append({"id":_new_id("R"),"user_id":int(uid),"action":action,"key_id":kid,"product":p.get("name"),"duration":d.get("name"),"status":"PENDING","created_at":time.time()})
        _save_json_setting("key_requests_v1",reqs)
        tg_call("sendMessage",{"chat_id":cid,"text":_lt(lang,"reset_request" if action=="reset" else "delete_request")})
        return True
    return False

def _keys_admin_menu(cid,mid=None):
    products=key_products(); total=sum(len(d.get("keys",[])) for p in products for d in p.get("durations",[]))
    txt=f"🔑 **KEY MANAGEMENT**\n\nProducts: `{len(products)}`\nInventory entries: `{total}`\n\nProducts start empty. Add your own products/durations/keys."
    kb=[[{"text":"➕ Add Product","callback_data":"ka_addp"},{"text":"✏️ Products","callback_data":"ka_products"}],
        [{"text":"➕ Add Key","callback_data":"ka_addk"},{"text":"📨 Requests","callback_data":"ka_requests"}],
        [{"text":"📝 Key Messages","callback_data":"ka_messages"}],
        [{"text":"⬅️ Back","callback_data":"adm_home"}]]
    if mid: _async_delete_message(cid, mid)
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}}))

def _keys_admin_products(cid,mid=None):
    products=key_products(); rows=[]
    for p in products:
        rows.append([{"text":f"{p.get('emoji','📦')} {p.get('name','Product')}","callback_data":"kap:"+str(p.get("id"))}])
    rows.append([{"text":"➕ Add Product","callback_data":"ka_addp"}]); rows.append([{"text":"⬅️ Back","callback_data":"m_keys"}])
    if mid: _async_delete_message(cid, mid)
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":"📦 **PRODUCTS**\n\nNo default products are created.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}))

def _keys_admin_product(cid,pid,mid=None):
    products=key_products(); p=_find_product(products,pid)
    if not p:return
    rows=[[{"text":"➕ Add Duration","callback_data":"kad_add:"+str(pid)}],[{"text":"✏️ Rename / Emoji","callback_data":"kap_edit:"+str(pid)}],[{"text":"🟢/🔴 Enable Product","callback_data":"kap_toggle:"+str(pid)}]]
    for d in p.get("durations",[]):
        rows.append([{"text":f"{d.get('emoji','📅')} {d.get('name')} • ₹{d.get('price')} • stock hidden","callback_data":"kad_view:"+str(pid)+":"+str(d.get('id'))}])
    rows.append([{"text":"🗑 Remove Product","callback_data":"kap_del:"+str(pid)}]); rows.append([{"text":"⬅️ Back","callback_data":"ka_products"}])
    if mid: _async_delete_message(cid, mid)
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":f"📦 **{p.get('name')}**\n\nConfigure durations below.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}))

def _key_admin_request_menu(cid,mid=None):
    reqs=_json_setting("key_requests_v1",[]); pending=[r for r in reqs if r.get("status")=="PENDING"]
    rows=[]
    for r in pending[-20:]: rows.append([{"text":f"{r.get('action','').upper()} • {r.get('user_id')} • {r.get('duration')}","callback_data":"kar:"+str(r.get("id"))}])
    rows.append([{"text":"⬅️ Back","callback_data":"m_keys"}])
    if mid: _async_delete_message(cid, mid)
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":f"📨 **KEY REQUESTS**\n\nPending: `{len(pending)}`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}))

def _keys_admin_callback(cq):
    uid=cq["from"]["id"]; cid=cq["message"]["chat"]["id"]; mid=cq["message"]["message_id"]; c=cq.get("data","")
    if uid not in ADMIN_IDS:return False
    if c in {"m_keys", "ka_products"} or c.startswith(("kap:", "kad_view:")):
        ADMIN_SESSIONS.pop(uid, None)
    if c=="m_keys": _keys_admin_menu(cid,mid); return True
    if c=="ka_products": _keys_admin_products(cid,mid); return True
    if c=="ka_addp": _admin_begin_input(cid,uid,{"step":"K_ADD_PRODUCT"},"Send product name. Example: Premium License"); return True
    if c.startswith("kap:"): _keys_admin_product(cid,c.split(":",1)[1],mid); return True
    if c.startswith("kap_toggle:"):
        pid=c.split(":",1)[1]; ps=key_products(); p=_find_product(ps,pid); p["enabled"]=not p.get("enabled",True); _save_products(ps); _keys_admin_product(cid,pid,mid); return True
    if c.startswith("kap_del:"):
        pid=c.split(":",1)[1]; ps=[p for p in key_products() if str(p.get("id"))!=pid]; _save_products(ps); _keys_admin_products(cid,mid); return True
    if c.startswith("kap_edit:"):
        _admin_begin_input(cid,uid,{"step":"K_EDIT_PRODUCT","pid":c.split(":",1)[1]},"Send: `Product Name | Emoji`"); return True
    if c.startswith("kad_add:"):
        _admin_begin_input(cid,uid,{"step":"K_ADD_DURATION","pid":c.split(":",1)[1]},"Send: `Duration Name | Emoji | Price`\nExample: `1 Day | 📅 | 120`"); return True
    if c.startswith("kad_view:"):
        _,pid,did=c.split(":",2); ps=key_products(); p=_find_product(ps,pid); d=_find_duration(p,did)
        if not d:return True
        stock=len(_available_keys(d)); rows=[[{"text":"➕ Add Keys","callback_data":f"ka_addkeys:{pid}:{did}"},{"text":"✏️ Edit Duration","callback_data":f"kad_edit:{pid}:{did}"}],[{"text":"🗑 Remove Duration","callback_data":f"kad_del:{pid}:{did}"}],[{"text":"⬅️ Back","callback_data":f"kap:{pid}"}]]
        tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"📅 **{d.get('name')}**\n\nPrice: ₹{d.get('price')}\nAvailable: {stock} (admin only)","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}); return True
    if c.startswith("ka_addkeys:"):
        _,pid,did=c.split(":",2); _admin_begin_input(cid,uid,{"step":"K_ADD_KEYS","pid":pid,"did":did},"Send keys one per line. Send `DONE` when finished."); return True
    if c.startswith("kad_edit:"):
        _,pid,did=c.split(":",2); _admin_begin_input(cid,uid,{"step":"K_EDIT_DURATION","pid":pid,"did":did},"Send: `Duration Name | Emoji | Price`"); return True
    if c.startswith("kad_del:"):
        _,pid,did=c.split(":",2); ps=key_products(); p=_find_product(ps,pid); p["durations"]= [d for d in p.get("durations",[]) if str(d.get("id"))!=did]; _save_products(ps); _keys_admin_product(cid,pid,mid); return True
    if c=="ka_addk":
        _admin_begin_input(cid,uid,{"step":"K_ADD_KEYS_GLOBAL"},"Send `Product ID | Duration ID | Key` for each key, one per line. Send `DONE` when finished."); return True
    if c=="ka_requests": _key_admin_request_menu(cid,mid); return True
    if c.startswith("kar:"):
        rid=c.split(":",1)[1]; reqs=_json_setting("key_requests_v1",[]); r=next((x for x in reqs if str(x.get("id"))==rid),None)
        if not r:return True
        kb=[[{"text":"✅ Done","callback_data":"kar_done:"+rid},{"text":"❌ Reject","callback_data":"kar_reject:"+rid}],[{"text":"⬅️ Back","callback_data":"ka_requests"}]]
        tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"📨 **{r.get('action','').upper()} REQUEST**\n\nUser: `{r.get('user_id')}`\nProduct: {r.get('product')}\nDuration: {r.get('duration')}\nKey: `{r.get('key_id')}`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}}); return True
    if c.startswith("kar_"):
        action,rid=c.split(":",1); reqs=_json_setting("key_requests_v1",[]); r=next((x for x in reqs if str(x.get("id"))==rid),None)
        if not r:return True
        if r.get("status") != "PENDING":
            _key_admin_request_menu(cid,mid); return True
        if action=="kar_done":
            ps=key_products(); target=None
            for p in ps:
                for d in p.get("durations",[]):
                    for k in d.get("keys",[]):
                        if str(k.get("id"))==str(r.get("key_id")):
                            target=(p,d,k); break
                    if target: break
                if target: break
            if not target:
                r["status"]="REJECTED"; r["reject_reason"]="key_not_found"; _save_json_setting("key_requests_v1",reqs); tg_call("sendMessage",{"chat_id":int(r.get("user_id")),"text":"❌ Key request could not be completed."}); _key_admin_request_menu(cid,mid); return True
            p,d,k=target
            if r.get("action")=="reset":
                days=_duration_days(d.get("name","")); max_resets=int(get_setting("key_max_resets","3"))
                if days is None or days < 7 or int(k.get("resets",0)) >= max_resets:
                    r["status"]="REJECTED"; r["reject_reason"]="reset_limit"; _save_json_setting("key_requests_v1",reqs); tg_call("sendMessage",{"chat_id":int(r.get("user_id")),"text":"❌ Reset limit reached or reset is unavailable."}); _key_admin_request_menu(cid,mid); return True
                k["hwid"]=None; k["resets"]=int(k.get("resets",0))+1
            else:
                k["status"]="deleted"; k["assigned_to"]=None
            _save_products(ps); r["status"]="DONE"; r["completed_at"]=time.time(); _save_json_setting("key_requests_v1",reqs); tg_call("sendMessage",{"chat_id":int(r.get("user_id")),"text":_key_text("reset_success" if r.get("action")=="reset" else "delete_success")});
        else:
            r["status"]="REJECTED"; r["rejected_at"]=time.time(); _save_json_setting("key_requests_v1",reqs); tg_call("sendMessage",{"chat_id":int(r.get("user_id")),"text":"❌ Request rejected by admin."})
        _key_admin_request_menu(cid,mid); return True
    if c=="ka_messages":
        tg_call("sendMessage",{"chat_id":cid,"text":"Editable key messages are stored as settings. Use `/keymsg <name>` to edit. Names: key_lang, key_home_en, key_home_hi, product_text, duration_text, key_details, no_keys, sold_admin, stock_zero_admin, reset_success, delete_success (English; Hindi uses keymsg_hi_*)"}); return True
    return False

def _keys_admin_message_step(uid,cid,txt,msg):
    sess=ADMIN_SESSIONS.get(uid,{}); st=sess.get("step")
    if st=="K_ADD_PRODUCT":
        p={"id":_new_id("P"),"name":txt.strip(),"emoji":"📦","enabled":True,"durations":[]}; ps=key_products(); ps.append(p); _save_products(ps); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Product added. Now add durations from Admin → Keys."}); _keys_admin_menu(cid); return True
    if st=="K_EDIT_PRODUCT":
        parts=[x.strip() for x in txt.split("|",1)]; ps=key_products(); p=_find_product(ps,sess.get("pid"));
        if p and parts: p["name"]=parts[0]; p["emoji"]=parts[1] if len(parts)>1 and parts[1] else p.get("emoji","📦"); _save_products(ps)
        ADMIN_SESSIONS.pop(uid,None); _keys_admin_menu(cid); return True
    if st in ("K_ADD_DURATION","K_EDIT_DURATION"):
        parts=[x.strip() for x in txt.split("|")]
        try:
            name,emoji,price=parts[0],parts[1],float(parts[2])
            if not name or not emoji or not math.isfinite(price) or price <= 0:
                raise ValueError
        except Exception:
            tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Format: Duration | Emoji | Price\nPrice must be a finite value greater than ₹0."}); return True
        ps=key_products(); p=_find_product(ps,sess.get("pid"));
        if p:
            if st=="K_ADD_DURATION": p.setdefault("durations",[]).append({"id":_new_id("D"),"name":name,"emoji":emoji,"price":price,"enabled":True,"keys":[]})
            else:
                d=_find_duration(p,sess.get("did")); d.update({"name":name,"emoji":emoji,"price":price}) if d else None
            _save_products(ps)
        ADMIN_SESSIONS.pop(uid,None); _keys_admin_product(cid,sess.get("pid")); return True
    if st=="K_ADD_KEYS":
        if txt.strip().upper()=="DONE": ADMIN_SESSIONS.pop(uid,None); _keys_admin_product(cid,sess.get("pid")); return True
        ps=key_products(); p=_find_product(ps,sess.get("pid")); d=_find_duration(p,sess.get("did"));
        if p and d:
            for line in txt.splitlines():
                key=line.strip()
                if key: d.setdefault("keys",[]).append({"id":_new_id("K"),"key":key,"status":"available","assigned_to":None,"resets":0,"hwid":None})
            _save_products(ps); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Keys added. Send more or `DONE`."})
        return True
    if st=="K_ADD_KEYS_GLOBAL":
        if txt.strip().upper()=="DONE": ADMIN_SESSIONS.pop(uid,None); _keys_admin_menu(cid); return True
        ps=key_products()
        for line in txt.splitlines():
            parts=[x.strip() for x in line.split("|",2)]
            if len(parts)!=3: continue
            p=_find_product(ps,parts[0]); d=_find_duration(p,parts[1])
            if p and d:d.setdefault("keys",[]).append({"id":_new_id("K"),"key":parts[2],"status":"available","assigned_to":None,"resets":0,"hwid":None})
        _save_products(ps); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Processed. Send more or `DONE`."}); return True
    return False

def _setup_admin_callback(cq):
    uid=cq["from"]["id"]; cid=cq["message"]["chat"]["id"]; mid=cq["message"]["message_id"]; c=cq.get("data","")
    if uid not in ADMIN_IDS:return False
    if c=="m_setup":
        items=key_setup(); rows=[[{"text":"➕ Add Content","callback_data":"setup_add"}],[{"text":"🗑 Clear Setup","callback_data":"setup_clear"}],[{"text":"⬅️ Back","callback_data":"adm_home"}]]
        tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"⚙️ **GLOBAL /setup CONTENT**\n\nSelected items: `{len(items)}`\n\nForward/send content one by one, then type `DONE`. After Force Join verification, `/setup` sends these global items.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":rows}}); return True
    if c=="setup_add": _admin_begin_input(cid,uid,{"step":"SETUP_ADD"},"Forward a message/media from Telegram. Send multiple items one by one. Type `DONE` when finished."); return True
    if c=="setup_clear": _save_setup([]); tg_call("sendMessage",{"chat_id":cid,"text":"✅ /setup content cleared."}); return True
    return False

def _setup_admin_message(uid,cid,txt,msg):
    sess=ADMIN_SESSIONS.get(uid,{}); st=sess.get("step")
    if st!="SETUP_ADD":return False
    if txt.strip().upper()=="DONE": ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Global /setup content saved."}); return True
    origin=msg.get("forward_origin") or {}
    fwd_chat=msg.get("forward_from_chat") or origin.get("chat") or {}
    fwd_mid=msg.get("forward_from_message_id") or origin.get("message_id")
    items=key_setup()
    if fwd_mid and fwd_chat.get("id"):
        items.append({"kind":"copy","from_chat_id":fwd_chat.get("id"),"message_id":fwd_mid})
    elif txt:
        items.append({"kind":"message","text":txt,"entities":msg.get("entities") or []})
    else:
        tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Send or forward a valid Telegram message/media."}); return True
    _save_setup(items); tg_call("sendMessage",{"chat_id":cid,"text":f"✅ Added. Total setup items: {len(items)}\nSend another or `DONE`."}); return True

def _key_command_wrapper(data):
    msg=data.get("message",{}); txt=msg.get("text","") or ""; cid=msg.get("chat",{}).get("id"); uid=msg.get("from",{}).get("id")
    cmd=txt.strip().split(maxsplit=1)[0].split("@",1)[0].lower() if txt.strip() else ""
    if cmd=="/buykeys":
        try:
            _start_buykeys(cid,uid)
        except Exception as exc:
            print(f"KEY COMMAND ERROR user={uid} chat={cid}: {exc}", flush=True)
            traceback.print_exc()
            tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Buy Keys is temporarily unavailable. Please try again."})
        return True
    if cmd=="/mykeys":
        try:
            _my_keys(cid,uid)
        except Exception as exc:
            print(f"MYKEYS COMMAND ERROR user={uid} chat={cid}: {exc}", flush=True)
            traceback.print_exc()
            tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ My Keys is temporarily unavailable. Please try again."})
        return True
    if cmd=="/setup":
        if not key_setup():
            tg_call("sendMessage",{"chat_id":cid,"text":"⚙️ /setup content is not configured yet."}); return True
        verification_on = get_setting("verification_enabled", "true").lower() == "true"
        if verification_on:
            all_clear = send_fj(cid, uid, "__setup__", msg.get("from",{}).get("first_name","User"))
            if all_clear: _setup_send(cid)
        else:
            _setup_send(cid)
        return True
    return False

def process_callback_query(cq):
    c=cq.get("data","")
    uid=int(cq.get("from",{}).get("id",0) or 0); cid=cq.get("message",{}).get("chat",{}).get("id"); mid=cq.get("message",{}).get("message_id")
    if uid in ADMIN_IDS and c=="m_ui": show_ui_studio(cid,mid); return "OK",200
    if uid in ADMIN_IDS and c.startswith("ui_pick:"): show_ui_picker(cid,mid,c.split(":",1)[1]); return "OK",200
    if uid in ADMIN_IDS and c == "ui_buttons": show_button_manager(cid,mid); return "OK",200
    if uid in ADMIN_IDS and c.startswith("uib:"): show_button_editor(cid,mid,c.split(":",1)[1]); return "OK",200
    if uid in ADMIN_IDS and c.startswith("uibset:"):
        parts=c.split(":",3)
        if len(parts)==4:
            _,token,field,value=parts
            if field=="style" and value in {"primary","success","danger"}: set_setting(f"ui_button_{token}_style",value)
            elif field=="visible" and value in {"true","false"}: set_setting(f"ui_button_{token}_visible",value)
            elif field=="reset":
                set_setting(f"ui_button_{token}_style","")
                set_setting(f"ui_button_{token}_visible","true")
                set_setting(f"ui_button_{token}_text","")
            show_button_editor(cid,mid,token)
        return "OK",200
    if uid in ADMIN_IDS and c in {"adm_home", "m_files", "m_fj", "m_bc", "m_users", "m_sett", "m_keys", "m_setup", "ui_buttons", "m_ui"}:
        ADMIN_SESSIONS.pop(uid, None)

    if uid in ADMIN_IDS and c.startswith("uibedit:"):
        parts=c.split(":",2)
        if len(parts)==3 and parts[2]=="text":
            ADMIN_SESSIONS[uid]={"step":"UI_BUTTON_TEXT","button_token":parts[1]}
            _admin_clear_menu(cid)
            tg_call("sendMessage",{"chat_id":cid,"text":"✏️ Send the new button label/emoji. Send `-` to restore the original label."})
        return "OK",200
    if uid in ADMIN_IDS and c.startswith("ui_set:"):
        _,role,value=c.split(":",2)
        if role in {"admin","buy","back"} and value in {"primary","success","danger"}: set_setting(f"ui_btn_{role}",value)
        show_ui_studio(cid,mid); return "OK",200
    if uid in ADMIN_IDS and c=="hide_admin":
        ADMIN_SESSIONS.pop(uid,None); _admin_clear_menu(cid); return "OK",200
    if c.startswith(("klang:", "k_home:", "k_buy", "k_my", "k_prod:", "k_dur:", "k_order:", "k_view:", "k_reset:", "k_delete:")):
        if _handle_key_callback(cq):
            return "OK", 200
    if c.startswith(("m_keys", "ka_", "kap:", "kap_", "kad_", "kar", "m_setup", "setup_")):
        if _keys_admin_callback(cq) or _setup_admin_callback(cq):
            return "OK", 200
    return _original_process_callback_query(cq)

def process_message_update(data):
    msg=data.get("message",{}); uid=msg.get("from",{}).get("id"); cid=msg.get("chat",{}).get("id"); txt=msg.get("text","") or ""
    print(f"MESSAGE PROCESSING user={uid} text={txt[:80]!r}", flush=True)
    if uid:
        USER_PROFILE_CACHE[int(uid)] = _profile_from_message(msg)
        try:
            _PROFILE_EXECUTOR.submit(_safe_run, _remember_user_profile, msg)
        except Exception as exc:
            print(f"PROFILE QUEUE ERROR user={uid}: {exc}", flush=True)
    cmd=txt.strip().split(maxsplit=1)[0].split("@",1)[0].lower() if txt.strip() else ""
    if uid in ADMIN_IDS and cmd in {"/username", "/id"}:
        parts = txt.strip().split(maxsplit=1)
        arg = parts[1].strip() if len(parts) > 1 else ""
        reply_msg = None
        if msg.get("reply_to_message"):
            reply_msg = msg.get("reply_to_message")
        _admin_user_lookup(cid, uid, cmd, arg, reply_msg)
        return "OK", 200

    contact = msg.get("contact") or {}
    if contact and contact.get("user_id") and int(contact.get("user_id")) == int(uid):
        _save_user_phone(uid, contact.get("phone_number"))
        _remember_user_profile(msg)
        tg_call("sendMessage", {"chat_id": cid, "text": "✅ Contact saved. Your mobile number is now available to the bot for admin lookup."})
        return "OK", 200
    
    sess = ADMIN_SESSIONS.get(uid) or {}
    if uid in ADMIN_IDS and sess.get("step")=="UI_BUTTON_TEXT":
        token=sess.get("button_token")
        value=(txt or "").strip()
        if token:
            set_setting(f"ui_button_{token}_text", "" if value=="-" else value[:64])
        ADMIN_SESSIONS.pop(uid,None)
        show_button_manager(cid)
        return "OK", 200

    if _key_command_wrapper(data):
        return "OK",200
    result = _original_process_message_update(data)
    if uid not in ADMIN_IDS:
        _forward_invalid_to_admins(msg, reason="Invalid / unrecognized message")
    return result

def warm_caches():
    try:
        _load_settings_cache()
        _ensure_key_defaults()
        get_channels()
    except Exception:
        pass

def _safe_run(fn, *args):
    try:
        return fn(*args)
    except Exception as exc:
        print(f"UPDATE HANDLER ERROR {getattr(fn, '__name__', 'handler')}: {exc}", flush=True)
        traceback.print_exc()
        return None

def _save_join_request(user_id, chat_id):
    if not supabase:
        return
    try:
        payload = {"user_id": int(user_id), "chat_id": str(chat_id), "status": "pending"}
        try:
            supabase.table("join_requests").upsert(payload).execute()
            return
        except Exception as first_exc:
            pass
        for chat_value in (str(chat_id), int(chat_id)):
            try:
                supabase.table("join_requests").upsert({
                    "user_id": int(user_id), "chat_id": chat_value
                }).execute()
                return
            except Exception:
                continue
    except Exception as exc:
        print(f"JOIN REQUEST DB ERROR user={user_id} chat={chat_id}: {exc}", flush=True)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200

    if "chat_join_request" in data:
        j_req = data["chat_join_request"]
        try:
            user_id = int(j_req["from"]["id"])
            chat_id_raw = j_req["chat"]["id"]
            chat_id = str(chat_id_raw)
            PENDING_JOIN_REQUESTS[(user_id, chat_id)] = time.time()
            if supabase:
                _UPDATE_EXECUTOR.submit(_save_join_request, user_id, chat_id)
        except Exception as exc:
            pass
        return "OK", 200

    if "callback_query" in data:
        cq = data["callback_query"]
        uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")
        _ACK_EXECUTOR.submit(_safe_run, tg_call, "answerCallbackQuery", {"callback_query_id": cq["id"]})
        if cdata.startswith("v:"):
            code = cdata.split(":", 1)[1]
            _VERIFY_EXECUTOR.submit(_safe_run, run_verify_process, cid, uid, code, mid)
        else:
            _COMMAND_EXECUTOR.submit(_safe_run, process_callback_query, cq)
        return "OK", 200

    if "message" in data:
        msg = data.get("message", {})
        uid = msg.get("from", {}).get("id")
        txt = msg.get("text", "") or ""
        cmd = txt.strip().split(maxsplit=1)[0].split("@", 1)[0].lower() if txt.strip() else ""
        critical = cmd in {"/start", "/admin", "/buykeys", "/mykeys", "/setup", "/username", "/id"}
        try:
            pool = _COMMAND_EXECUTOR if critical or (uid in ADMIN_IDS) else _PUBLIC_EXECUTOR
            pool.submit(_safe_run, process_message_update, data)
        except Exception as exc:
            _safe_run(process_message_update, data)
        return "OK", 200

    return "OK", 200

def _profile_from_message(msg):
    u = msg.get("from") or {}
    return {
        "telegram_user_id": u.get("id"),
        "username": u.get("username") or "",
        "first_name": u.get("first_name") or "",
        "last_name": u.get("last_name") or "",
        "language_code": u.get("language_code") or "",
        "is_premium": bool(u.get("is_premium", False)),
        "added_to_attachment_menu": bool(u.get("added_to_attachment_menu", False)),
        "chat_id": (msg.get("chat") or {}).get("id"),
        "chat_type": (msg.get("chat") or {}).get("type", ""),
    }

def _remember_user_profile(msg):
    prof = _profile_from_message(msg)
    uid = prof.get("telegram_user_id")
    if not uid:
        return
    USER_PROFILE_CACHE[int(uid)] = dict(prof)
    if supabase:
        try:
            base = {"telegram_user_id": int(uid), "username": prof["username"]}
            try:
                supabase.table("users").upsert({**base, "first_name": prof["first_name"]}).execute()
            except Exception:
                supabase.table("users").upsert(base).execute()
        except Exception as exc:
            print(f"USER TRACK ERROR user={uid}: {exc}", flush=True)

def _track_user(uid, username, first_name, last_name="", language_code="", is_premium=False, chat_id=None):
    if not uid:
        return
    prof = {
        "telegram_user_id": int(uid),
        "username": username or "",
        "first_name": first_name or "User",
        "last_name": last_name or "",
        "language_code": language_code or "",
        "is_premium": bool(is_premium),
        "chat_id": chat_id,
    }
    USER_PROFILE_CACHE[int(uid)] = dict(prof)
    if not supabase:
        return
    try:
        base = {"telegram_user_id": int(uid), "username": username or ""}
        try:
            supabase.table("users").upsert({**base, "first_name": first_name or "User"}).execute()
        except Exception:
            supabase.table("users").upsert(base).execute()
    except Exception as exc:
        print(f"USER TRACK ERROR user={uid}: {exc}", flush=True)

def _get_stored_phone(uid):
    try:
        val = get_setting(f"user_phone_{int(uid)}", "")
        return str(val or "")
    except Exception:
        return ""

def _save_user_phone(uid, phone):
    phone = str(phone or "").strip()
    if not uid or not phone:
        return
    try:
        set_setting(f"user_phone_{int(uid)}", phone)
    except Exception as exc:
        print(f"PHONE SAVE ERROR user={uid}: {exc}", flush=True)

def _safe_user_text(v):
    return str(v or "—").replace("<", "&lt;").replace(">", "&gt;")

def _lookup_user_by_username(username):
    username = str(username or "").strip().lstrip("@").lower()
    if not username:
        return None
    for uid, prof in list(USER_PROFILE_CACHE.items()):
        if str(prof.get("username", "")).lower() == username:
            return int(uid), dict(prof)
    if not supabase:
        return None
    try:
        rows = supabase.table("users").select("telegram_user_id,username,first_name").execute().data or []
        for row in rows:
            if str(row.get("username", "")).lstrip("@").lower() == username:
                uid = int(row.get("telegram_user_id"))
                prof = dict(USER_PROFILE_CACHE.get(uid, {}))
                prof.update(row)
                USER_PROFILE_CACHE[uid] = prof
                return uid, prof
    except Exception as exc:
        pass
    return None

def _user_details_text(uid, seed=None, source="lookup"):
    uid = int(uid)
    prof = dict(USER_PROFILE_CACHE.get(uid, {}))
    if seed:
        prof.update({k:v for k,v in seed.items() if v not in (None, "")})

    chat = {}
    try:
        r = tg_call("getChat", {"chat_id": uid})
        if r.get("ok"):
            result = r.get("result")
            chat = result if isinstance(result, dict) else {}
            for k in ("id","type","username","first_name","last_name","bio","has_private_forwards"):
                if chat.get(k) not in (None, ""):
                    if k == "id": prof["telegram_user_id"] = chat[k]
                    else: prof[k] = chat[k]
    except Exception:
        pass

    photo_count = "—"
    try:
        r = tg_call("getUserProfilePhotos", {"user_id": uid, "offset": 0, "limit": 1})
        if r.get("ok"):
            photo_count = (r.get("result") or {}).get("total_count", 0)
    except Exception:
        pass

    username = prof.get("username") or ""
    phone = _get_stored_phone(uid)
    name = " ".join(x for x in (prof.get("first_name"), prof.get("last_name")) if x).strip() or "—"
    mention = f'<a href="tg://user?id={uid}">{_safe_user_text(name)}</a>'
    lines = [
        "👤 <b>USER DETAILS</b>",
        "",
        f"🆔 <b>Telegram ID:</b> <code>{uid}</code>",
        f"👤 <b>Name:</b> {mention}",
        f"🔗 <b>Username:</b> @{_safe_user_text(username)}" if username else "🔗 <b>Username:</b> —",
        f"📞 <b>Mobile:</b> <code>{_safe_user_text(phone)}</code>" if phone else "📞 <b>Mobile:</b> Not shared with bot",
        f"🌐 <b>Language:</b> {_safe_user_text(prof.get('language_code'))}",
        f"⭐ <b>Premium:</b> {'Yes' if prof.get('is_premium') else 'No / unknown'}",
        f"🖼 <b>Profile photos:</b> {_safe_user_text(photo_count)}",
        f"💬 <b>Chat ID:</b> <code>{_safe_user_text(prof.get('chat_id') or uid)}</code>",
        f"📋 <b>Chat type:</b> {_safe_user_text(prof.get('chat_type') or chat.get('type'))}",
        f"📝 <b>Bio:</b> {_safe_user_text(prof.get('bio') or chat.get('bio'))}",
        f"🔒 <b>Private forwards:</b> {_safe_user_text(prof.get('has_private_forwards') if 'has_private_forwards' in prof else chat.get('has_private_forwards'))}",
        f"📌 <b>Lookup:</b> {source}",
        "",
        "ℹ️ Phone number is shown only when the user has voluntarily shared a contact with this bot.",
    ]
    return "\n".join(lines)

def _admin_user_lookup(cid, uid, command, arg, reply_msg=None):
    if uid not in ADMIN_IDS:
        return True
    target_id = None
    seed = None
    source = command
    if reply_msg:
        target = reply_msg.get("from") or {}
        target_id = target.get("id")
        seed = _profile_from_message(reply_msg)
        source = "reply"
    elif command == "/id":
        try:
            target_id = int(str(arg or "").strip())
        except Exception:
            target_id = None
    elif command == "/username":
        found = _lookup_user_by_username(arg)
        if found:
            target_id, seed = found
        else:
            tg_call("sendMessage", {"chat_id": cid, "text": "❌ Username not found in users known to this bot. Telegram Bot API does not provide arbitrary-user username → ID lookup."})
            return True
    if not target_id:
        tg_call("sendMessage", {"chat_id": cid, "text": "Usage: /username @username  or  /id <telegram_id>\nYou can also reply to a user\'s message with /id."})
        return True
    text = _user_details_text(target_id, seed=seed, source=source)
    tg_call("sendMessage", {"chat_id": cid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
    return True

def _forward_invalid_to_admins(msg, reason="Invalid / unrecognized message"):
    if not ADMIN_IDS:
        return
    uid = (msg.get("from") or {}).get("id")
    if not uid:
        return
    prof = _profile_from_message(msg)
    details = _user_details_text(uid, seed=prof, source=reason)
    text = f"🚨 <b>UNRECOGNIZED MESSAGE</b>\n<b>Reason:</b> {_safe_user_text(reason)}\n\n{details}"
    for aid in ADMIN_IDS:
        try:
            r = tg_call("forwardMessage", {"chat_id": aid, "from_chat_id": msg.get("chat",{}).get("id"), "message_id": msg.get("message_id")})
            if not r.get("ok"):
                tg_call("copyMessage", {"chat_id": aid, "from_chat_id": msg.get("chat",{}).get("id"), "message_id": msg.get("message_id")})
        except Exception:
            pass
        tg_call("sendMessage", {"chat_id": aid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})

def _original_process_message_update(data):
    msg = data["message"]
    cid, uid, txt = msg["chat"]["id"], msg["from"]["id"], msg.get("text", "")
    uname = msg["from"].get("first_name", "User")
    mid = msg["message_id"]

    command = txt.strip().split(maxsplit=1)[0].split("@", 1)[0].lower() if txt.strip() else ""
    if command == "/admin" and uid in ADMIN_IDS:
        ADMIN_SESSIONS.pop(uid, None)
        _admin_clear_menu(cid)
        show_admin_home(cid)
        return "OK", 200

    if uid in ADMIN_IDS and uid in ADMIN_SESSIONS:
        sess = ADMIN_SESSIONS[uid]
        st = sess.get("step")

        # === FIX YAHI PE HUA HAI ===
        # Pehle "K_" aur "SETUP_" ko verify karke block mein bhejne ka logic nahi tha.
        if st and str(st).startswith("K_"):
            if _keys_admin_message_step(uid, cid, txt, msg):
                return "OK", 200
        if st and str(st).startswith("SETUP_"):
            if _setup_admin_message(uid, cid, txt, msg):
                return "OK", 200
        # ===========================

        if st == "SET_VERIFY_RETRY":
            set_setting("verify_retry_text", txt); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Retry message updated."}); show_settings_menu(cid); return "OK",200
        elif st == "F_TITLE":
            sess["t"], sess["step"] = txt.strip(), "F_CODE"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter short code (e.g. `1`, `vip`):"})
        elif st == "F_CODE":
            code = txt.strip()
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", code):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Use only letters, numbers, `_` or `-` (1-40 chars):"})
                return "OK", 200
            if supabase:
                try:
                    exists = supabase.table("apps").select("deep_link_code").eq("deep_link_code", code).execute().data or []
                    if exists:
                        tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ That short code already exists. Send another:"})
                        return "OK", 200
                except Exception as exc:
                    tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Database check failed. Please try again."})
                    return "OK", 200
            sess["c"], sess["step"] = code, "F_FILE"
            tg_call("sendMessage", {"chat_id": cid, "text": "Now forward the file post directly from Storage Channel:"})
        elif st == "F_FILE":
            fwd_id = msg.get("forward_from_message_id")
            fwd_chat = msg.get("forward_from_chat", {})
            if not fwd_id:
                origin = msg.get("forward_origin") or {}
                fwd_id = origin.get("message_id")
                fwd_chat = origin.get("chat") or fwd_chat
            if not fwd_id:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please forward directly from Storage Channel."})
                return "OK", 200
            if fwd_chat and fwd_chat.get("id"):
                set_setting("storage_channel_id", str(fwd_chat["id"]))
            sess["fid"] = fwd_id
            kb = [
                [{"text": "👤 Hide Sender", "callback_data": "fmode_copy"}],
                [{"text": "🏷 Show Sender (Premium Emojis)", "callback_data": "fmode_forward"}]
            ]
            tg_call("sendMessage", {"chat_id": cid, "text": "Choose delivery mode:", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif st == "F_EDIT_TITLE":
            if supabase:
                supabase.table("apps").update({"title": txt.strip()}).eq("deep_link_code", sess["code"]).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid)

        elif st == "F_EDIT_CODE":
            new_code = txt.strip()
            if not new_code or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", new_code):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Use only letters, numbers, `_` or `-` (1-40 chars):"})
                return "OK", 200
            if supabase:
                try:
                    existing = (supabase.table("apps").select("deep_link_code").eq("deep_link_code", new_code).execute().data or [])
                    if existing and new_code != sess["old_code"]:
                        tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ That short code is already in use. Send another:"})
                        return "OK", 200
                    supabase.table("apps").update({"deep_link_code": new_code}).eq("deep_link_code", sess["old_code"]).execute()
                    tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                except Exception as exc:
                    tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Update failed. Please try again."})
                    return "OK", 200
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid)

        elif st == "F_EDIT_FILE":
            fwd_id = msg.get("forward_from_message_id")
            fwd_chat = msg.get("forward_from_chat", {})
            if not fwd_id:
                origin = msg.get("forward_origin") or {}
                fwd_id = origin.get("message_id")
                chat_obj = origin.get("chat") or {}
                if chat_obj:
                    fwd_chat = chat_obj
            if not fwd_id:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please forward directly from Storage Channel."})
                return "OK", 200
            storage_id = get_setting("storage_channel_id", DEFAULT_STORAGE)
            if fwd_chat and fwd_chat.get("id"):
                storage_id = str(fwd_chat["id"])
            old_row = None
            if supabase:
                try:
                    rows = supabase.table("apps").select("storage_message_id").eq("deep_link_code", sess["code"]).execute().data or []
                    if rows:
                        old_row = rows[0].get("storage_message_id")
                    supabase.table("apps").update({"storage_message_id": fwd_id}).eq("deep_link_code", sess["code"]).execute()
                    set_setting("storage_channel_id", storage_id)
                    if old_row and str(old_row) != str(fwd_id):
                        tg_call("deleteMessage", {"chat_id": storage_id, "message_id": int(old_row)})
                    tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                except Exception as exc:
                    tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Update failed. Old file remains active."})
                    return "OK", 200
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid)

        elif st == "SET_FJ_CAPTION":
            set_setting("force_join_text", txt)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_fj_menu(cid)

        elif st == "SET_POSTER":
            fwd_id = msg.get("forward_from_message_id")
            fwd_chat = msg.get("forward_from_chat", {})
            if not fwd_id:
                origin = msg.get("forward_origin") or {}
                fwd_id = origin.get("message_id")
                fwd_chat = origin.get("chat") or fwd_chat
            if fwd_id:
                if fwd_chat and fwd_chat.get("id"):
                    set_setting("storage_channel_id", str(fwd_chat["id"]))
                set_setting("poster_storage_id", str(fwd_id))
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_fj_menu(cid)
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please forward directly from Storage Channel."})

        elif st == "FJ_ADD_TITLE":
            sess["title"], sess["step"] = txt.strip(), "FJ_ADD_URL"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter channel join invite link:"})
        elif st == "FJ_ADD_URL":
            url = txt.strip()
            if not re.match(r"^https?://\S+$", url):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please send a valid http/https invite link:"})
                return "OK", 200
            sess["url"], sess["step"] = url, "FJ_ADD_ID"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter channel Chat ID (e.g. `-100...`):"})
        elif st == "FJ_ADD_ID":
            try:
                ch_id = int(txt.strip())
                chs = get_channels()
                next_slot = max([c.get("slot", 0) for c in chs] + [0]) + 1
                if supabase:
                    supabase.table("force_join_channels").insert({
                        "slot": next_slot,
                        "button_text": sess["title"],
                        "join_url": sess["url"],
                        "chat_id": ch_id,
                        "active": True
                    }).execute()
                    invalidate_channels_cache()
                    tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                else:
                    tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
                ADMIN_SESSIONS.pop(uid, None)
                show_fj_menu(cid)
            except Exception:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Invalid ID format. Send valid integer:"})

        elif st == "FJ_E_TITLE":
            sess["title"], sess["step"] = txt.strip(), "FJ_E_URL"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter updated channel invite link:"})
        elif st == "FJ_E_URL":
            url = txt.strip()
            if not re.match(r"^https?://\S+$", url):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please send a valid http/https invite link:"})
                return "OK", 200
            sess["url"], sess["step"] = url, "FJ_E_ID"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter updated channel Chat ID:"})
        elif st == "FJ_E_ID":
            try:
                ch_id = int(txt.strip())
                if supabase:
                    supabase.table("force_join_channels").update({
                        "button_text": sess["title"],
                        "join_url": sess["url"],
                        "chat_id": ch_id
                    }).eq("slot", sess["slot"]).execute()
                    invalidate_channels_cache()
                    tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                else:
                    tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
                ADMIN_SESSIONS.pop(uid, None)
                show_fj_menu(cid)
            except Exception:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Invalid ID format. Send valid integer:"})

        elif st == "WAIT_BC_CONTENT":
            sess["bc_payload"] = {"from_chat": cid, "mid": mid}
            kb = [[{"text": "✅ Yes, Send Broadcast", "callback_data": "send_confirmed_bc"}], [{"text": "❌ Cancel", "callback_data": "adm_home"}]]
            tg_call("sendMessage", {"chat_id": cid, "text": "Broadcast preview recorded. Confirm delivery?", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif st == "SET_W":
            set_setting("welcome_text", txt)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif st == "SET_E":
            set_setting("error_feedback_text", txt)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif st == "SET_WARN":
            set_setting("auto_delete_warn_text", txt)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success!"})
            show_settings_menu(cid)
        elif st == "SET_CUSTOM_TIMER":
            try:
                s_val = int(txt.strip())
                set_setting("auto_delete_time", str(s_val))
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
            except Exception:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please send valid integer in seconds:"})
        elif st == "SET_UP_TITLE":
            if txt.strip().lower() == "none":
                set_setting("update_btn_text", "none")
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
            else:
                sess["up_title"], sess["step"] = txt.strip(), "SET_UP_URL"
                cur = get_setting("update_btn_url", DEFAULT_UP_URL)
                tg_call("sendMessage", {"chat_id": cid, "text": f"🔗 CURRENT UPDATE BUTTON URL:\n\n{cur}\n\n✏️ Send the new URL:"})
        elif st == "SET_UP_URL":
            set_setting("update_btn_text", sess["up_title"])
            set_setting("update_btn_url", txt.strip())
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif st == "SET_NOTICE_URL":
            url = txt.strip()
            if not re.match(r"^https?://\S+$", url):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please send a valid http/https link:"})
            else:
                set_setting("notice_btn_url", url)
                set_setting("notice_btn_text", DEFAULT_NOTICE_BTN_TEXT)
                set_setting("notice_btn_enabled", "true")
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success!"})
                show_settings_menu(cid)
        elif st == "SET_ANIM_SPEED":
            try:
                speed = float(txt.strip())
                if speed < 0.10 or speed > 5.00:
                    raise ValueError
                set_setting("anim_speed", f"{speed:.2f}")
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
            except Exception:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Enter a valid speed between 0.10 and 5.00 seconds:"})
        elif st == "SET_ANIM_TEXT":
            set_setting("anim_text", txt.strip())
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)

        return "OK", 200

    if txt.startswith("/start"):
        code = ""
        try:
            parts = txt.strip().split(maxsplit=1)
            command = parts[0].split("@", 1)[0].lower() if parts else ""
            if command != "/start":
                return "OK", 200
            code = parts[1].strip() if len(parts) > 1 else ""

            if code:
                verification_on = get_setting("verification_enabled", "true").lower() == "true"
                if not verification_on:
                    deliver_file(cid, code)
                else:
                    all_clear = send_fj(cid, uid, code, uname)
                    if all_clear:
                        deliver_file(cid, code)
            else:
                result = _start_main_menu(cid,uid)
                if not result.get("ok"):
                    welcome = get_setting("welcome_text", DEFAULT_WELCOME) or DEFAULT_WELCOME
                    tg_call("sendMessage", {"chat_id": cid, "text": welcome})
        except Exception as exc:
            print(f"PUBLIC START ERROR user={uid} code={code!r}: {exc}", flush=True)
            traceback.print_exc()
            error_text = get_setting("error_feedback_text", DEFAULT_ERROR) or DEFAULT_ERROR
            result = tg_call("sendMessage", {
                "chat_id": cid,
                "text": error_text,
                "parse_mode": "Markdown"
            })
            if not result.get("ok"):
                tg_call("sendMessage", {"chat_id": cid, "text": error_text})

        if supabase and uid:
            try:
                _PROFILE_EXECUTOR.submit(_safe_run, _track_user, uid, msg["from"].get("username", ""), uname, msg["from"].get("last_name", ""), msg["from"].get("language_code", ""), msg["from"].get("is_premium", False), cid)
            except Exception:
                pass
        return "OK", 200

    return "OK", 200

def register_webhook():
    if not BOT_TOKEN:
        print("TELEGRAM WEBHOOK NOT REGISTERED: BOT_TOKEN missing", flush=True)
        return
    raw = (os.environ.get("TELEGRAM_WEBHOOK_URL") or os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").strip().rstrip("/")
    for suffix in ("/webhook",):
        if raw.endswith(suffix):
            raw = raw[:-len(suffix)].rstrip("/")
            break
    if not raw:
        print("TELEGRAM WEBHOOK NOT REGISTERED: public URL missing", flush=True)
        return
    webhook_url = raw + "/webhook"
    result = tg_call("setWebhook", {
        "url": webhook_url,
        "allowed_updates": ["message", "callback_query", "chat_join_request"],
        "drop_pending_updates": False
    })
    print(f"TELEGRAM WEBHOOK SET url={webhook_url} ok={result.get('ok')}", flush=True)
    info = tg_call("getWebhookInfo", {})
    if info.get("ok"):
        wi = info.get("result", {})
        print(f"TELEGRAM WEBHOOK INFO url={wi.get('url')} pending={wi.get('pending_update_count')} last_error={wi.get('last_error_message')!r}", flush=True)


threading.Thread(target=warm_caches, daemon=True).start()

if BOT_TOKEN and (os.environ.get("TELEGRAM_WEBHOOK_URL") or os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL")):
    threading.Thread(target=register_webhook, daemon=True).start()

if __name__ == "__main__":
    register_webhook()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

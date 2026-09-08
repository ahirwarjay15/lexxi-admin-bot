import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor
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
PENDING_JOIN_REQUESTS = {}  # (user_id, normalized_chat_id) -> timestamp
VERIFICATION_MESSAGES = {}  # (chat_id, user_id, code) -> {base, failed, retry, anim}
VERIFICATION_LOCKS = {}

# Reuse HTTP connections and cache read-heavy config so Telegram/Supabase latency
# does not repeat on every message. Writes update the cache immediately.
from requests.adapters import HTTPAdapter
_TG_SESSION = requests.Session()
_TG_SESSION.mount("https://", HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0))
_SETTINGS_CACHE = {}
_SETTINGS_CACHE_READY = False
_SETTINGS_CACHE_LOCK = threading.Lock()
_CHANNELS_CACHE = None
_CHANNELS_CACHE_READY = False
_CHANNELS_CACHE_LOCK = threading.Lock()

# System Defaults
DEFAULT_WELCOME = ">> LOOKING FOR HACKS, ID'S OR ANYTHING ELSE?\nMESSAGE [@lexxiowner](https://t.me/lexxiowner) — WE MIGHT HAVE IT. ⚡"
DEFAULT_ERROR = "⚠️ [INVALID_FILE]\nFile not found or expired.\nContact @lexxiowner"
DEFAULT_FJ = ">> HEY {name} ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T JOINED TO\nOUR CHANNELS YET,"
DEFAULT_STORAGE = "-1004430375220"
DEFAULT_WARN = "⚠️ **IMPORTANT NOTICE:**\n\n_All files will be deleted in {time}.\nPlease forward/save this to your Saved Messages!_"
DEFAULT_UP_BTN = "📟 Update Channel"
DEFAULT_UP_URL = "https://t.me/lexxiowner"
DEFAULT_ANIM_TEXT = "Verifying"
DEFAULT_VERIFY_FAILED = "❌ **VERIFICATION FAILED**\n\nYou have not joined/requested all required channels yet."
DEFAULT_VERIFY_RETRY = "🔄 **Please complete the remaining channels and verify again.**"
DEFAULT_NOTICE_BTN_TEXT = "JOIN NOW"
DEFAULT_NOTICE_BTN_URL = DEFAULT_UP_URL

MONO_TRANS = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    "𝙰𝙱𝙲𝙳𝙴𝙵𝙶𝙷𝙸𝙹𝙺𝙻𝙼𝙽𝙾𝙿𝚀𝚁𝚂𝚃𝚄𝚅𝚆𝚇𝚈𝚉𝚊𝚋𝚌𝚍𝚎𝚏𝚐𝚑𝚒𝚓𝚔𝚕𝚖𝚗𝚘𝚙𝚚𝚛𝚜𝚝𝚞𝚟𝚠𝚡𝚢𝚣"
)

def mono(text):
    return str(text).translate(MONO_TRANS)

def mono_admin_kb(keyboard):
    """Apply monospace only to Admin Panel inline-button labels."""
    out = []
    for row in keyboard or []:
        new_row = []
        for btn in row or []:
            b = dict(btn)
            if "text" in b:
                b["text"] = mono(b["text"])
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
        r = _TG_SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
            json=data, timeout=(2.5, 8)
        ).json()
        if not r.get("ok") and "parse_mode" in data:
            retry_data = dict(data)
            retry_data.pop("parse_mode", None)
            return _TG_SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                json=retry_data, timeout=(2.5, 8)
            ).json()
        return r
    except Exception:
        return {}

def _load_settings_cache():
    global _SETTINGS_CACHE_READY
    if _SETTINGS_CACHE_READY or not supabase:
        return
    with _SETTINGS_CACHE_LOCK:
        if _SETTINGS_CACHE_READY or not supabase:
            return
        try:
            rows = supabase.table("settings").select("key,value").execute().data or []
            _SETTINGS_CACHE.update({str(r.get("key")): r.get("value", "") for r in rows if r.get("key") is not None})
        except Exception:
            pass
        _SETTINGS_CACHE_READY = True


def get_setting(key, default=""):
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
        except Exception:
            _CHANNELS_CACHE = []
        _CHANNELS_CACHE_READY = True
        return list(_CHANNELS_CACHE)


def get_unjoined(user_id):
    """Return active channels still not joined/requested by this exact user.

    A pending Join Request from this same user for the same channel counts as
    verified immediately, even while Telegram still reports the user as left.
    Membership API checks run in parallel so 4+ force-join channels do not add
    their network latency one after another.
    """
    channels = get_channels()
    uid = int(user_id)

    active_channels = [c for c in channels if c.get("active") and c.get("chat_id") is not None]
    if not active_channels:
        return []

    # Read all pending requests for THIS user once, then normalize chat IDs so
    # TEXT/BIGINT Supabase schemas cannot cause a false Verify failure.
    pending_chat_ids = {str(chat) for (pending_uid, chat), ts in PENDING_JOIN_REQUESTS.items()
                        if pending_uid == uid}
    if supabase and get_setting("verification_method", "both").lower() != "joined" and not pending_chat_ids:
        try:
            rows = (supabase.table("join_requests").select("chat_id,status")
                    .eq("user_id", uid).execute().data or [])
            for row in rows:
                if str(row.get("status", "pending")).lower() == "pending":
                    raw_id = row.get("chat_id")
                    if raw_id is not None:
                        pending_chat_ids.add(str(raw_id))
                        try: pending_chat_ids.add(str(int(raw_id)))
                        except (TypeError, ValueError): pass
        except Exception as exc:
            print(f"PENDING REQUEST READ ERROR user={uid}: {exc}")

    def check_channel(c):
        chat_id = c.get("chat_id")
        normalized_chat_id = str(chat_id)

        # Exact same-user pending request => verified, regardless of Telegram
        # membership status or whether an admin has approved the request yet.
        method=get_setting("verification_method","both").lower()
        if method != "joined" and normalized_chat_id in pending_chat_ids:
            print(f"VERIFY REQUEST MATCH user={uid} chat={normalized_chat_id}")
            return None
        if method == "request" and normalized_chat_id not in pending_chat_ids:
            return c

        method=get_setting("verification_method","both").lower()
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

        # Pending requests were already loaded in one DB query above.
        # Do not make another Supabase request per channel on every Verify.
        return c

    # Telegram membership checks are independent, so run them concurrently.
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
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})

def cleanup_verification_state(chat_id,user_id,code,include_base=True):
    key=(chat_id,user_id,code)
    item=VERIFICATION_MESSAGES.pop(key,{})
    mapping=[("cleanup_force",item.get("base") if include_base else None),("cleanup_failed",item.get("failed")),("cleanup_retry",item.get("retry")),("cleanup_anim",item.get("anim"))]
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
    if del_seconds > 0:
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
        if warn_msg:
            w = tg_call("sendMessage", {
                "chat_id": chat_id,
                "text": warn_msg,
                "parse_mode": "Markdown",
                "reply_markup": {"inline_keyboard": kb} if kb else None
            })
            if w.get("ok"):
                mids.append(w["result"]["message_id"])

        threading.Thread(target=auto_del_task, args=(chat_id, mids, del_seconds), daemon=True).start()

# --- Verification Worker Engine ---
def run_verify_process(chat_id, user_id, code, fj_mid):
    if get_setting("verification_enabled", "true").lower() != "true":
        cleanup_verification_state(chat_id,user_id,code,include_base=True)
        deliver_file(chat_id,code)
        return
    key=(chat_id,user_id,code)
    lock=VERIFICATION_LOCKS.setdefault(key, threading.Lock())
    if not lock.acquire(blocking=False):
        return
    try:
        anim_enabled=get_setting("anim_enabled","true").lower()=="true"
        try: anim_speed=max(0.10,min(float(get_setting("anim_speed","0.50")),5.0))
        except Exception: anim_speed=0.50
        base=get_setting("anim_text",DEFAULT_ANIM_TEXT).strip() or "Verifying"
        frames=[f"{base}.",f"{base}..",f"{base}..."]
        stop=threading.Event(); anim_mid=None
        def animate():
            nonlocal anim_mid
            r=tg_call("sendMessage",{"chat_id":chat_id,"text":frames[0]})
            if not r.get("ok"): return
            anim_mid=r["result"]["message_id"]; _track_verification(chat_id,user_id,code,anim=anim_mid)
            idx=1
            while not stop.wait(anim_speed):
                r=tg_call("editMessageText",{"chat_id":chat_id,"message_id":anim_mid,"text":frames[idx]})
                if not r.get("ok"): break
                idx=(idx+1)%3
        th=None
        if anim_enabled:
            th=threading.Thread(target=animate,daemon=True); th.start()
        # Verification is the only expensive operation; all channels are checked concurrently.
        unjoined=get_unjoined(user_id)
        if anim_enabled:
            elapsed=0
            if th: th.join(timeout=0)
            # Ensure at least one visible cycle, but never add a post-result delay.
            time.sleep(max(0, min(anim_speed*3, 0.9)))
            stop.set()
            if th: th.join(timeout=max(0.5,anim_speed+0.2))
            if anim_mid and get_setting("cleanup_anim","true").lower()=="true":
                tg_call("deleteMessage",{"chat_id":chat_id,"message_id":anim_mid})
            VERIFICATION_MESSAGES.get(key,{}).pop("anim",None)
        if not unjoined:
            cleanup_verification_state(chat_id,user_id,code,include_base=True)
            deliver_file(chat_id,code)
            return
        # Remove the old force-join/retry UI before presenting the fresh failure state.
        cleanup_verification_state(chat_id,user_id,code,include_base=True)
        failed_on=get_setting("verify_failed_enabled","true").lower()=="true"
        retry_on=get_setting("verify_retry_enabled","true").lower()=="true"
        mids={}
        kb={"reply_markup":{"inline_keyboard":_fj_keyboard(unjoined,code)}}
        if failed_on:
            r=tg_call("sendMessage",{"chat_id":chat_id,"text":get_setting("verify_failed_text",DEFAULT_VERIFY_FAILED),"parse_mode":"Markdown",**kb})
            if r.get("ok"): mids["failed"]=r["result"]["message_id"]
        if retry_on:
            r=tg_call("sendMessage",{"chat_id":chat_id,"text":get_setting("verify_retry_text",DEFAULT_VERIFY_RETRY),"parse_mode":"Markdown",**kb})
            if r.get("ok"): mids["retry"]=r["result"]["message_id"]
        if not failed_on and not retry_on:
            r=tg_call("sendMessage",{"chat_id":chat_id,"text":get_setting("verify_retry_text",DEFAULT_VERIFY_RETRY),"parse_mode":"Markdown",**kb})
            if r.get("ok"): mids["retry"]=r["result"]["message_id"]
        _track_verification(chat_id,user_id,code,**mids)
    finally:
        lock.release(); VERIFICATION_LOCKS.pop(key,None)

# --- Sub-Menu Display Functions ---
def show_admin_home(chat_id, mid=None):
    kb=[[{"text":"📦 "+mono("Files"),"callback_data":"m_files"},{"text":"📢 "+mono("Force Join"),"callback_data":"m_fj"}],
        [{"text":"📣 "+mono("Broadcast"),"callback_data":"m_bc"},{"text":"👥 "+mono("Users"),"callback_data":"m_users"}],
        [{"text":"⚙️ "+mono("Settings"),"callback_data":"m_sett"},{"text":"📊 "+mono("Statistics"),"callback_data":"m_stats"}]]
    txt="⚡ **ADMIN CONTROL**\n\nManage files, access rules, users and bot behaviour.\n\n🟢 System Online"
    data={"chat_id":chat_id,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}} if mid else {"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}}
    tg_call("editMessageText" if mid else "sendMessage",data)

def show_files_menu(chat_id, mid=None):
    kb = [
        [{"text": "➕ "+mono("Add File"), "callback_data": "f_add"}, {"text": "🗑 "+mono("Remove File"), "callback_data": "f_del"}],
        [{"text": "✏️ "+mono("Edit File"), "callback_data": "f_edit"}, {"text": "📁 "+mono("List Files"), "callback_data": "f_list"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = f"📦 **Files Management:**"
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

def show_fj_menu(chat_id, mid=None):
    kb = [
        [{"text": "📝 "+mono("Caption"), "callback_data": "fj_caption"}, {"text": "🖼 "+mono("Poster (Add/Remove)"), "callback_data": "fj_poster"}],
        [{"text": "➕ "+mono("ADD (Slot)"), "callback_data": "fj_add"}, {"text": "✏️ "+mono("EDIT (Slot)"), "callback_data": "fj_edit"}],
        [{"text": "🔘 "+mono("ON/OFF (Slot)"), "callback_data": "fj_onoff"}, {"text": "🗑 "+mono("REMOVE (Slot)"), "callback_data": "fj_rem"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = f"📢 **Force Join Configuration:**"
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

def show_settings_menu(chat_id, mid=None):
    txt="⚙️ **SETTINGS**\n\nConfigure every part of the bot from organized sections.\n\nChoose a category below."
    kb=[[{"text":"👤 "+mono("User Experience"),"callback_data":"set_user"}],
        [{"text":"📢 "+mono("Force Join"),"callback_data":"set_fj"},{"text":"✅ "+mono("Verification"),"callback_data":"set_verify"}],
        [{"text":"🎞 "+mono("Animation"),"callback_data":"set_anim"},{"text":"🗑 "+mono("Message Cleanup"),"callback_data":"set_cleanup"}],
        [{"text":"📝 "+mono("Messages"),"callback_data":"set_msg"},{"text":"📦 "+mono("File Delivery"),"callback_data":"set_delivery"}],
        [{"text":"🔘 "+mono("Buttons"),"callback_data":"set_buttons"},{"text":"🖼 "+mono("Media"),"callback_data":"set_media"}],
        [{"text":"🔗 "+mono("Links"),"callback_data":"set_links"},{"text":"🛠 "+mono("System"),"callback_data":"set_system"}],
        [{"text":"⬅️ "+mono("Back"),"callback_data":"adm_home"}]]
    data={"chat_id":chat_id,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}} if mid else {"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}}
    tg_call("editMessageText" if mid else "sendMessage",data)

def settings_submenu(cid,mid,kind):
    def status(k,default="true"): return "ON" if get_setting(k,default).lower()=="true" else "OFF"
    if kind=="verify":
        txt=f"✅ **VERIFICATION**\n\nStatus: `{status('verification_enabled')}`\nMethod: `BOTH` — Joined + Join Request\n\nBoth methods are accepted. Request approval is not required."
        kb=[[{"text":"🟢 ON","callback_data":"verify_on"},{"text":"🔴 OFF","callback_data":"verify_off"}],
            [{"text":"🔍 "+mono("Verification Method"),"callback_data":"verify_method"}],
            [{"text":"❌ "+mono("Failed Message"),"callback_data":"msg_failed"},{"text":"🔄 "+mono("Retry Message"),"callback_data":"msg_retry"}],
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
        items=[("Force Join","cleanup_force"),("Failed Message","cleanup_failed"),("Retry Message","cleanup_retry"),("Animation","cleanup_anim"),("Delivered File","cleanup_file"),("Important Notice","cleanup_notice")]
        kb=[]
        for label,key in items:
            kb.append([{"text":f"{label}: {'🟢 ON' if status(key) == 'ON' else '🔴 OFF'}","callback_data":key}])
        kb += [[{"text":"⏱ "+mono("Delivery Timer"),"callback_data":"s_timer"}],[{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    elif kind=="msg":
        txt="📝 **MESSAGES**\n\nEdit every user-facing message without touching the code."
        kb=[[{"text":"👋 "+mono("Welcome"),"callback_data":"s_wel"},{"text":"⚠️ "+mono("Error"),"callback_data":"s_err"}],
            [{"text":"❌ "+mono("Verification Failed"),"callback_data":"msg_failed"},{"text":"🔄 "+mono("Retry"),"callback_data":"msg_retry"}],
            [{"text":"⚠️ "+mono("Important Notice"),"callback_data":"s_warn"}],
            [{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    else:
        txt=f"{kind.replace('_',' ').title()} Settings"
        kb=[[{"text":"⬅️ "+mono("Back"),"callback_data":"m_sett"}]]
    tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def show_users_menu(chat_id, mid=None):
    cnt = len(supabase.table("users").select("telegram_user_id").execute().data or []) if supabase else 0
    kb = [
        [{"text": "📜 View Serial List", "callback_data": "u_list"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = f"👥 **Users Overview:**\n\n• Total Active Users: `{cnt}`"
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

@app.route("/", methods=["GET"])
def home():
    return "Bot Core Online 🚀"

def process_callback_query(cq):
    uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")

    if uid in ADMIN_IDS:
        # Main Categories Navigation
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
        elif cdata == "set_user":
            settings_submenu(cid,mid,"msg")
        elif cdata == "set_fj":
            show_fj_menu(cid,mid)
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
        elif cdata == "msg_failed":
            ADMIN_SESSIONS[uid]={"step":"SET_VERIFY_FAILED"}; tg_call("sendMessage",{"chat_id":cid,"text":"Send new Verification Failed message:"})
        elif cdata == "msg_retry":
            ADMIN_SESSIONS[uid]={"step":"SET_VERIFY_RETRY"}; tg_call("sendMessage",{"chat_id":cid,"text":"Send new Retry message:"})
        elif cdata.startswith("cleanup_"):
            k=cdata; cur=get_setting(k,"true").lower()=="true"; set_setting(k,"false" if cur else "true"); settings_submenu(cid,mid,"cleanup")
        elif cdata == "anim_preview":
            tg_call("sendMessage",{"chat_id":cid,"text":"Verifying.\nVerifying..\nVerifying..."})

        # --- Files Sub-Module ---
        elif cdata == "f_add":
            ADMIN_SESSIONS[uid] = {"step": "F_TITLE"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter file title:"})

        elif cdata.startswith("fmode_"):
            mode = cdata.split("_")[1]
            sess = ADMIN_SESSIONS.get(uid, {})
            if supabase and "t" in sess and "c" in sess and "fid" in sess:
                supabase.table("apps").upsert({
                    "title": sess["t"],
                    "deep_link_code": sess["c"],
                    "storage_message_id": sess["fid"],
                    "sender_mode": mode,
                    "active": True
                }, on_conflict="deep_link_code").execute()
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
            f_code = cb_suffix(cdata, "cf_fdel_")
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
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_TITLE", "code": cb_suffix(cdata, "ef_t_")}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter new title:"})
        elif cdata.startswith("ef_c_"):
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_CODE", "old_code": cb_suffix(cdata, "ef_c_")}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter new short code:"})
        elif cdata.startswith("ef_p_"):
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
            ADMIN_SESSIONS[uid] = {"step": "SET_FJ_CAPTION"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Send new Force Join caption (`{name}` for dynamic user name):"})

        elif cdata == "fj_poster":
            kb = [
                [{"text": "➕ Set / Replace Poster", "callback_data": "fjp_add"}],
                [{"text": "🗑 Remove Poster", "callback_data": "fjp_rem"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "🖼 **Poster Management:**", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata == "fjp_add":
            ADMIN_SESSIONS[uid] = {"step": "SET_POSTER"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Forward image/post directly from Storage Channel:"})

        elif cdata == "fjp_rem":
            set_setting("poster_storage_id", "")
            tg_call("sendMessage", {"chat_id": cid, "text": "Deleted"})
            show_fj_menu(cid)

        elif cdata == "fj_add":
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
            ADMIN_SESSIONS[uid] = {"step": "SET_W"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Send new welcome message:"})
        elif cdata == "s_err":
            ADMIN_SESSIONS[uid] = {"step": "SET_E"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Send new error message:"})
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
            ADMIN_SESSIONS[uid] = {"step": "SET_WARN"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Send new Important Notice text ({time} will be dynamically replaced with the duration):"})
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
            ADMIN_SESSIONS[uid] = {"step": "SET_NOTICE_URL"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Send JOIN NOW link (same Lexi Mods link you want to use):"})
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
                ADMIN_SESSIONS[uid] = {"step": "SET_CUSTOM_TIMER"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter time in seconds:"})
            else:
                set_setting("auto_delete_time", sec)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "s_upbtn":
            ADMIN_SESSIONS[uid] = {"step": "SET_UP_TITLE"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter update button label (or send `none` to disable):"})
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
                ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_SPEED"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter animation speed in seconds (0.10 - 5.00):"})
            else:
                set_setting("anim_speed", speed_value)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "anim_text":
            ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_TEXT"}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter new base animated text (e.g. `Verifying`):"})

    return "OK", 200


def warm_caches():
    try:
        _load_settings_cache()
        get_channels()
    except Exception:
        pass


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200

    # Join Request Router
    # A request itself is enough for verification; Telegram approval is NOT required.
    if "chat_join_request" in data:
        j_req = data["chat_join_request"]
        try:
            user_id = int(j_req["from"]["id"])
            chat_id_raw = j_req["chat"]["id"]
            chat_id = str(chat_id_raw)
            saved = False
            PENDING_JOIN_REQUESTS[(user_id, chat_id)] = time.time()
            if supabase:
                payload = {"user_id": user_id, "chat_id": chat_id, "status": "pending"}
                try:
                    supabase.table("join_requests").upsert(payload).execute()
                    saved = True
                except Exception:
                    # Some older schemas use BIGINT for chat_id. Fall back to integer.
                    try:
                        payload["chat_id"] = int(chat_id)
                        supabase.table("join_requests").upsert(payload).execute()
                        saved = True
                    except Exception as exc:
                        print(f"JOIN REQUEST DB ERROR user={user_id} chat={chat_id}: {exc}")
            print(f"JOIN REQUEST RECEIVED user={user_id} chat={chat_id} saved={saved}")
        except Exception as exc:
            print(f"JOIN REQUEST HANDLER ERROR: {exc}")
        return "OK", 200

    # Callback Query Router: acknowledge immediately, then do all work in a worker.
    if "callback_query" in data:
        cq = data["callback_query"]
        uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")
        tg_call("answerCallbackQuery", {"callback_query_id": cq["id"]})
        if cdata.startswith("v:"):
            code = cdata.split(":", 1)[1]
            threading.Thread(target=run_verify_process, args=(cid, uid, code, mid), daemon=True).start()
        else:
            threading.Thread(target=process_callback_query, args=(cq,), daemon=True).start()
        return "OK", 200

    # Message Router: dispatch immediately so the webhook can return 200 without
    # waiting for Supabase/Telegram network calls. This removes webhook latency.
    if "message" in data:
        threading.Thread(target=process_message_update, args=(data,), daemon=True).start()
        return "OK", 200

    return "OK", 200

def process_message_update(data):
    msg = data["message"]
    cid, uid, txt = msg["chat"]["id"], msg["from"]["id"], msg.get("text", "")
    uname = msg["from"].get("first_name", "User")
    mid = msg["message_id"]

    # Admin Panel Command
    if txt == "/admin" and uid in ADMIN_IDS:
        ADMIN_SESSIONS.pop(uid, None)
        show_admin_home(cid)
        return "OK", 200

    # Admin Multi-Step Input Processing
    if uid in ADMIN_IDS and uid in ADMIN_SESSIONS:
        sess = ADMIN_SESSIONS[uid]
        st = sess.get("step")

        # File Registration Steps
        if st == "SET_VERIFY_FAILED":
            set_setting("verify_failed_text", txt); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Verification Failed message updated."}); show_settings_menu(cid); return "OK",200
        elif st == "SET_VERIFY_RETRY":
            set_setting("verify_retry_text", txt); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Retry message updated."}); show_settings_menu(cid); return "OK",200
        elif st == "F_TITLE":
            sess["t"], sess["step"] = txt.strip(), "F_CODE"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter short code (e.g. `1`, `vip`):"})
        elif st == "F_CODE":
            sess["c"], sess["step"] = txt.strip(), "F_FILE"
            tg_call("sendMessage", {"chat_id": cid, "text": "Now forward the file post directly from Storage Channel:"})
        elif st == "F_FILE":
            fwd_id = msg.get("forward_from_message_id")
            fwd_chat = msg.get("forward_from_chat", {})
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

        # File Edit Steps
        elif st == "F_EDIT_TITLE":
            if supabase:
                supabase.table("apps").update({"title": txt.strip()}).eq("deep_link_code", sess["code"]).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid)

        elif st == "F_EDIT_CODE":
            if supabase:
                supabase.table("apps").update({"deep_link_code": txt.strip()}).eq("deep_link_code", sess["old_code"]).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid)

        elif st == "F_EDIT_FILE":
            fwd_id = msg.get("forward_from_message_id")
            if not fwd_id:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please forward directly from Storage Channel."})
                return "OK", 200
            if supabase:
                supabase.table("apps").update({"storage_message_id": fwd_id}).eq("deep_link_code", sess["code"]).execute()
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
            ADMIN_SESSIONS.pop(uid, None)
            show_files_menu(cid)

        # Force Join Caption & Poster
        elif st == "SET_FJ_CAPTION":
            set_setting("force_join_text", txt)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_fj_menu(cid)

        elif st == "SET_POSTER":
            fwd_id = msg.get("forward_from_message_id")
            fwd_chat = msg.get("forward_from_chat", {})
            if fwd_id:
                if fwd_chat and fwd_chat.get("id"):
                    set_setting("storage_channel_id", str(fwd_chat["id"]))
                set_setting("poster_storage_id", str(fwd_id))
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_fj_menu(cid)
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please forward directly from Storage Channel."})

        # Force Join Slot Add
        elif st == "FJ_ADD_TITLE":
            sess["title"], sess["step"] = txt.strip(), "FJ_ADD_URL"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter channel join invite link:"})
        elif st == "FJ_ADD_URL":
            sess["url"], sess["step"] = txt.strip(), "FJ_ADD_ID"
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

        # Force Join Slot Edit
        elif st == "FJ_E_TITLE":
            sess["title"], sess["step"] = txt.strip(), "FJ_E_URL"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter updated channel invite link:"})
        elif st == "FJ_E_URL":
            sess["url"], sess["step"] = txt.strip(), "FJ_E_ID"
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

        # Broadcast Execution
        elif st == "WAIT_BC_CONTENT":
            sess["bc_payload"] = {"from_chat": cid, "mid": mid}
            kb = [[{"text": "✅ Yes, Send Broadcast", "callback_data": "send_confirmed_bc"}], [{"text": "❌ Cancel", "callback_data": "adm_home"}]]
            tg_call("sendMessage", {"chat_id": cid, "text": "Broadcast preview recorded. Confirm delivery?", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        # Settings Inputs
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
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter update button URL:"})
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

    # Public Deep-Link & Default /start
    if txt.startswith("/start"):
        parts = txt.split()
        if len(parts) > 1:
            code = parts[1].strip()
            if send_fj(cid, uid, code, uname):
                deliver_file(cid, code)
        else:
            tg_call("sendMessage", {"chat_id": cid, "text": get_setting("welcome_text", DEFAULT_WELCOME), "parse_mode": "Markdown"})

        # Track the user after the user-facing /start response is initiated.
        # This prevents a Supabase write from adding latency to the response path.
        if supabase and uid:
            try:
                supabase.table("users").upsert({
                    "telegram_user_id": uid,
                    "username": msg["from"].get("username", ""),
                    "first_name": uname
                }).execute()
            except Exception:
                pass
        return "OK", 200



    return "OK", 200

def register_webhook():
    # Render provides RENDER_EXTERNAL_URL automatically. WEBHOOK_URL can
    # override it when using another host/domain.
    base_url = os.environ.get("WEBHOOK_URL", "").strip().rstrip("/")
    if not base_url:
        base_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    if not base_url or not BOT_TOKEN:
        return
    webhook_url = base_url if base_url.endswith("/webhook") else f"{base_url}/webhook"
    tg_call("setWebhook", {
        "url": webhook_url,
        "allowed_updates": ["message", "callback_query", "chat_join_request"]
    })


# Warm read-heavy caches in the background so first user replies are fast.
threading.Thread(target=warm_caches, daemon=True).start()

# Register webhook when imported by Gunicorn on Render as well as when run directly.
if BOT_TOKEN and (os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL")):
    threading.Thread(target=register_webhook, daemon=True).start()

if __name__ == "__main__":
    register_webhook()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

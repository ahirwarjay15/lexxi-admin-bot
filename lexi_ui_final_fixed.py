import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import traceback
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
ADMIN_MENU_MESSAGES = {}  # admin chat_id -> set of active bot menu message_ids
KEY_FLOW_MESSAGES = {}    # user chat_id -> bot messages belonging to current key-purchase flow
KEY_REPLY_ACTIONS = {}    # chat_id -> reply-keyboard text -> logical action
PENDING_JOIN_REQUESTS = {}  # (user_id, normalized_chat_id) -> timestamp
VERIFICATION_MESSAGES = {}  # (chat_id, user_id, code) -> {base, failed, retry, anim}
VERIFICATION_LOCKS = {}
# Persistent worker pool: background webhook jobs must not disappear with the request thread.
_UPDATE_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="update")
# Public /start and normal-user updates get their own pool. This prevents
# database-heavy verification/request bookkeeping from ever delaying a deep link.
_PUBLIC_EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="public")
# Verification can legitimately wait on Telegram/Supabase. Keep it isolated so
# slow verification jobs can never starve admin commands or normal messages.
_VERIFY_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="verify")

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
    except Exception as exc:
        print(f"TELEGRAM API ERROR method={method}: {exc}", flush=True)
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
            _SETTINGS_CACHE.clear()
            _SETTINGS_CACHE.update({str(r.get("key")): r.get("value", "") for r in rows if r.get("key") is not None})
            _SETTINGS_CACHE_READY = True
        except Exception as exc:
            print(f"SETTINGS CACHE LOAD ERROR: {exc}", flush=True)


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
            _CHANNELS_CACHE_READY = True
        except Exception as exc:
            print(f"CHANNELS CACHE LOAD ERROR: {exc}", flush=True)
            _CHANNELS_CACHE = None
            _CHANNELS_CACHE_READY = False
            return None
        return list(_CHANNELS_CACHE or [])


def get_unjoined(user_id):
    """Return active channels still not joined/requested by this exact user.

    A pending Join Request from this same user for the same channel counts as
    verified immediately, even while Telegram still reports the user as left.
    Membership API checks run in parallel so 4+ force-join channels do not add
    their network latency one after another.
    """
    channels = get_channels()
    uid = int(user_id)

    if channels is None:
        raise RuntimeError("Unable to load Force Join configuration")

    active_channels = [c for c in channels if c.get("active") and c.get("chat_id") is not None]
    if not active_channels:
        return []

    # Read all pending requests for THIS user once, then normalize chat IDs so
    # TEXT/BIGINT Supabase schemas cannot cause a false Verify failure.
    pending_chat_ids = {str(chat) for (pending_uid, chat), ts in PENDING_JOIN_REQUESTS.items()
                        if pending_uid == uid}
    method = get_setting("verification_method", "both").lower()
    if supabase and method != "joined":
        try:
            # New schema may contain status; older installations may only have
            # user_id/chat_id.  First try the richer query, then fall back to
            # chat_id-only so a missing status column never breaks verification.
            try:
                rows = (supabase.table("join_requests").select("chat_id,status")
                        .eq("user_id", uid).execute().data or [])
                for row in rows:
                    if str(row.get("status", "pending")).lower() == "pending":
                        raw_id = row.get("chat_id")
                        if raw_id is not None:
                            pending_chat_ids.add(str(raw_id))
                            try:
                                pending_chat_ids.add(str(int(raw_id)))
                            except (TypeError, ValueError):
                                pass
            except Exception as rich_exc:
                print(f"PENDING REQUEST STATUS FALLBACK user={uid}: {rich_exc}", flush=True)
                rows = (supabase.table("join_requests").select("chat_id")
                        .eq("user_id", uid).execute().data or [])
                for row in rows:
                    raw_id = row.get("chat_id")
                    if raw_id is not None:
                        pending_chat_ids.add(str(raw_id))
                        try:
                            pending_chat_ids.add(str(int(raw_id)))
                        except (TypeError, ValueError):
                            pass
        except Exception as exc:
            print(f"PENDING REQUEST READ ERROR user={uid}: {exc}", flush=True)

    def check_channel(c):
        chat_id = c.get("chat_id")
        normalized_chat_id = str(chat_id)

        # Exact same-user pending request => verified, regardless of Telegram
        # membership status or whether an admin has approved the request yet.
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

        # Respect independent cleanup toggles: the timer can remove the file,
        # the notice, both, or neither.
        delete_mids = []
        if cleanup_file:
            delete_mids.append(mids[0])
        if cleanup_notice and len(mids) > 1:
            delete_mids.extend(mids[1:])
        if delete_mids:
            threading.Thread(target=auto_del_task, args=(chat_id, delete_mids, del_seconds), daemon=True).start()

# --- Verification Worker Engine ---
def run_verify_process(chat_id, user_id, code, fj_mid):
    """Verify all configured channels while the optional animation runs.

    There is deliberately no artificial post-verification sleep: the file is
    delivered as soon as verification finishes and the animation message is
    stopped/cleaned up.
    """
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
                # If verification completed while sendMessage was in flight,
                # remove the just-created message instead of leaving it behind.
                mid = r.get("result", {}).get("message_id") if r.get("ok") else None
                if mid:
                    tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})
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

        # The expensive part runs immediately and independently of animation.
        unjoined = get_unjoined(user_id)

        stop.set()
        if anim_thread:
            # Do not hold delivery behind a slow Telegram edit.  The cleanup
            # call below also handles an animation message that arrived late.
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

        # Verification failed: show ONLY one short retry message, then
        # recreate the Force Join post with only the channels still pending.
        # The previous Force Join post is always removed before the new one
        # appears, so users never see two FJ posts at the same time.
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

        # send_fj() performs a fresh membership check and builds buttons only
        # for the remaining channels. It also tracks the new FJ message.
        send_fj(chat_id, user_id, code, "User")
    finally:
        lock.release()
        VERIFICATION_LOCKS.pop(key, None)

# --- Sub-Menu Display Functions ---
def _admin_clear_menu(chat_id):
    # Delete every tracked admin menu before opening the next one.
    # This prevents old menus from stacking after navigation.
    old = ADMIN_MENU_MESSAGES.pop(chat_id, None)
    if old is None:
        return
    mids = old if isinstance(old, (set, list, tuple)) else {old}
    for mid in list(mids):
        try:
            tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})
        except Exception:
            pass

def _admin_store_menu(chat_id, result):
    if isinstance(result, dict) and result.get("ok"):
        mid = result.get("result", {}).get("message_id")
        if mid:
            # A newly sent menu is the only menu that should remain tracked.
            # (Edited menus keep the same message_id.)
            ADMIN_MENU_MESSAGES[chat_id] = {mid}
    return result

def _key_send(chat_id, data, track=True):
    r = tg_call("sendMessage", data)
    if track and r.get("ok"):
        KEY_FLOW_MESSAGES.setdefault(chat_id, set()).add(r["result"]["message_id"])
    return r

def _reply_keyboard(chat_id, rows, actions=None):
    if actions is not None:
        KEY_REPLY_ACTIONS[chat_id] = dict(actions)
    return {"keyboard":[[{"text":str(x)} for x in row] for row in rows],
            "resize_keyboard":True,"is_persistent":True,
            "input_field_placeholder":"Choose an option…"}

def _key_reply_send(chat_id, text, rows, actions, track=True):
    # Buy Keys flow is a single-screen UI: remove the previous screen before
    # sending the next one, so only the current keyboard/menu remains visible.
    _clear_key_flow(chat_id)
    return _key_send(chat_id,{"chat_id":chat_id,"text":text,"parse_mode":"Markdown",
                              "reply_markup":_reply_keyboard(chat_id,rows,actions)},track=track)

def _clear_key_flow(chat_id):
    mids = list(KEY_FLOW_MESSAGES.pop(chat_id, set()))
    for mid in mids:
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})

def show_admin_home(chat_id, mid=None):
    # Main admin navigation is a Reply Keyboard; detail/submenus continue to use inline buttons.
    kb=[[{"text":"📦 Files","callback_data":"m_files"},{"text":"📢 Force Join","callback_data":"m_fj"}],
        [{"text":"📣 Broadcast","callback_data":"m_bc"},{"text":"👥 Users","callback_data":"m_users"}],
        [{"text":"⚙️ Settings","callback_data":"m_sett"},{"text":"🔑 Keys","callback_data":"m_keys"}],
        [{"text":"⚙️ /setup Content","callback_data":"m_setup"},{"text":"📊 Statistics","callback_data":"m_stats"}]]
    txt="⚡ **ADMIN CONTROL**\n\nManage files, Force Join, broadcasts, users, keys and /setup.\n\n🟢 System Online"
    # Telegram ReplyKeyboardButton cannot carry callback_data; use a real text keyboard for main navigation.
    style=get_setting("ui_style","crystal")
    prefix={"crystal":"💎","neon":"⚡","minimal":"•"}.get(style,"💎")
    reply_kb={"keyboard":[[{"text":f"{prefix} 📦 Files"},{"text":f"{prefix} 📢 Force Join"}],[{"text":f"{prefix} 📣 Broadcast"},{"text":f"{prefix} 👥 Users"}],[{"text":f"{prefix} ⚙️ Settings"},{"text":f"{prefix} 🔑 Keys"}],[{"text":f"{prefix} ⚙️ /setup Content"},{"text":f"{prefix} 📊 Statistics"}],[{"text":f"{prefix} 🎨 UI Studio"}],[{"text":"🚫 Hide Admin Panel"}]],"resize_keyboard":True,"is_persistent":True}
    if mid:
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})
    else:
        _admin_clear_menu(chat_id)
    data={"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":reply_kb}
    return _admin_store_menu(chat_id, tg_call("sendMessage",data))

def show_files_menu(chat_id, mid=None):
    kb = [
        [{"text": "➕ "+mono("Add File"), "callback_data": "f_add"}, {"text": "🗑 "+mono("Remove File"), "callback_data": "f_del"}],
        [{"text": "✏️ "+mono("Edit File"), "callback_data": "f_edit"}, {"text": "📁 "+mono("List Files"), "callback_data": "f_list"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = f"📦 **Files Management:**"
    if mid:
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})
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
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})
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
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})
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
    if mid: tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
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
    if mid: tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
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
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": mid})
    else:
        _admin_clear_menu(chat_id)
    return _admin_store_menu(chat_id, tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}}))

@app.route("/", methods=["GET"])
def home():
    return "Bot Core Online 🚀"

def _original_process_callback_query(cq):
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
            ADMIN_SESSIONS[uid]={"step":"SET_VERIFY_RETRY"}; cur=get_setting("verify_retry_text", DEFAULT_VERIFY_RETRY); tg_call("sendMessage",{"chat_id":cid,"text":f"📝 CURRENT MESSAGE:\n\n{cur}\n\n✏️ Send the new Retry message:"})
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
                    # Keep compatibility with older apps tables that do not have sender_mode.
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
            cur = get_setting("welcome_text", DEFAULT_WELCOME)
            tg_call("sendMessage", {"chat_id": cid, "text": f"📝 CURRENT WELCOME MESSAGE:\n\n{cur}\n\n✏️ Send the new welcome message:"})
        elif cdata == "s_err":
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
                ADMIN_SESSIONS[uid] = {"step": "SET_CUSTOM_TIMER"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter time in seconds:"})
            else:
                set_setting("auto_delete_time", sec)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "s_upbtn":
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
                ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_SPEED"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter animation speed in seconds (0.10 - 5.00):"})
            else:
                set_setting("anim_speed", speed_value)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "anim_text":
            ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_TEXT"}
            cur = get_setting("anim_text", DEFAULT_ANIM_TEXT)
            tg_call("sendMessage", {"chat_id": cid, "text": f"🎞 CURRENT ANIMATION TEXT:\n\n{cur}\n\n✏️ Send the new base text:"})

    return "OK", 200



# ========================= KEY STORE / BUYKEYS SYSTEM =========================
# This subsystem intentionally uses the existing `settings` table so no new
# database migration is required just to test the workflow. For production,
# Supabase tables with row-level locking are preferable.
KEY_DATA_LOCK = threading.RLock()
KEY_DATA_KEYS = {
    "products": "keys_products_v1",
    "orders": "keys_orders_v1",
    "users": "keys_users_v1",
    "setup": "setup_global_v1",
}
DEFAULT_KEY_TEXTS = {
    "key_lang": "🌐 **Select Language**\n\nChoose your preferred language:",
    "key_home_en": "🔑 **KEY CENTER**\n\nChoose an option below.",
    "key_home_hi": "🔑 **KEY CENTER**\n\nनीचे से एक option चुनें।",
    "product_text": "🛒 **{product}**\n\nSelect your duration:",
    "duration_text": "📅 **{duration}**\n💰 Price: ₹{price}\n\nProceed to payment to receive your key.",
    "payment_pending": "💳 **Payment Pending**\n\nOrder: `{order_id}`\nAmount: ₹{amount}\n\nPay using the QR/UPI option, then tap **Verify Payment**.",
    "payment_success": "✅ **Payment Successful**\n\nYour key has been assigned below.",
    "payment_failed": "❌ **Payment Not Confirmed**\n\nNo verified payment was found for this order.\nPlease retry with a new payment order.",
    "key_details": "🔐 **KEY DETAILS**\n\nProduct: {product}\nDuration: {duration}\nPrice: ₹{price}\nPurchased: {purchased}\nExpires: {expires}\nResets: {resets}/{max_resets}\n\n🔑 Key:\n`{key}`",
    "no_keys": "🔑 **MY KEYS**\n\nYou don't have any active keys yet.",
    "sold_admin": "💰 **1 Key Sold**\n\nProduct: {product}\nDuration: {duration}\nUser ID: `{user_id}`\nOrder: `{order_id}`",
    "stock_zero_admin": "⚠️ **Stock Empty**\n\n{product} → {duration} has reached 0 keys. The duration is now hidden from users.",
    "reset_success": "✅ **Key Reset Successful**",
    "delete_success": "🗑 **Key Deleted Successfully**",
}


def _json_setting(key, default):
    raw = get_setting(key, "")
    if not raw:
        return default
    try:
        value = __import__('json').loads(raw)
        return value
    except Exception:
        return default


def _save_json_setting(key, value):
    set_setting(key, __import__('json').dumps(value, ensure_ascii=False, separators=(',', ':')))


def key_products():
    return _json_setting(KEY_DATA_KEYS["products"], [])


def key_orders():
    return _json_setting(KEY_DATA_KEYS["orders"], [])


def key_setup():
    return _json_setting(KEY_DATA_KEYS["setup"], [])


def _key_text(name, default=None, **kwargs):
    value = get_setting("keymsg_" + name, DEFAULT_KEY_TEXTS.get(name, default or name))
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
    return prefix + str(int(time.time() * 1000)) + str(threading.get_ident())[-4:]


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


def _key_detail_text(p, d, k):
    max_resets = int(get_setting("key_max_resets", "3"))
    return _key_text("key_details", product=p.get("name",""), duration=d.get("name",""), price=d.get("price",0), purchased=_format_dt(k.get("purchased_at")), expires=_format_dt(k.get("expires_at")), resets=int(k.get("resets",0)), max_resets=max_resets, key=k.get("key",""))


def _send_user_key_message(chat_id, p, d, k, pin=True):
    text = _key_detail_text(p, d, k)
    r = tg_call("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    if r.get("ok") and pin:
        tg_call("pinChatMessage", {"chat_id": chat_id, "message_id": r["result"]["message_id"], "disable_notification": True})
    return r


def _admin_notice(text):
    for aid in ADMIN_IDS:
        tg_call("sendMessage", {"chat_id": aid, "text": text, "parse_mode": "Markdown"})


def _new_order(user_id, pid, did):
    with KEY_DATA_LOCK:
        products = key_products()
        p = _find_product(products, pid)
        d = _find_duration(p, did)
        if not p or not d or not p.get("enabled", True) or not d.get("enabled", True):
            return None, "unavailable"
        if not _available_keys(d):
            return None, "empty"
        amount = float(d.get("price", 0))
        if amount <= 0:
            return None, "invalid_price"
        order = {
            "id": "LEXI" + str(int(time.time() * 1000))[-10:],
            "user_id": int(user_id), "product_id": str(pid), "duration_id": str(did),
            "amount": amount, "status": "PENDING", "created_at": time.time(),
            "expires_at": time.time() + int(get_setting("payment_timeout", "600")),
            "payment_ref": _new_id("PAY"), "txn_ref": None, "payment_id": None,
        }
        orders = key_orders(); orders.append(order); _save_orders(orders)
        return order, None


def _mark_order_paid(order_id, amount, txn_ref, payment_id=None):
    with KEY_DATA_LOCK:
        orders = key_orders()
        order = next((o for o in orders if o.get("id") == order_id), None)
        if not order or order.get("status") != "PENDING":
            return None, "invalid_or_already_processed"
        if float(order.get("amount", 0)) != float(amount):
            return None, "amount_mismatch"
        if float(order.get("expires_at", 0)) < time.time():
            return None, "order_expired"
        if any(o.get("txn_ref") == txn_ref and o.get("status") == "PAID" for o in orders):
            return None, "duplicate_transaction"
        products = key_products(); p = _find_product(products, order.get("product_id")); d = _find_duration(p, order.get("duration_id"))
        if not p or not d:
            return None, "product_missing"
        available = _available_keys(d)
        if not available:
            return None, "stock_empty"
        key_obj = available[0]
        days = _duration_days(d.get("name", ""))
        if days is None:
            days = float(d.get("days", 1))
        now = time.time()
        key_obj.update({"status":"active", "assigned_to":int(order["user_id"]), "purchased_at":now, "expires_at":now + days*86400, "resets":0, "hwid":None, "order_id":order_id})
        order.update({"status":"PAID", "paid_at":now, "txn_ref":str(txn_ref), "payment_id":str(payment_id or txn_ref), "key_id":key_obj.get("id")})
        _save_products(products); _save_orders(orders)
        return (order, p, d, key_obj), None


def _qr_link(amount, order_id, payment_ref):
    # UPI deep-link. It does NOT verify payment by itself.
    upi_id = get_setting("payment_upi_id", "").strip()
    payee = get_setting("payment_payee_name", "LEXI").strip()
    if not upi_id:
        return ""
    from urllib.parse import quote
    return f"upi://pay?pa={quote(upi_id)}&am={quote(str(amount))}&pn={quote(payee)}&tn={quote('OrderID:'+order_id)}&tr={quote(payment_ref)}"


def _payment_message(chat_id, order):
    link = _qr_link(order["amount"], order["id"], order["payment_ref"])
    rows=[]; actions={}
    if link:
        rows.append(["💳 𝗣𝗔𝗬 𝗩𝗜𝗔 𝗨𝗣𝗜"]); actions[rows[-1][0]]="PAYURL:"+link
    rows += [["🔎 𝗩𝗘𝗥𝗜𝗙𝗬 𝗣𝗔𝗬𝗠𝗘𝗡𝗧"],["🔄 𝗡𝗘𝗪 𝗣𝗔𝗬𝗠𝗘𝗡𝗧"],["🔙 𝗕𝗔𝗖𝗞"]]
    actions.update({rows[-3][0]:"kp_verify:"+order["id"],rows[-2][0]:"kp_retry:"+order["id"],rows[-1][0]:"k_buy"})
    text = _key_text("payment_pending", order_id=order["id"], amount=order["amount"])
    if link: text += "\n\n⚠️ Pay the exact amount for this order."
    else: text += "\n\n⚠️ Admin payment UPI is not configured yet."
    return _key_reply_send(chat_id,text,rows,actions)

def _key_lang_menu(chat_id):
    rows=[["🇬🇧 𝗘𝗡𝗚𝗟𝗜𝗦𝗛","🇮🇳 𝗛𝗜𝗡𝗗𝗜"]]
    return _key_reply_send(chat_id,_key_text("key_lang"),rows,{rows[0][0]:"klang:en",rows[0][1]:"klang:hi"})

def _key_home(chat_id, lang="en"):
    rows=[["🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦"],["🔑 𝗠𝗬 𝗞𝗘𝗬𝗦"],["🏠 𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨"]]
    return _key_reply_send(chat_id,_key_text("key_home_en" if lang=="en" else "key_home_hi"),rows,
                           {rows[0][0]:"k_buy",rows[1][0]:"k_my",rows[2][0]:f"k_home:{lang}"})

def _product_menu(chat_id, lang="en"):
    products=[p for p in key_products() if p.get("enabled",True)]
    rows=[]; actions={}
    for p in products:
        label=f"💎 {p.get('emoji','📦')} {p.get('name','Product')}"
        rows.append([label]); actions[label]="k_prod:"+str(p.get("id"))
    back="🔙 𝗕𝗔𝗖𝗞"; rows.append([back]); actions[back]=f"k_home:{lang}"
    text="🛒 **BUY KEYS**\n\nSelect a product:" if products else "🛒 **BUY KEYS**\n\nNo products are available yet."
    return _key_reply_send(chat_id,text,rows,actions)

def _duration_menu(chat_id,pid):
    products=key_products(); p=_find_product(products,pid)
    if not p:return
    rows=[]; actions={}
    for d in p.get("durations",[]):
        if not d.get("enabled",True) or not _available_keys(d): continue
        label=f"📅 {d.get('name','1 Day')}"  # duration button intentionally contains no price
        rows.append([label]); actions[label]=f"k_dur:{pid}:{d.get('id')}"
    back="🔙 𝗕𝗔𝗖𝗞"; rows.append([back]); actions[back]="k_buy"
    return _key_reply_send(chat_id,_key_text("product_text",product=p.get("name","")),rows,actions)

def _duration_details(chat_id,pid,did):
    products=key_products(); p=_find_product(products,pid); d=_find_duration(p,did)
    if not p or not d or not d.get("enabled",True) or not _available_keys(d):
        tg_call("sendMessage",{"chat_id":chat_id,"text":"⚠️ This duration is currently unavailable."}); return
    rows=[["💳 𝗕𝗨𝗬 𝗡𝗢𝗪"],["🔙 𝗕𝗔𝗖𝗞"]]
    return _key_reply_send(chat_id,_key_text("duration_text",duration=d.get("name"),price=d.get("price")),rows,
                           {rows[0][0]:f"k_order:{pid}:{did}",rows[1][0]:f"k_prod:{pid}"})

def _my_keys(chat_id,user_id):
    active=_active_user_key(user_id)
    if not active:
        rows=[["🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦"],["🏠 𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨"]]
        return _key_reply_send(chat_id,_key_text("no_keys"),rows,{rows[0][0]:"k_buy",rows[1][0]:"k_home:en"})
    active.sort(key=lambda x:float(x[2].get("purchased_at",0)),reverse=True)
    if len(active)==1:
        p,d,k=active[0]; return _send_user_key_detail_menu(chat_id,user_id,p,d,k)
    rows=[]; actions={}
    for p,d,k in active:
        label=f"🔑 {d.get('name','Key')} • {_format_dt(k.get('purchased_at'))}"
        rows.append([label]); actions[label]="k_view:"+str(k.get("id"))
    rows += [["🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦"],["🏠 𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨"]]
    actions.update({rows[-2][0]:"k_buy",rows[-1][0]:"k_home:en"})
    return _key_reply_send(chat_id,"🔑 **MY KEYS**\n\nLatest purchase is shown first. Select a key:",rows,actions)

def _send_user_key_detail_menu(chat_id,user_id,p,d,k):
    days=_duration_days(d.get("name","")); can_reset=(days is not None and days>=7) or float(d.get("days",0) or 0)>=7
    resets=int(k.get("resets",0)); max_resets=int(get_setting("key_max_resets","3"))
    rows=[]; actions={}
    if can_reset and resets<max_resets:
        rows.append(["🔄 𝗥𝗘𝗦𝗘𝗧 𝗗𝗘𝗩𝗜𝗖𝗘"]); actions[rows[-1][0]]="k_reset:"+str(k.get("id"))
    rows.append(["🗑 𝗗𝗘𝗟𝗘𝗧𝗘 𝗞𝗘𝗬"]); actions[rows[-1][0]]="k_delete:"+str(k.get("id"))
    rows += [["🔙 𝗠𝗬 𝗞𝗘𝗬𝗦"],["🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦"]]
    actions.update({rows[-2][0]:"k_my",rows[-1][0]:"k_buy"})
    return _key_reply_send(chat_id,_key_detail_text(p,d,k),rows,actions,track=False)


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


def _start_main_menu(chat_id):
    # /start gets the Buy Keys entry directly at the bottom.
    # All buttons are real ReplyKeyboardButtons, not inline callbacks.
    buy = "🛒 𝗕𝗨𝗬 𝗞𝗘𝗬𝗦"
    my = "🔑 𝗠𝗬 𝗞𝗘𝗬𝗦"
    rows=[[buy],[my]]
    actions={buy:"START_BUY", my:"START_MY"}
    return _key_reply_send(chat_id, get_setting("welcome_text", DEFAULT_WELCOME) or DEFAULT_WELCOME, rows, actions)


def _start_buykeys(chat_id):
    # Language is always the first screen.
    _key_lang_menu(chat_id)


def _handle_key_callback(cq):
    uid=cq["from"]["id"]; cid=cq["message"]["chat"]["id"]; mid=cq["message"]["message_id"]; cdata=cq.get("data","")
    if cdata.startswith("klang:"):
        lang=cdata.split(":",1)[1]
        _key_home(cid,lang if lang in ("en","hi") else "en"); return True
    if cdata.startswith("k_home:"):
        _key_home(cid,cdata.split(":",1)[1]); return True
    if cdata=="k_buy": _product_menu(cid); return True
    if cdata=="k_my": _my_keys(cid,uid); return True
    if cdata.startswith("k_prod:"):
        _duration_menu(cid,cdata.split(":",1)[1]); return True
    if cdata.startswith("k_dur:"):
        _,pid,did=cdata.split(":",2); _duration_details(cid,pid,did); return True
    if cdata.startswith("k_order:"):
        _,pid,did=cdata.split(":",2); order,err=_new_order(uid,pid,did)
        if not order:
            tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ This option is currently unavailable."}); return True
        _payment_message(cid,order); return True
    if cdata.startswith("kp_verify:"):
        oid=cdata.split(":",1)[1]
        order=next((o for o in key_orders() if o.get("id")==oid and int(o.get("user_id"))==int(uid)),None)
        if not order:
            tg_call("sendMessage",{"chat_id":cid,"text":"❌ Order not found."}); return True
        if order.get("status")=="PAID":
            products=key_products(); p=_find_product(products,order.get("product_id")); d=_find_duration(p,order.get("duration_id")); k=next((x for x in d.get("keys",[]) if x.get("id")==order.get("key_id")),None) if d else None
            if p and d and k:
                _clear_key_flow(cid)
                _send_user_key_message(cid,p,d,k); return True
        if float(order.get("expires_at",0))<time.time():
            tg_call("sendMessage",{"chat_id":cid,"text":_key_text("payment_failed")}); return True
        zap_key=os.environ.get("ZAPUPI_API_KEY","").strip()
        if zap_key:
            try:
                rr=requests.post("https://pay.zapupi.com/api/order-status",json={"zap_key":zap_key,"order_id":oid},timeout=8)
                vd=(rr.json() or {}).get("data",{})
                if str(vd.get("status","")).lower()=="success":
                    amount=float(vd.get("pay_amount",vd.get("amount",0)) or 0); txn=str(vd.get("txn_id") or vd.get("utr") or "").strip()
                    result,err=_mark_order_paid(oid,amount,txn,vd.get("txn_id"))
                    if result:
                        order,p,d,k=result; _clear_key_flow(cid); _send_user_key_message(cid,p,d,k,pin=True); return True
            except Exception as exc: print(f"ZAPUPI VERIFY ERROR: {exc}",flush=True)
        tg_call("sendMessage",{"chat_id":cid,"text":"⏳ Payment is not verified yet. Please wait for confirmation, then tap Verify again."}); return True
    if cdata.startswith("kp_retry:"):
        old=cdata.split(":",1)[1]
        with KEY_DATA_LOCK:
            orders=key_orders()
            for o in orders:
                if o.get("id")==old and o.get("status")=="PENDING": o["status"]="EXPIRED"
            _save_orders(orders)
        order,err=_new_order(uid, next((o.get("product_id") for o in orders if o.get("id")==old),""), next((o.get("duration_id") for o in orders if o.get("id")==old),""))
        if order: _payment_message(cid,order)
        else: tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Unable to create a new order. The stock may be empty."})
        return True
    if cdata.startswith("k_view:"):
        kid=cdata.split(":",1)[1]
        for p,d,k in _active_user_key(uid):
            if str(k.get("id"))==kid: _send_user_key_detail_menu(cid,uid,p,d,k); break
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
        if not target: tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Key not found."}); return True
        p,d,k=target
        reqs=_json_setting("key_requests_v1",[]); reqs.append({"id":_new_id("REQ"),"action":action,"user_id":uid,"key_id":kid,"product":p.get("name"),"duration":d.get("name"),"created_at":time.time(),"status":"PENDING"}); _save_json_setting("key_requests_v1",reqs)
        _admin_notice(f"📨 **{action.title()} Key Request**\n\nUser: `{uid}`\nProduct: {p.get('name')}\nDuration: {d.get('name')}\nKey: `{k.get('key')}`\n\nUse Admin → Keys → Requests.")
        tg_call("sendMessage",{"chat_id":cid,"text":f"📨 {action.title()} request sent to admin.\n\nPlease wait for admin confirmation."})
        return True
    return False


def _keys_admin_menu(cid,mid=None):
    products=key_products(); total=sum(len(d.get("keys",[])) for p in products for d in p.get("durations",[]))
    txt=f"🔑 **KEY MANAGEMENT**\n\nProducts: `{len(products)}`\nInventory entries: `{total}`\n\nProducts start empty. Add your own products/durations/keys."
    kb=[[{"text":"➕ Add Product","callback_data":"ka_addp"},{"text":"✏️ Products","callback_data":"ka_products"}],
        [{"text":"➕ Add Key","callback_data":"ka_addk"},{"text":"📨 Requests","callback_data":"ka_requests"}],
        [{"text":"💳 Payment Settings","callback_data":"ka_payment"},{"text":"📝 Key Messages","callback_data":"ka_messages"}],
        [{"text":"⬅️ Back","callback_data":"adm_home"}]]
    if mid: tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}}))


def _keys_admin_products(cid,mid=None):
    products=key_products(); rows=[]
    for p in products:
        rows.append([{"text":f"{p.get('emoji','📦')} {p.get('name','Product')}","callback_data":"kap:"+str(p.get("id"))}])
    rows.append([{"text":"➕ Add Product","callback_data":"ka_addp"}]); rows.append([{"text":"⬅️ Back","callback_data":"m_keys"}])
    if mid: tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":"📦 **PRODUCTS**\n\nNo default products are created.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}))


def _keys_admin_product(cid,pid,mid=None):
    products=key_products(); p=_find_product(products,pid)
    if not p:return
    rows=[[{"text":"➕ Add Duration","callback_data":"kad_add:"+str(pid)}],[{"text":"✏️ Rename / Emoji","callback_data":"kap_edit:"+str(pid)}],[{"text":"🟢/🔴 Enable Product","callback_data":"kap_toggle:"+str(pid)}]]
    for d in p.get("durations",[]):
        rows.append([{"text":f"{d.get('emoji','📅')} {d.get('name')} • ₹{d.get('price')} • stock hidden","callback_data":"kad_view:"+str(pid)+":"+str(d.get('id'))}])
    rows.append([{"text":"🗑 Remove Product","callback_data":"kap_del:"+str(pid)}]); rows.append([{"text":"⬅️ Back","callback_data":"ka_products"}])
    if mid: tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":f"📦 **{p.get('name')}**\n\nConfigure durations below.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}))


def _key_admin_request_menu(cid,mid=None):
    reqs=_json_setting("key_requests_v1",[]); pending=[r for r in reqs if r.get("status")=="PENDING"]
    rows=[]
    for r in pending[-20:]: rows.append([{"text":f"{r.get('action','').upper()} • {r.get('user_id')} • {r.get('duration')}","callback_data":"kar:"+str(r.get("id"))}])
    rows.append([{"text":"⬅️ Back","callback_data":"m_keys"}])
    if mid: tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
    else: _admin_clear_menu(cid)
    return _admin_store_menu(cid, tg_call("sendMessage",{"chat_id":cid,"text":f"📨 **KEY REQUESTS**\n\nPending: `{len(pending)}`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}))


def _keys_admin_callback(cq):
    uid=cq["from"]["id"]; cid=cq["message"]["chat"]["id"]; mid=cq["message"]["message_id"]; c=cq.get("data","")
    if uid not in ADMIN_IDS:return False
    # Navigation cancels unfinished key-entry sessions, just like a normal Back
    # button should. Action callbacks below explicitly create a new session.
    if c in {"m_keys", "ka_products"} or c.startswith(("kap:", "kad_view:")):
        ADMIN_SESSIONS.pop(uid, None)
    if c=="m_keys": _keys_admin_menu(cid,mid); return True
    if c=="ka_products": _keys_admin_products(cid,mid); return True
    if c=="ka_addp": ADMIN_SESSIONS[uid]={"step":"K_ADD_PRODUCT"}; tg_call("sendMessage",{"chat_id":cid,"text":"Send product name. Example: Premium License"}); return True
    if c.startswith("kap:"): _keys_admin_product(cid,c.split(":",1)[1],mid); return True
    if c.startswith("kap_toggle:"):
        pid=c.split(":",1)[1]; ps=key_products(); p=_find_product(ps,pid); p["enabled"]=not p.get("enabled",True); _save_products(ps); _keys_admin_product(cid,pid,mid); return True
    if c.startswith("kap_del:"):
        pid=c.split(":",1)[1]; ps=[p for p in key_products() if str(p.get("id"))!=pid]; _save_products(ps); _keys_admin_products(cid,mid); return True
    if c.startswith("kap_edit:"):
        ADMIN_SESSIONS[uid]={"step":"K_EDIT_PRODUCT","pid":c.split(":",1)[1]}; tg_call("sendMessage",{"chat_id":cid,"text":"Send: `Product Name | Emoji`"}); return True
    if c.startswith("kad_add:"):
        ADMIN_SESSIONS[uid]={"step":"K_ADD_DURATION","pid":c.split(":",1)[1]}; tg_call("sendMessage",{"chat_id":cid,"text":"Send: `Duration Name | Emoji | Price`\nExample: `1 Day | 📅 | 120`"}); return True
    if c.startswith("kad_view:"):
        _,pid,did=c.split(":",2); ps=key_products(); p=_find_product(ps,pid); d=_find_duration(p,did)
        if not d:return True
        stock=len(_available_keys(d)); rows=[[{"text":"➕ Add Keys","callback_data":f"ka_addkeys:{pid}:{did}"},{"text":"✏️ Edit Duration","callback_data":f"kad_edit:{pid}:{did}"}],[{"text":"🗑 Remove Duration","callback_data":f"kad_del:{pid}:{did}"}],[{"text":"⬅️ Back","callback_data":f"kap:{pid}"}]]
        tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"📅 **{d.get('name')}**\n\nPrice: ₹{d.get('price')}\nAvailable: {stock} (admin only)","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(rows)}}); return True
    if c.startswith("ka_addkeys:"):
        _,pid,did=c.split(":",2); ADMIN_SESSIONS[uid]={"step":"K_ADD_KEYS","pid":pid,"did":did}; tg_call("sendMessage",{"chat_id":cid,"text":"Send keys one per line. Send `DONE` when finished."}); return True
    if c.startswith("kad_edit:"):
        _,pid,did=c.split(":",2); ADMIN_SESSIONS[uid]={"step":"K_EDIT_DURATION","pid":pid,"did":did}; tg_call("sendMessage",{"chat_id":cid,"text":"Send: `Duration Name | Emoji | Price`"}); return True
    if c.startswith("kad_del:"):
        _,pid,did=c.split(":",2); ps=key_products(); p=_find_product(ps,pid); p["durations"]= [d for d in p.get("durations",[]) if str(d.get("id"))!=did]; _save_products(ps); _keys_admin_product(cid,pid,mid); return True
    if c=="ka_addk":
        ADMIN_SESSIONS[uid]={"step":"K_ADD_KEYS_GLOBAL"}; tg_call("sendMessage",{"chat_id":cid,"text":"Send `Product ID | Duration ID | Key` for each key, one per line. Send `DONE` when finished."}); return True
    if c=="ka_requests": _key_admin_request_menu(cid,mid); return True
    if c.startswith("kar:"):
        rid=c.split(":",1)[1]; reqs=_json_setting("key_requests_v1",[]); r=next((x for x in reqs if str(x.get("id"))==rid),None)
        if not r:return True
        kb=[[{"text":"✅ Done","callback_data":"kar_done:"+rid},{"text":"❌ Reject","callback_data":"kar_reject:"+rid}],[{"text":"⬅️ Back","callback_data":"ka_requests"}]]
        tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"📨 **{r.get('action','').upper()} REQUEST**\n\nUser: `{r.get('user_id')}`\nProduct: {r.get('product')}\nDuration: {r.get('duration')}\nKey: `{r.get('key_id')}`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}}); return True
    if c.startswith("kar_"):
        action,rid=c.split(":",1); reqs=_json_setting("key_requests_v1",[]); r=next((x for x in reqs if str(x.get("id"))==rid),None)
        if not r:return True
        if action=="kar_done":
            ps=key_products()
            for p in ps:
                for d in p.get("durations",[]):
                    for k in d.get("keys",[]):
                        if str(k.get("id"))==str(r.get("key_id")):
                            if r.get("action")=="reset": k["hwid"]=None; k["resets"]=int(k.get("resets",0))+1
                            else: k["status"]="deleted"; k["assigned_to"]=None
            _save_products(ps); r["status"]="DONE"; _save_json_setting("key_requests_v1",reqs); tg_call("sendMessage",{"chat_id":int(r.get("user_id")),"text":_key_text("reset_success" if r.get("action")=="reset" else "delete_success")});
        else: r["status"]="REJECTED"; _save_json_setting("key_requests_v1",reqs); tg_call("sendMessage",{"chat_id":int(r.get("user_id")),"text":"❌ Request rejected by admin."})
        _key_admin_request_menu(cid,mid); return True
    if c=="ka_payment":
        ADMIN_SESSIONS[uid]={"step":"K_PAYMENT_SETTINGS"}; tg_call("sendMessage",{"chat_id":cid,"text":"Send: `UPI ID | Payee Name | Bridge Secret`\nBridge Secret is the secret your no-API companion verifier will use to POST verified credits."}); return True
    if c=="ka_messages":
        tg_call("sendMessage",{"chat_id":cid,"text":"Editable key messages are stored as settings. Use `/keymsg <name>` to edit. Names: key_lang, key_home_en, key_home_hi, product_text, duration_text, payment_pending, payment_success, payment_failed, key_details, no_keys, sold_admin, stock_zero_admin, reset_success, delete_success"}); return True
    return False


@app.route("/payment/webhook", methods=["POST"])
def zapupi_payment_webhook():
    data=request.get_json(silent=True) or {}
    try:
        order_id=str(data.get("order_id","")).strip(); status=str(data.get("status","")).strip().lower(); env=str(data.get("environment","")).strip().lower()
        if not order_id or env=="test" or status!="success": return {"status":"ok"},200
        zap_key=os.environ.get("ZAPUPI_API_KEY","").strip()
        if not zap_key: return {"status":"ok"},200
        rr=requests.post("https://pay.zapupi.com/api/order-status",json={"zap_key":zap_key,"order_id":order_id},timeout=8)
        body=rr.json() if rr.ok else {}; vd=body.get("data",{}) if isinstance(body,dict) else {}
        if str(vd.get("status","")).lower()!="success": return {"status":"ok"},200
        amount=float(vd.get("pay_amount",vd.get("amount",0)) or 0); txn=str(vd.get("txn_id") or vd.get("utr") or data.get("txn_id") or data.get("utr") or "").strip()
        result,err=_mark_order_paid(order_id,amount,txn,vd.get("txn_id") or data.get("txn_id"))
        if not result: print(f"ZAPUPI fulfillment skipped {order_id}: {err}",flush=True); return {"status":"ok"},200
        order,p,d,k=result; _admin_notice(_key_text("sold_admin",product=p.get("name"),duration=d.get("name"),user_id=order.get("user_id"),order_id=order.get("id")))
        if not _available_keys(d): _admin_notice(_key_text("stock_zero_admin",product=p.get("name"),duration=d.get("name")))
        _clear_key_flow(int(order["user_id"])); _send_user_key_message(int(order["user_id"]),p,d,k,pin=True)
    except Exception as exc: print(f"ZAPUPI WEBHOOK ERROR: {exc}",flush=True)
    return {"status":"ok"},200

@app.route("/payment/credit", methods=["POST"])
def payment_credit_bridge():
    # NO-API verification bridge endpoint. A trusted companion app must call
    # this after it has independently observed a real credit notification.
    secret=os.environ.get("PAYMENT_BRIDGE_SECRET", "")
    supplied=request.headers.get("X-Payment-Secret", "") or request.form.get("secret", "")
    if not secret or supplied != secret:
        return {"ok":False,"error":"unauthorized"},401
    data=request.get_json(silent=True) or request.form.to_dict()
    try:
        order_id=str(data.get("order_id","")); amount=float(data.get("amount",0)); txn_ref=str(data.get("txn_ref","")).strip()
        if not order_id or not txn_ref or amount<=0:return {"ok":False,"error":"invalid_payload"},400
        result,err=_mark_order_paid(order_id,amount,txn_ref,data.get("payment_id"))
        if not result:return {"ok":False,"error":err},409
        order,p,d,k=result
        _admin_notice(_key_text("sold_admin",product=p.get("name"),duration=d.get("name"),user_id=order.get("user_id"),order_id=order.get("id")))
        if not _available_keys(d): _admin_notice(_key_text("stock_zero_admin",product=p.get("name"),duration=d.get("name")))
        _clear_key_flow(int(order["user_id"]))
        _send_user_key_message(int(order["user_id"]),p,d,k,pin=True)
        return {"ok":True,"status":"PAID","order_id":order_id},200
    except Exception as exc:
        print(f"PAYMENT BRIDGE ERROR: {exc}",flush=True); return {"ok":False,"error":"server_error"},500


@app.route("/payment/status", methods=["POST"])
def payment_status_bridge():
    secret=os.environ.get("PAYMENT_BRIDGE_SECRET", "")
    supplied=request.headers.get("X-Payment-Secret", "")
    if not secret or supplied != secret:return {"ok":False},401
    data=request.get_json(silent=True) or {}
    oid=str(data.get("order_id","")); o=next((x for x in key_orders() if x.get("id")==oid),None)
    return {"ok":bool(o),"status":o.get("status") if o else "NOT_FOUND","amount":o.get("amount") if o else None},200


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
        try: name,emoji,price=parts[0],parts[1],float(parts[2])
        except Exception: tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Format: Duration | Emoji | Price"}); return True
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
    if st=="K_PAYMENT_SETTINGS":
        parts=[x.strip() for x in txt.split("|",2)]
        if len(parts)>=1:set_setting("payment_upi_id",parts[0])
        if len(parts)>=2:set_setting("payment_payee_name",parts[1])
        if len(parts)>=3:set_setting("payment_bridge_secret_hint",parts[2])
        ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Payment settings saved. Keep PAYMENT_BRIDGE_SECRET in Render Environment Variables; do not put the secret in chat."}); _keys_admin_menu(cid); return True
    return False


def _setup_admin_callback(cq):
    uid=cq["from"]["id"]; cid=cq["message"]["chat"]["id"]; mid=cq["message"]["message_id"]; c=cq.get("data","")
    if uid not in ADMIN_IDS:return False
    if c=="m_setup":
        items=key_setup(); rows=[[{"text":"➕ Add Content","callback_data":"setup_add"}],[{"text":"🗑 Clear Setup","callback_data":"setup_clear"}],[{"text":"⬅️ Back","callback_data":"adm_home"}]]
        tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"⚙️ **GLOBAL /setup CONTENT**\n\nSelected items: `{len(items)}`\n\nForward/send content one by one, then type `DONE`. After Force Join verification, `/setup` sends these global items.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":rows}}); return True
    if c=="setup_add": ADMIN_SESSIONS[uid]={"step":"SETUP_ADD"}; tg_call("sendMessage",{"chat_id":cid,"text":"Forward a message/media from Telegram. Send multiple items one by one. Type `DONE` when finished."}); return True
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
    if cmd=="/buykeys": _start_buykeys(cid); return True
    if cmd=="/mykeys": _my_keys(cid,uid); return True
    if cmd=="/setup":
        # Global /setup content uses the same Force Join + Join Request
        # verification path. Existing file verification remains untouched.
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


# Wrapper keeps all legacy Force Join/Join Request/file/admin behaviour intact,
# while adding the new key/setup/payment routes above it.
def process_callback_query(cq):
    c = cq.get("data", "")
    if c.startswith(("klang:", "k_home:", "k_buy", "k_my", "k_prod:", "k_dur:", "k_order:", "kp_verify:", "kp_retry:", "k_view:", "k_reset:", "k_delete:")):
        if _handle_key_callback(cq):
            return "OK", 200
    if c.startswith(("m_keys", "ka_", "kap:", "kap_", "kad_", "kar", "m_setup", "setup_")):
        if _keys_admin_callback(cq) or _setup_admin_callback(cq):
            return "OK", 200
    return _original_process_callback_query(cq)


def _handle_key_reply_text(uid,cid,txt):
    action=KEY_REPLY_ACTIONS.get(cid,{}).get(txt.strip())
    if not action:return False
    if action == "START_BUY":
        _start_buykeys(cid); return True
    if action == "START_MY":
        _my_keys(cid,uid); return True
    if action.startswith("PAYURL:"):
        tg_call("sendMessage",{"chat_id":cid,"text":"💳 **Open Payment Page:**\n"+action[7:],"parse_mode":"Markdown","disable_web_page_preview":False})
        return True
    return bool(_handle_key_callback({"from":{"id":uid},"message":{"chat":{"id":cid},"message_id":0},"data":action}))

def process_message_update(data):
    msg=data.get("message",{}); uid=msg.get("from",{}).get("id"); cid=msg.get("chat",{}).get("id"); txt=msg.get("text","") or ""
    if txt and _handle_key_reply_text(uid,cid,txt): return "OK",200
    # Commands/navigation always take priority over any unfinished admin input
    # session. This prevents `/buykeys`, `/mykeys`, `/setup`, `/admin`, etc. from
    # being accidentally consumed as duration/product/key input.
    cmd=txt.strip().split(maxsplit=1)[0].split("@",1)[0].lower() if txt.strip() else ""
    nav_candidate=re.sub(r"^(?:💎|⚡|•)\s*", "", txt.strip())
    admin_nav_words={"📦 Files","📢 Force Join","📣 Broadcast","👥 Users","⚙️ Settings","🔑 Keys","⚙️ /setup Content","📊 Statistics","🎨 UI Studio","💎 Crystal","⚡ Neon","⚪ Minimal","🔙 Back","🚫 Hide Admin Panel"}
    if uid in ADMIN_IDS and uid in ADMIN_SESSIONS and not cmd.startswith("/") and nav_candidate not in admin_nav_words and txt.strip() not in admin_nav_words:
        if _keys_admin_message_step(uid,cid,txt,msg) or _setup_admin_message(uid,cid,txt,msg):
            return "OK",200
        # `/keymsg name` is a direct admin editor and preserves raw message text.
    if uid in ADMIN_IDS:
        cmd=txt.strip().split(maxsplit=1)[0].split("@",1)[0].lower() if txt.strip() else ""
        if cmd=="/keymsg":
            parts=txt.split(maxsplit=2)
            if len(parts)>=3:
                set_setting("keymsg_"+parts[1].strip(),parts[2]); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Key message updated."}); return "OK",200
    # Reply-keyboard admin navigation opens the corresponding submenu as a new
    # bot message (the keyboard button itself is a user message and cannot be edited).
    if uid in ADMIN_IDS and txt.strip() in {"💎 Crystal","⚡ Neon","⚪ Minimal"}:
        style={"💎 Crystal":"crystal","⚡ Neon":"neon","⚪ Minimal":"minimal"}[txt.strip()]
        set_setting("ui_style",style)
        tg_call("sendMessage",{"chat_id":cid,"text":f"✅ UI style set to **{style.title()}**.","parse_mode":"Markdown"})
        show_admin_home(cid); return "OK",200
    if uid in ADMIN_IDS and txt.strip()=="🔙 Back":
        show_admin_home(cid); return "OK",200
    nav_txt=re.sub(r"^(?:💎|⚡|•)\s*", "", txt.strip())
    if uid in ADMIN_IDS and nav_txt in {"📦 Files","📢 Force Join","📣 Broadcast","👥 Users","⚙️ Settings","🔑 Keys","⚙️ /setup Content","📊 Statistics","🎨 UI Studio"}:
        ADMIN_SESSIONS.pop(uid, None)
        _admin_clear_menu(cid)
        if nav_txt=="📦 Files": show_files_menu(cid)
        elif nav_txt=="📢 Force Join": show_fj_menu(cid)
        elif nav_txt=="📣 Broadcast":
            ADMIN_SESSIONS[uid]={"step":"WAIT_BC_CONTENT"}; tg_call("sendMessage",{"chat_id":cid,"text":"📣 Please forward or send the message to broadcast:"})
        elif nav_txt=="👥 Users": show_users_menu(cid)
        elif nav_txt=="⚙️ Settings": show_settings_menu(cid)
        elif nav_txt=="🔑 Keys": _keys_admin_menu(cid)
        elif nav_txt=="⚙️ /setup Content":
            items=key_setup(); tg_call("sendMessage",{"chat_id":cid,"text":f"⚙️ **GLOBAL /setup CONTENT**\n\nSelected items: `{len(items)}`\n\nUse the buttons below to add/clear content.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":[[{"text":"➕ Add Content","callback_data":"setup_add"}],[{"text":"🗑 Clear Setup","callback_data":"setup_clear"}],[{"text":"⬅️ Back","callback_data":"adm_home"}]]}})
        elif nav_txt=="🎨 UI Studio":
            rows=[["💎 Crystal"],["⚡ Neon"],["⚪ Minimal"],["🔙 Back"]]
            tg_call("sendMessage",{"chat_id":cid,"text":"🎨 **UI STUDIO**\n\nTelegram ReplyKeyboard buttons do not support custom background/font colours. This studio controls the available visual style, Unicode typography, emoji and layout without pretending the Telegram client supports colours.","parse_mode":"Markdown","reply_markup":_reply_keyboard(cid,rows,{"💎 Crystal":"UI:crystal","⚡ Neon":"UI:neon","⚪ Minimal":"UI:minimal","🔙 Back":"adm_home"})})
        elif nav_txt=="📊 Statistics":
            users=len(supabase.table("users").select("telegram_user_id").execute().data or []) if supabase else 0; files=len(get_all_apps()); active=len([c for c in (get_channels() or []) if c.get("active")]); tg_call("sendMessage",{"chat_id":cid,"text":f"📊 **STATISTICS**\n\n👥 Users: `{users}`\n📦 Files: `{files}`\n📢 Active Channels: `{active}`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":[[{"text":"⬅️ Back","callback_data":"adm_home"}]]}})
        return "OK",200
    if uid in ADMIN_IDS and txt.strip()=="🚫 Hide Admin Panel":
        tg_call("sendMessage",{"chat_id":cid,"text":"Admin panel hidden.","reply_markup":{"remove_keyboard":True}}); return "OK",200
    if _key_command_wrapper(data):
        return "OK",200
    return _original_process_message_update(data)

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
            print(f"JOIN REQUEST STATUS SCHEMA FALLBACK user={user_id} chat={chat_id}: {first_exc}", flush=True)
        # Compatibility with older join_requests tables without status.
        for chat_value in (str(chat_id), int(chat_id)):
            try:
                supabase.table("join_requests").upsert({
                    "user_id": int(user_id), "chat_id": chat_value
                }).execute()
                return
            except Exception:
                continue
        raise RuntimeError("join_requests table is missing a compatible user_id/chat_id schema")
    except Exception as exc:
        print(f"JOIN REQUEST DB ERROR user={user_id} chat={chat_id}: {exc}", flush=True)


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
                # Keep the webhook response path fast. The in-memory entry above
                # already makes the request count as verified immediately.
                _UPDATE_EXECUTOR.submit(_save_join_request, user_id, chat_id)
            print(f"JOIN REQUEST RECEIVED user={user_id} chat={chat_id} saved=queued")
        except Exception as exc:
            print(f"JOIN REQUEST HANDLER ERROR: {exc}")
        return "OK", 200

    # Callback Query Router. Verification is isolated in its own executor so
    # slow membership/DB checks cannot starve the admin UI. Admin/non-verify
    # callbacks are handled directly because they are lightweight navigation
    # and should never wait behind background verification work.
    if "callback_query" in data:
        cq = data["callback_query"]
        uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")
        tg_call("answerCallbackQuery", {"callback_query_id": cq["id"]})
        if cdata.startswith("v:"):
            code = cdata.split(":", 1)[1]
            _VERIFY_EXECUTOR.submit(_safe_run, run_verify_process, cid, uid, code, mid)
        else:
            # Never block the Gunicorn request worker on Supabase/Telegram work
            # triggered by an admin button. Slow DB calls must not cause a 30s
            # WORKER TIMEOUT and kill the worker.
            _UPDATE_EXECUTOR.submit(_safe_run, process_callback_query, cq)
        return "OK", 200

    # Message Router. Critical commands (/start and /admin) are processed in
    # the webhook request itself. This is intentional: Telegram must never get
    # a 200 response while a worker-queue problem prevents a visible reply.
    # Other public messages stay asynchronous for throughput.
    if "message" in data:
        msg = data.get("message", {})
        uid = msg.get("from", {}).get("id")
        txt = msg.get("text", "") or ""
        try:
            _UPDATE_EXECUTOR.submit(_safe_run, process_message_update, data)
        except Exception as exc:
            print(f"UPDATE QUEUE ERROR user={uid} text={txt[:80]!r}: {exc}", flush=True)
            _safe_run(process_message_update, data)
        return "OK", 200

    return "OK", 200

def _track_user(uid, username, first_name):
    if not supabase or not uid:
        return
    base = {"telegram_user_id": int(uid), "username": username or ""}
    try:
        try:
            supabase.table("users").upsert({**base, "first_name": first_name or "User"}).execute()
            return
        except Exception as rich_exc:
            print(f"USER TRACK SCHEMA FALLBACK user={uid}: {rich_exc}", flush=True)
        # Compatibility with older users tables that do not have first_name.
        supabase.table("users").upsert(base).execute()
    except Exception as exc:
        print(f"USER TRACK ERROR user={uid}: {exc}", flush=True)


def _original_process_message_update(data):
    msg = data["message"]
    cid, uid, txt = msg["chat"]["id"], msg["from"]["id"], msg.get("text", "")
    uname = msg["from"].get("first_name", "User")
    mid = msg["message_id"]

    # Admin Panel Command. Some Telegram updates (photos/forwards) have no
    # text, so never index an empty split result here.
    command = txt.strip().split(maxsplit=1)[0].split("@", 1)[0].lower() if txt.strip() else ""
    if command == "/admin" and uid in ADMIN_IDS:
        ADMIN_SESSIONS.pop(uid, None)
        _admin_clear_menu(cid)
        show_admin_home(cid)
        return "OK", 200

    # Admin Multi-Step Input Processing
    if uid in ADMIN_IDS and uid in ADMIN_SESSIONS:
        sess = ADMIN_SESSIONS[uid]
        st = sess.get("step")

        # File Registration Steps
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
                    print(f"FILE CODE CHECK ERROR: {exc}", flush=True)
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
                    print(f"FILE CODE UPDATE ERROR: {exc}", flush=True)
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
                    # Update first, then delete the old post so a failed DB update
                    # never destroys the currently working file.
                    if old_row and str(old_row) != str(fwd_id):
                        tg_call("deleteMessage", {"chat_id": storage_id, "message_id": int(old_row)})
                    tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                except Exception as exc:
                    print(f"FILE POST UPDATE ERROR: {exc}", flush=True)
                    tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Update failed. Old file remains active."})
                    return "OK", 200
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

        # Force Join Slot Add
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

        # Force Join Slot Edit
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

    # Public Deep-Link & Default /start
    if txt.startswith("/start"):
        code = ""
        try:
            # Telegram may send /start as plain text or /start <payload>.
            # Keep parsing strict so an accidental /starter command never
            # becomes a file lookup.
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
                    # send_fj() returns True only when every active channel is
                    # already satisfied.  In that case delivery starts at once.
                    all_clear = send_fj(cid, uid, code, uname)
                    if all_clear:
                        deliver_file(cid, code)
            else:
                # Plain /start should always get a response, even if the DB is
                # temporarily unavailable.  get_setting falls back to the
                # built-in welcome text.
                # Plain /start opens the normal welcome screen with Buy Keys +
                # My Keys as ReplyKeyboard buttons.
                result = _start_main_menu(cid)
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

        # User tracking is deliberately isolated from the response path.
        if supabase and uid:
            try:
                _UPDATE_EXECUTOR.submit(_safe_run, _track_user, uid, msg["from"].get("username", ""), uname)
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

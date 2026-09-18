import os
import re
import json
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
PENDING_JOIN_REQUESTS = {}  # (user_id, normalized_chat_id) -> timestamp
# Telegram Bot API does not emit a separate event when an admin/user cancels a
# join request. Keep pending requests for a bounded period so stale/cancelled
# requests do not permanently hide a Force Join button.
PENDING_REQUEST_TTL = 10 * 60
VERIFICATION_MESSAGES = {}  # (chat_id, user_id, code) -> {base, failed, retry, anim}
VERIFICATION_LOCKS = {}

# Broadcast state. The persistent history stores the bot message IDs of the
# previous broadcast so those messages can be removed before the next one is
# delivered, even after a Render restart.
BROADCAST_HISTORY_KEY = "last_broadcast_messages_v2"
BROADCAST_ALBUM_BUFFERS = {}
BROADCAST_ALBUM_LOCK = threading.Lock()
BROADCAST_SEND_LOCK = threading.Lock()
SUPPORT_REPLY_MAP_KEY = "support_reply_map_v1"
BLOCKED_USERS_KEY = "blocked_users_v1"
SUPPORT_REPLY_MAP = {}  # admin_message_id -> user chat id
SUPPORT_REPLY_LOCK = threading.Lock()
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

# --- Admin UI state / button color system ---
ADMIN_MENU_MESSAGES = {}      # uid -> current admin menu message id
ADMIN_PROMPT_MESSAGES = {}    # uid -> current input prompt message id
BUTTON_COLORS = {}            # callback/slot key -> color marker
BUTTON_COLOR_CHOICES = {
    "red": "🔴", "green": "🟢", "blue": "🔵", "yellow": "🟡",
    "purple": "🟣", "orange": "🟠", "white": "⚪", "black": "⚫",
}
BUTTON_COLOR_LABELS = {
    "adm_home":"Admin Home","m_files":"Files","m_fj":"Force Join","m_bc":"Broadcast",
    "m_users":"Users","m_sett":"Settings","m_stats":"Statistics","f_add":"Add File",
    "f_del":"Remove File","f_edit":"Edit File","f_list":"List Files","fj_caption":"Caption",
    "fj_poster":"Poster","fj_add":"Add Channel","fj_edit":"Edit Channel","fj_onoff":"ON/OFF",
    "fj_rem":"Remove Channel","set_verify":"Verification","set_cleanup":"Cleanup","msg_retry":"Retry",
    "s_wel":"Welcome","s_err":"Error","s_warn":"Important Notice","s_timer":"Delete Timer",
    "s_upbtn":"Update Button","s_layout":"FJ Layout","s_anim":"Animation","set_grp_fj":"FJ & Media",
    "set_grp_verify":"Verification & Cleanup","set_grp_msg":"Messages & UX","set_grp_delivery":"Delivery & Links",
    "set_grp_buttons":"Buttons & Layout","set_grp_system":"Animation & System","adm_cancel":"Cancel",
}

def _color_key(callback, label=None):
    cb = str(callback or "")
    if cb.startswith(("st_","setlay_","aspeed_","vm_","act_")):
        return cb.split("_",1)[0] + "_*"
    return cb

def _button_color(callback, label=None, default=None):
    key = _color_key(callback, label)
    # Register real admin buttons lazily, including buttons added later to the
    # source. Internal color-picker callbacks are deliberately excluded.
    if label and not str(callback or "").startswith(("pickcolor:", "setcolor:", "color_menu_")):
        BUTTON_COLOR_LABELS.setdefault(key, str(label))
    if key not in BUTTON_COLORS:
        # Persist lazily in settings when available; no schema change required.
        try:
            saved = get_setting("btncolor_" + key, "")
        except Exception:
            saved = ""
        if saved in BUTTON_COLOR_CHOICES:
            BUTTON_COLORS[key] = saved
        else:
            low = (label or "").lower()
            if default:
                BUTTON_COLORS[key] = default
            elif any(x in low for x in ("cancel","back","remove","delete","off")) or "adm_cancel" in callback:
                BUTTON_COLORS[key] = "red"
            elif any(x in low for x in ("save","update","confirm","yes","on")):
                BUTTON_COLORS[key] = "green"
            else:
                BUTTON_COLORS[key] = "blue"
            try:
                set_setting("btncolor_" + key, BUTTON_COLORS[key])
            except Exception:
                pass
    return BUTTON_COLORS[key]

def _colorize_button(btn):
    """Register/persist a button color without adding a visible color emoji.

    Telegram Bot API does not expose inline-keyboard background/text colors.
    Color markers are therefore shown only inside the dedicated Button Colors
    settings screens; normal Admin Panel buttons stay clean.
    """
    b = dict(btn)
    cb = b.get("callback_data")
    if cb:
        text_value = str(b.get("text", ""))
        _button_color(cb, text_value)
    return b

def _remember_admin_menu(uid, mid):
    if uid and mid:
        old = ADMIN_MENU_MESSAGES.get(uid)
        if old and old != mid:
            tg_call("deleteMessage", {"chat_id": uid, "message_id": old})
        ADMIN_MENU_MESSAGES[uid] = mid

def _delete_admin_menu(uid, cid=None):
    mid = ADMIN_MENU_MESSAGES.pop(uid, None)
    if mid and cid:
        tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})

def _admin_prompt(uid, cid, text, step, return_cb="adm_home", extra=None):
    _delete_admin_menu(uid, cid)
    oldp = ADMIN_PROMPT_MESSAGES.pop(uid, None)
    if oldp:
        tg_call("deleteMessage", {"chat_id": cid, "message_id": oldp})
    sess = {"step": step, "return_callback": return_cb, "chat_id": cid}
    if extra: sess.update(extra)
    ADMIN_SESSIONS[uid] = sess
    kb = [[{"text":"❌ Cancel", "callback_data":"adm_cancel"}]]
    r = tg_call("sendMessage", {"chat_id":cid,"text":text,"reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
    if r.get("ok"):
        ADMIN_PROMPT_MESSAGES[uid] = r["result"].get("message_id")
    return r

def _admin_finish(uid, cid, success=True, return_cb=None):
    sess = ADMIN_SESSIONS.pop(uid, {})
    ADMIN_PROMPT_MESSAGES.pop(uid, None)
    cb = return_cb or sess.get("return_callback") or "adm_home"
    # Remove the input message and any stale menu before rebuilding the previous menu.
    pmid = sess.get("prompt_mid")
    if pmid:
        tg_call("deleteMessage", {"chat_id":cid,"message_id":pmid})
    if success:
        tg_call("sendMessage", {"chat_id":cid,"text":"✅ Success"})
    else:
        tg_call("sendMessage", {"chat_id":cid,"text":"❌ Failed"})
    show = {"adm_home":show_admin_home,"m_files":show_files_menu,"m_fj":show_fj_menu,"m_sett":show_settings_menu,"m_users":show_users_menu}.get(cb, show_admin_home)
    show(cid)

def _button_color_menu(cid, mid):
    groups = [
        ("🏠 Main Menu", ["adm_home","m_files","m_fj","m_bc","m_users","m_sett","m_stats"]),
        ("📦 Files Menu", ["f_add","f_del","f_edit","f_list"]),
        ("📢 Force Join Menu", ["fj_caption","fj_poster","fj_add","fj_edit","fj_onoff","fj_rem"]),
        ("⚙️ Settings", ["set_grp_fj","set_grp_verify","set_grp_msg","set_grp_delivery","set_grp_buttons","set_grp_system"]),
        ("🛠 Actions", ["msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"]),
    ]
    kb=[]
    for title, keys in groups:
        kb.append([{"text":title, "callback_data":"color_menu_"+title.split()[-1].lower().replace("&","")}])
    kb.append([{"text":"📢 FJ Channel Buttons", "callback_data":"color_menu_fjslots"}])
    kb.append([{"text":"🧩 All Registered Buttons", "callback_data":"color_menu_all"}])
    kb.append([{"text":"⬅️ Back", "callback_data":"m_sett"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,"text":"🎨 **BUTTON COLORS**\n\nSelect a category:","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def _color_items(cid, mid, keys, title):
    kb=[]
    for key in keys:
        label=BUTTON_COLOR_LABELS.get(key, key)
        marker=BUTTON_COLOR_CHOICES.get(_button_color(key,label),"🔵")
        kb.append([{"text":f"{marker} {label}","callback_data":"setcolor:"+key}])
    kb.append([{"text":"⬅️ Back","callback_data":"button_colors"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,"text":f"🎨 **{title}**\n\nTap a button to change its color.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def _fj_color_items(cid, mid):
    chs = get_channels() or []
    kb=[]
    for c in chs:
        slot=int(c.get("slot"))
        key=f"fj_slot_{slot}"
        label=f"Slot {slot}: {c.get('button_text','Channel')}"
        marker=BUTTON_COLOR_CHOICES.get(_button_color(key,label),"🔵")
        kb.append([{"text":f"{marker} {label}","callback_data":"setcolor:"+key}])
    kb.append([{"text":"⬅️ Back","callback_data":"button_colors"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,"text":"🎨 **FJ CHANNEL BUTTON COLORS**\n\nEach Force Join channel button can have its own color.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def _color_picker(cid, mid, key):
    current=_button_color(key, BUTTON_COLOR_LABELS.get(key,key))
    kb=[]
    for name,mark in BUTTON_COLOR_CHOICES.items():
        prefix="✅ " if name==current else ""
        kb.append([{"text":f"{prefix}{mark} {name.title()}","callback_data":f"pickcolor:{key}:{name}"}])
    kb.append([{"text":"⬅️ Back","callback_data":"button_colors"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,"text":f"🎨 **Choose Color**\n\nCurrent: {BUTTON_COLOR_CHOICES.get(current,'🔵')} {current.title()}","reply_markup":{"inline_keyboard":kb}})

def mono_admin_kb(keyboard):
    """Render Admin buttons with monospace text and the configured color marker.
    Any newly introduced callback is auto-registered with a default color.
    """
    out=[]
    for row in keyboard or []:
        nr=[]
        for btn in row or []:
            b=dict(btn)
            if "text" in b:
                b["text"] = mono(b["text"])
            if b.get("callback_data"):
                b=_colorize_button(b)
            nr.append(b)
        out.append(nr)
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
        data = dict(data or {})
        chat_id = data.get("chat_id")
        # Automatically attach Cancel to every admin input prompt.
        if method == "sendMessage" and chat_id in ADMIN_SESSIONS and not data.get("reply_markup"):
            txt = str(data.get("text", ""))
            low = txt.lower().strip()
            if not low.startswith(("success", "failed", "deleted", "removed", "error")):
                data["reply_markup"] = {"inline_keyboard": [[{"text":"❌ Cancel", "callback_data":"adm_cancel"}]]}
        # Normalize every Admin inline keyboard before it reaches Telegram.
        # This catches legacy menus and future buttons automatically.
        if chat_id in ADMIN_IDS and isinstance(data.get("reply_markup"), dict):
            markup0 = data.get("reply_markup") or {}
            if isinstance(markup0.get("inline_keyboard"), list):
                data["reply_markup"] = {
                    **markup0,
                    "inline_keyboard": mono_admin_kb(markup0.get("inline_keyboard") or [])
                }

        r = _TG_SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
            json=data, timeout=(2.5, 8)
        ).json()
        if not r.get("ok") and "parse_mode" in data:
            retry_data = dict(data)
            retry_data.pop("parse_mode", None)
            r = _TG_SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                json=retry_data, timeout=(2.5, 8)
            ).json()
        # Track the single Admin UI message automatically. This covers every
        # existing and future admin keyboard without requiring manual wiring.
        markup = data.get("reply_markup") or {}
        if r.get("ok") and chat_id in ADMIN_IDS and isinstance(markup, dict) and markup.get("inline_keyboard"):
            if method == "sendMessage":
                new_mid = r.get("result", {}).get("message_id")
                if new_mid:
                    has_cancel = any(
                        isinstance(b, dict) and b.get("callback_data") == "adm_cancel"
                        for row in markup.get("inline_keyboard", []) for b in (row or [])
                    )
                    if has_cancel:
                        old_prompt = ADMIN_PROMPT_MESSAGES.get(chat_id)
                        if old_prompt and old_prompt != new_mid:
                            tg_call("deleteMessage", {"chat_id":chat_id,"message_id":old_prompt})
                        ADMIN_PROMPT_MESSAGES[chat_id] = new_mid
                    else:
                        ADMIN_MENU_MESSAGES[chat_id] = new_mid
            elif method == "editMessageText":
                edit_mid = data.get("message_id")
                if edit_mid:
                    ADMIN_MENU_MESSAGES[chat_id] = edit_mid
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
        return False
    try:
        supabase.table("settings").upsert({"key": key, "value": str(value)}).execute()
        _SETTINGS_CACHE[str(key)] = str(value)
        return True
    except Exception as exc:
        print(f"SETTING WRITE ERROR key={key}: {exc}", flush=True)
        return False


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
    now_ts = time.time()
    # Expire local pending entries so a cancelled/handled request cannot hide
    # the channel forever. A fresh chat_join_request refreshes this timestamp.
    for key, ts in list(PENDING_JOIN_REQUESTS.items()):
        try:
            if now_ts - float(ts) > PENDING_REQUEST_TTL:
                PENDING_JOIN_REQUESTS.pop(key, None)
        except Exception:
            PENDING_JOIN_REQUESTS.pop(key, None)
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
                    status_value = str(row.get("status", "pending")).lower()
                    is_pending = status_value == "pending"
                    # New records use pending:<unix_timestamp>, allowing the bot
                    # to expire cancelled/handled requests even after restart.
                    if status_value.startswith("pending:"):
                        try:
                            request_ts = float(status_value.split(":", 1)[1])
                            is_pending = (now_ts - request_ts) <= PENDING_REQUEST_TTL
                        except Exception:
                            is_pending = False
                    elif is_pending:
                        # Legacy plain 'pending' rows have no reliable timestamp.
                        # Only the fresh in-memory event can prove they are recent.
                        raw_for_legacy = row.get("chat_id")
                        is_pending = (uid, str(raw_for_legacy)) in PENDING_JOIN_REQUESTS
                    if is_pending:
                        raw_id = row.get("chat_id")
                        if raw_id is not None:
                            pending_chat_ids.add(str(raw_id))
                            try:
                                pending_chat_ids.add(str(int(raw_id)))
                            except (TypeError, ValueError):
                                pass
            except Exception as rich_exc:
                print(f"PENDING REQUEST STATUS FALLBACK user={uid}: {rich_exc}", flush=True)
                # Legacy tables do not expose request status/timestamp. Do not
                # let an ancient row permanently hide a channel; rely on the
                # fresh in-memory chat_join_request event for legacy schemas.
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
        _button_color(f"fj_slot_{c.get('slot')}", btn_txt)
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


def _broadcast_history_load():
    try:
        raw = get_setting(BROADCAST_HISTORY_KEY, "{}")
        data = json.loads(raw) if raw else {}
        if not isinstance(data, dict):
            return {}
        out = {}
        for chat_id, mids in data.items():
            try:
                vals = [int(x) for x in (mids if isinstance(mids, list) else [mids])]
                if vals:
                    out[str(chat_id)] = vals
            except Exception:
                continue
        return out
    except Exception as exc:
        print(f"BROADCAST HISTORY LOAD ERROR: {exc}", flush=True)
        return {}


def _broadcast_history_save(history):
    try:
        return bool(set_setting(BROADCAST_HISTORY_KEY, json.dumps(history, separators=(",", ":"))))
    except Exception as exc:
        print(f"BROADCAST HISTORY SAVE ERROR: {exc}", flush=True)
        return False


def _delete_previous_broadcasts(history):
    """Delete all recorded messages from the previous broadcast, grouped per chat.

    Telegram supports deleteMessages for up to 100 message IDs from one chat at a
    time. We still verify each batch and never start the new broadcast if a batch
    fails, so the ordering remains: OLD DELETE -> NEW SEND.
    """
    if not history:
        return True, 0
    total = 0
    failed = []
    for chat_id_raw, mids in history.items():
        try:
            chat_id = int(chat_id_raw)
            ids = sorted({int(x) for x in (mids if isinstance(mids, list) else [mids])})
        except Exception:
            failed.append((chat_id_raw, mids))
            continue
        for i in range(0, len(ids), 100):
            batch = ids[i:i+100]
            r = tg_call("deleteMessages", {"chat_id": chat_id, "message_ids": batch})
            if r.get("ok"):
                total += len(batch)
                continue
            # Fallback to individual deletes so one problematic ID does not hide
            # successful deletions of the other messages in the same chat.
            for message_id in batch:
                rr = tg_call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
                if rr.get("ok"):
                    total += 1
                    continue
                desc = str(rr.get("description", "")).lower()
                # Already-gone messages are safe to treat as deleted.
                if any(x in desc for x in ("message to delete not found", "message_id_invalid", "message id invalid")):
                    total += 1
                else:
                    failed.append((chat_id, message_id, desc))
    if failed:
        print(f"BROADCAST OLD MESSAGE DELETE FAILED count={len(failed)} details={failed[:5]}", flush=True)
        return False, total
    return True, total


def _broadcast_confirm(cid, uid):
    sess = ADMIN_SESSIONS.get(uid, {})
    payload = sess.get("bc_payload") or {}
    mids = payload.get("message_ids") or []
    if not mids:
        tg_call("sendMessage", {"chat_id": cid, "text": "❌ No broadcast content recorded."})
        ADMIN_SESSIONS.pop(uid, None)
        _delete_admin_menu(uid, cid)
        show_admin_home(cid)
        return
    _delete_admin_menu(uid, cid)
    pmid = ADMIN_PROMPT_MESSAGES.pop(uid, None)
    if pmid:
        tg_call("deleteMessage", {"chat_id": cid, "message_id": pmid})
    ADMIN_SESSIONS[uid]["bc_payload"] = payload
    kb = [
        [{"text": "✅ Yes, Send Broadcast", "callback_data": "send_confirmed_bc"}],
        [{"text": "❌ Cancel", "callback_data": "adm_cancel"}]
    ]
    r = tg_call("sendMessage", {
        "chat_id": cid,
        "text": f"📣 Broadcast ready ({len(mids)} message{'s' if len(mids) != 1 else ''}).\n\nConfirm delivery?",
        "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}
    })
    if r.get("ok"):
        ADMIN_MENU_MESSAGES[uid] = r["result"].get("message_id")
        ADMIN_SESSIONS[uid]["return_callback"] = "adm_home"


def _finish_broadcast_album(uid, cid, group_id):
    # Give Telegram a short window to deliver the remaining media-group updates.
    time.sleep(1.0)
    with BROADCAST_ALBUM_LOCK:
        item = BROADCAST_ALBUM_BUFFERS.pop(uid, None)
    if not item or item.get("group_id") != group_id:
        return
    message_ids = sorted(set(int(x) for x in item.get("message_ids", [])))
    if not message_ids:
        return
    sess = ADMIN_SESSIONS.get(uid)
    if not sess or sess.get("step") != "WAIT_BC_CONTENT":
        return
    sess["bc_payload"] = {"from_chat": cid, "message_ids": message_ids, "media_group_id": group_id}
    _broadcast_confirm(cid, uid)

# --- Sub-Menu Display Functions ---
def show_admin_home(chat_id, mid=None):
    kb=[
        [_settings_button("📦 Files","m_files","mono"), _settings_button("📢 Force Join","m_fj","bold")],
        [_settings_button("📣 Broadcast","m_bc","serif"), _settings_button("👥 Users","m_users","mono")],
        [_settings_button("⚙️ Settings","m_sett","bold"), _settings_button("📊 Statistics","m_stats","serif")]
    ]
    txt="⚡ **ADMIN CONTROL**\n\nManage files, access rules, users and bot behaviour.\n\n🟢 System Online"
    data={"chat_id":chat_id,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}} if mid else {"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}}
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
        [_settings_button("🎨 Button Colors","button_colors","mono")],
        [_settings_button("⬅️ Back","adm_home","mono")]
    ]
    data={"chat_id":chat_id,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}} if mid else {"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}}
    tg_call("editMessageText" if mid else "sendMessage",data)

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
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,"text":f"{title}\n\n{desc}","parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}})

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
    tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def show_users_menu(chat_id, mid=None):
    if not supabase:
        cnt = 0
    else:
        try:
            resp = supabase.table("users").select("telegram_user_id", count="exact").execute()
            cnt = int(resp.count or 0)
        except Exception:
            cnt = 0
    blocked_count = len(_blocked_users_load()) if 'BLOCKED_USERS_KEY' in globals() else 0
    txt = f"👥 **Users Overview:**\n\n• Total Users: `{cnt}`\n• Blocked Users: `{blocked_count}`"
    kb = [
        [{"text": "📜 View All Users", "callback_data": "u_list_0"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    data={"chat_id":chat_id,"message_id":mid,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}} if mid else {"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}}
    tg_call("editMessageText" if mid else "sendMessage", data)

@app.route("/", methods=["GET"])
def home():
    return "Bot Core Online 🚀"

def process_callback_query(cq):
    uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")

    if uid in ADMIN_IDS:
        # Color settings / cancellation are handled before normal routing.
        if cdata == "button_colors":
            _button_color_menu(cid, mid); return "OK", 200
        if cdata.startswith("color_menu_"):
            k=cdata[len("color_menu_"):]
            maps={
                "menu":["adm_home","m_files","m_fj","m_bc","m_users","m_sett","m_stats"],
                "files":["f_add","f_del","f_edit","f_list"],
                "join":["fj_caption","fj_poster","fj_add","fj_edit","fj_onoff","fj_rem"],
                "settings":["set_grp_fj","set_grp_verify","set_grp_msg","set_grp_delivery","set_grp_buttons","set_grp_system"],
                "actions":["msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"]
            }
            if k=="fjslots": _fj_color_items(cid,mid)
            elif k=="all": _color_items(cid,mid,sorted(BUTTON_COLORS.keys()),"All Registered Buttons")
            elif k in maps: _color_items(cid,mid,maps[k],k.title())
            else: _button_color_menu(cid,mid)
            return "OK", 200
        if cdata.startswith("setcolor:"):
            _color_picker(cid,mid,cdata.split(":",1)[1]); return "OK", 200
        if cdata.startswith("pickcolor:"):
            _,key,color=cdata.split(":",2)
            if color in BUTTON_COLOR_CHOICES:
                BUTTON_COLORS[key]=color
                try: set_setting("btncolor_"+key,color)
                except Exception: pass
            # Return to the appropriate list.
            if key.startswith("fj_slot_"): _fj_color_items(cid,mid)
            else:
                group = "actions" if key in {"msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"} else ("files" if key in {"f_add","f_del","f_edit","f_list"} else ("join" if key.startswith("fj_") else ("settings" if key.startswith("set_grp_") else "menu")))
                maps={"menu":["adm_home","m_files","m_fj","m_bc","m_users","m_sett","m_stats"],"files":["f_add","f_del","f_edit","f_list"],"join":["fj_caption","fj_poster","fj_add","fj_edit","fj_onoff","fj_rem"],"settings":["set_grp_fj","set_grp_verify","set_grp_msg","set_grp_delivery","set_grp_buttons","set_grp_system"],"actions":["msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"]}
                _color_items(cid,mid,maps[group],group.title())
            return "OK", 200
        if cdata == "adm_cancel":
            with BROADCAST_ALBUM_LOCK:
                BROADCAST_ALBUM_BUFFERS.pop(uid, None)
            sess=ADMIN_SESSIONS.pop(uid,{})
            pmid=ADMIN_PROMPT_MESSAGES.pop(uid,None)
            if pmid: tg_call("deleteMessage", {"chat_id":cid,"message_id":pmid})
            cb=sess.get("return_callback","adm_home")
            {"adm_home":show_admin_home,"m_files":show_files_menu,"m_fj":show_fj_menu,"m_sett":show_settings_menu,"m_users":show_users_menu}.get(cb,show_admin_home)(cid)
            return "OK", 200
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
            _delete_admin_menu(uid, cid)
            oldp = ADMIN_PROMPT_MESSAGES.pop(uid, None)
            if oldp:
                tg_call("deleteMessage", {"chat_id": cid, "message_id": oldp})
            ADMIN_SESSIONS[uid] = {"step": "WAIT_BC_CONTENT", "return_callback": "adm_home", "chat_id": cid}
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
            active=len([c for c in (get_channels() or []) if c.get("active")])
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
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "F_TITLE", "return_callback": "m_files", "chat_id": cid}
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
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_TITLE", "code": cb_suffix(cdata, "ef_t_"), "return_callback":"m_files", "chat_id":cid}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter new title:"})
        elif cdata.startswith("ef_c_"):
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_CODE", "old_code": cb_suffix(cdata, "ef_c_"), "return_callback":"m_files", "chat_id":cid}
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter new short code:"})
        elif cdata.startswith("ef_p_"):
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "F_EDIT_FILE", "code": cb_suffix(cdata, "ef_p_"), "return_callback":"m_files", "chat_id":cid}
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
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_FJ_CAPTION", "return_callback": "m_fj", "chat_id": cid}
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
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_POSTER", "return_callback": "m_fj", "chat_id": cid}
            tg_call("sendMessage", {"chat_id": cid, "text": "Forward image/post directly from Storage Channel:"})

        elif cdata == "fjp_rem":
            set_setting("poster_storage_id", "")
            tg_call("sendMessage", {"chat_id": cid, "text": "Deleted"})
            show_fj_menu(cid)

        elif cdata == "fj_add":
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "FJ_ADD_TITLE", "return_callback": "m_fj", "chat_id": cid}
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
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "FJ_E_TITLE", "slot": int(cdata.split("_")[1]), "return_callback":"m_fj", "chat_id":cid}
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
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "❌ Broadcast content expired. Please start again."})
                show_admin_home(cid)
                return "OK", 200

            # Serialize broadcasts so two admin clicks can never overlap.
            if not BROADCAST_SEND_LOCK.acquire(blocking=False):
                tg_call("sendMessage", {"chat_id":cid,"text":"⏳ Another broadcast is already being processed. Please wait."})
                return "OK",200
            try:
                previous = _broadcast_history_load()
                ok, deleted_count = _delete_previous_broadcasts(previous)
                if not ok:
                    tg_call("sendMessage", {
                        "chat_id": cid,
                        "text": f"❌ Old broadcast cleanup failed ({deleted_count} deleted before failure).\nNew broadcast was NOT sent."
                    })
                    return "OK", 200
                # Keep the old history until the new history has been durably
                # written. If storage fails after delivery, we can immediately
                # remove the newly-created messages instead of losing tracking.
                message_ids = [int(x) for x in (bc_data.get("message_ids") or [])]
                if not message_ids:
                    ADMIN_SESSIONS.pop(uid, None)
                    tg_call("sendMessage", {"chat_id": cid, "text": "❌ No broadcast messages found."})
                    show_admin_home(cid)
                    return "OK", 200

                blocked=_blocked_users_load()
                users = supabase.table("users").select("telegram_user_id").execute().data or [] if supabase else []
                users=[u for u in users if int(u.get("telegram_user_id")) not in blocked]
                new_history = {}
                history_lock = threading.Lock()

                def deliver_to_user(u):
                    try:
                        target = int(u["telegram_user_id"])
                        copied=[]
                        if len(message_ids) > 1:
                            r = tg_call("copyMessages", {
                                "chat_id": target, "from_chat_id": bc_data["from_chat"], "message_ids": message_ids
                            })
                            if r.get("ok"):
                                copied=[int(x.get("message_id")) for x in (r.get("result") or []) if isinstance(x,dict) and x.get("message_id")]
                            else:
                                for source_mid in message_ids:
                                    rr=tg_call("copyMessage", {"chat_id":target,"from_chat_id":bc_data["from_chat"],"message_id":source_mid})
                                    if not rr.get("ok"):
                                        return
                                    copied.append(int(rr["result"]["message_id"]))
                        else:
                            r=tg_call("copyMessage", {"chat_id":target,"from_chat_id":bc_data["from_chat"],"message_id":message_ids[0]})
                            if not r.get("ok"):
                                return
                            copied=[int(r["result"]["message_id"])]
                        if copied:
                            with history_lock:
                                new_history[str(target)]=copied
                    except Exception as exc:
                        print(f"BROADCAST DELIVERY ERROR user={u.get('telegram_user_id')}: {exc}", flush=True)

                workers=min(8,max(1,len(users)))
                with ThreadPoolExecutor(max_workers=workers,thread_name_prefix="broadcast-send") as ex:
                    list(ex.map(deliver_to_user,users))

                if not _broadcast_history_save(new_history):
                    # Roll back the newly delivered broadcast if its cleanup
                    # history cannot be persisted. This prevents an untracked
                    # broadcast from surviving into the next send.
                    rollback_ok, _ = _delete_previous_broadcasts(new_history)
                    tg_call("sendMessage", {"chat_id":cid,"text":"❌ Broadcast history could not be saved; the new broadcast was rolled back." if rollback_ok else "❌ Broadcast history could not be saved and rollback was incomplete. Do not send another broadcast until storage is fixed."})
                    return "OK",200

                ADMIN_SESSIONS.pop(uid,None)
                pmid=ADMIN_PROMPT_MESSAGES.pop(uid,None)
                if pmid:
                    tg_call("deleteMessage", {"chat_id":cid,"message_id":pmid})
                tg_call("sendMessage", {"chat_id":cid,"text":f"✅ Broadcast sent. {len(new_history)} user(s) received it."})
                show_admin_home(cid)
            finally:
                BROADCAST_SEND_LOCK.release()

        # --- Users Sub-Module ---
        elif cdata == "u_list":
            _show_users_page(cid, mid, 0)
        elif cdata.startswith("u_list_"):
            try:
                page=int(cdata.rsplit("_",1)[1])
            except Exception:
                page=0
            _show_users_page(cid, mid, max(0,page))
        elif cdata.startswith("u_view_"):
            try:
                target=int(cdata[len("u_view_"):])
                _show_user_detail(cid, mid, target)
            except Exception:
                tg_call("sendMessage", {"chat_id":cid,"text":"❌ Invalid user ID."})
        elif cdata.startswith("u_block_") or cdata.startswith("u_unblock_"):
            action = "block" if cdata.startswith("u_block_") else "unblock"
            try:
                target=int(cdata.split("_",2)[2])
                if target in ADMIN_IDS:
                    tg_call("sendMessage", {"chat_id":cid,"text":"❌ Admin accounts cannot be blocked."})
                else:
                    ok=_set_blocked(target, action=="block")
                    tg_call("sendMessage", {"chat_id":cid,"text":("✅ User blocked." if action=="block" else "✅ User unblocked.") if ok else "❌ Could not update block status."})
                    _show_user_detail(cid, mid, target)
            except Exception:
                tg_call("sendMessage", {"chat_id":cid,"text":"❌ Invalid user ID."})

        # --- Settings Sub-Module ---
        elif cdata == "s_wel":
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_W", "return_callback": "m_sett", "chat_id": cid}
            cur = get_setting("welcome_text", DEFAULT_WELCOME)
            tg_call("sendMessage", {"chat_id": cid, "text": f"📝 CURRENT WELCOME MESSAGE:\n\n{cur}\n\n✏️ Send the new welcome message:"})
        elif cdata == "s_err":
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_E", "return_callback": "m_sett", "chat_id": cid}
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
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_WARN", "return_callback": "m_sett", "chat_id": cid}
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
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_NOTICE_URL", "return_callback": "m_sett", "chat_id": cid}
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
                _delete_admin_menu(uid, cid)
                ADMIN_SESSIONS[uid] = {"step": "SET_CUSTOM_TIMER", "return_callback": "m_sett", "chat_id": cid}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter time in seconds:"})
            else:
                set_setting("auto_delete_time", sec)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "s_upbtn":
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_UP_TITLE", "return_callback": "m_sett", "chat_id": cid}
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
                _delete_admin_menu(uid, cid)
                ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_SPEED", "return_callback": "m_sett", "chat_id": cid}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter animation speed in seconds (0.10 - 5.00):"})
            else:
                set_setting("anim_speed", speed_value)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
        elif cdata == "anim_text":
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_TEXT", "return_callback": "m_sett", "chat_id": cid}
            cur = get_setting("anim_text", DEFAULT_ANIM_TEXT)
            tg_call("sendMessage", {"chat_id": cid, "text": f"🎞 CURRENT ANIMATION TEXT:\n\n{cur}\n\n✏️ Send the new base text:"})

    return "OK", 200


def warm_caches():
    try:
        _load_settings_cache()
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
        payload = {"user_id": int(user_id), "chat_id": str(chat_id), "status": f"pending:{time.time():.3f}"}
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
            _safe_run(process_callback_query, cq)
        return "OK", 200

    # Message Router. Critical commands (/start and /admin) are processed in
    # the webhook request itself. This is intentional: Telegram must never get
    # a 200 response while a worker-queue problem prevents a visible reply.
    # Other public messages stay asynchronous for throughput.
    if "message" in data:
        msg = data.get("message", {})
        uid = msg.get("from", {}).get("id")
        txt = msg.get("text", "") or ""
        command = (txt.strip().split(maxsplit=1)[0].split("@", 1)[0].lower()
                   if txt.strip() else "")
        critical = command in ("/start", "/admin")
        try:
            if critical or uid in ADMIN_IDS:
                _safe_run(process_message_update, data)
            else:
                _PUBLIC_EXECUTOR.submit(_safe_run, process_message_update, data)
        except Exception as exc:
            print(f"UPDATE QUEUE ERROR user={uid} text={txt[:80]!r}: {exc}", flush=True)
            _safe_run(process_message_update, data)
        return "OK", 200

    return "OK", 200

def _load_json_setting(key, default):
    try:
        raw = get_setting(key, "")
        if not raw:
            return default
        value = json.loads(raw)
        return value
    except Exception:
        return default


def _blocked_users_load():
    value = _load_json_setting(BLOCKED_USERS_KEY, [])
    if not isinstance(value, list):
        return set()
    out = set()
    for x in value:
        try:
            out.add(int(x))
        except Exception:
            pass
    return out


def _blocked_users_save(users):
    return set_setting(BLOCKED_USERS_KEY, json.dumps(sorted({int(x) for x in users}), separators=(",", ":")))


def _is_blocked(uid):
    try:
        return int(uid) in _blocked_users_load()
    except Exception:
        return False


def _set_blocked(uid, blocked=True):
    try:
        uid = int(uid)
        users = _blocked_users_load()
        if blocked:
            users.add(uid)
        else:
            users.discard(uid)
        return _blocked_users_save(users)
    except Exception as exc:
        print(f"BLOCK USER ERROR user={uid}: {exc}", flush=True)
        return False


def _support_map_load():
    value = _load_json_setting(SUPPORT_REPLY_MAP_KEY, {})
    if not isinstance(value, dict):
        return {}
    out = {}
    for mid, user_id in value.items():
        try:
            out[str(int(mid))] = int(user_id)
        except Exception:
            pass
    return out


def _support_map_save(mapping):
    # Keep the persistent map bounded. Newest entries win.
    items = list(mapping.items())[-2000:]
    return set_setting(SUPPORT_REPLY_MAP_KEY, json.dumps(dict(items), separators=(",", ":")))


def _support_map_put(admin_message_id, user_id):
    try:
        key = str(int(admin_message_id))
        with SUPPORT_REPLY_LOCK:
            SUPPORT_REPLY_MAP[key] = int(user_id)
            # Persist only when possible; memory remains the fast path.
            _support_map_save(SUPPORT_REPLY_MAP)
    except Exception as exc:
        print(f"SUPPORT MAP SAVE ERROR: {exc}", flush=True)


def _support_map_get(admin_message_id):
    key = str(int(admin_message_id))
    with SUPPORT_REPLY_LOCK:
        if key in SUPPORT_REPLY_MAP:
            return SUPPORT_REPLY_MAP[key]
        loaded = _support_map_load()
        SUPPORT_REPLY_MAP.update(loaded)
        return SUPPORT_REPLY_MAP.get(key)


def _message_kind(msg):
    for key in ("text", "photo", "video", "document", "audio", "voice", "animation", "sticker", "video_note", "contact", "location", "venue", "poll", "dice"):
        if key in msg:
            return key
    return "other"


def _relay_user_message_to_admin(msg):
    if not ADMIN_IDS:
        return
    sender = msg.get("from") or {}
    uid = int(sender.get("id"))
    username = sender.get("username")
    first_name = sender.get("first_name") or ""
    last_name = sender.get("last_name") or ""
    full_name = " ".join(x for x in (first_name, last_name) if x).strip() or "User"
    lang = sender.get("language_code") or "—"
    premium = "Yes" if sender.get("is_premium") else "No"
    kind = _message_kind(msg)
    source_chat = msg.get("chat") or {}
    chat_type = source_chat.get("type") or "private"
    msg_id = msg.get("message_id")
    lines = [
        "📩 **NEW USER MESSAGE**",
        "",
        f"👤 Name: {clean_markdown(full_name)}",
        f"🔹 Username: @{clean_markdown(username)}" if username else "🔹 Username: —",
        f"🆔 Telegram ID: `{uid}`",
        f"🌐 Language: `{clean_markdown(lang)}`",
        f"⭐ Premium: `{premium}`",
        f"💬 Chat Type: `{clean_markdown(chat_type)}`",
        f"📝 Message ID: `{msg_id}`",
        f"📦 Type: `{kind}`",
    ]
    contact = msg.get("contact") or {}
    if contact.get("phone_number"):
        lines.append(f"📞 Shared phone: `{clean_markdown(contact.get('phone_number'))}`")
    text = "\n".join(lines)
    for admin_id in ADMIN_IDS:
        try:
            header = tg_call("sendMessage", {"chat_id": admin_id, "text": text, "parse_mode": "Markdown"})
            if header.get("ok"):
                _support_map_put(header["result"]["message_id"], uid)
            copied = tg_call("copyMessage", {"chat_id": admin_id, "from_chat_id": msg.get("chat", {}).get("id"), "message_id": msg_id})
            if copied.get("ok"):
                _support_map_put(copied["result"]["message_id"], uid)
            else:
                # Protected/service content may not be copyable; the admin still
                # receives the user identity and a clear failure note.
                fallback = tg_call("sendMessage", {"chat_id": admin_id, "text": f"⚠️ Message type `{kind}` could not be copied automatically. Telegram returned: {clean_markdown(copied.get('description', 'unknown error'))}", "parse_mode": "Markdown"})
                if fallback.get("ok"):
                    _support_map_put(fallback["result"]["message_id"], uid)
        except Exception as exc:
            print(f"USER RELAY ERROR user={uid} admin={admin_id}: {exc}", flush=True)


def _admin_reply_to_user(msg):
    reply = msg.get("reply_to_message") or {}
    target = _support_map_get(reply.get("message_id")) if reply else None
    if not target:
        return False
    if _is_blocked(target):
        # Still allow the admin to explicitly reply to a blocked user.
        pass
    result = tg_call("copyMessage", {
        "chat_id": int(target),
        "from_chat_id": msg.get("chat", {}).get("id"),
        "message_id": msg.get("message_id")
    })
    if not result.get("ok"):
        # Text fallback for the most common admin-reply case.
        txt = msg.get("text") or msg.get("caption")
        if txt:
            result = tg_call("sendMessage", {"chat_id": int(target), "text": txt})
    return bool(result.get("ok"))


def _user_display_name(u):
    name = " ".join(x for x in (u.get("first_name"), u.get("last_name")) if x).strip() or "User"
    username = u.get("username")
    return f"{name} (@{username})" if username else name


def _load_users(page=0, page_size=20):
    if not supabase:
        return [], 0
    try:
        rows = supabase.table("users").select("*").order("telegram_user_id").range(page*page_size, page*page_size+page_size-1).execute().data or []
        # We need one extra query for total only when pagination UI is shown.
        count_resp = supabase.table("users").select("telegram_user_id", count="exact").execute()
        total = int(count_resp.count or len(rows))
        return rows, total
    except Exception as exc:
        print(f"USER LIST ERROR: {exc}", flush=True)
        return [], 0


def process_message_update(data):
    msg = data["message"]
    cid, uid, txt = msg["chat"]["id"], msg["from"]["id"], msg.get("text", "")
    uname = msg["from"].get("first_name", "User")
    mid = msg["message_id"]

    # Track every non-admin user before any workflow branch. Blocked users are
    # completely ignored (no relay, no bot response, no broadcast delivery).
    if uid not in ADMIN_IDS and _is_blocked(uid):
        return "OK", 200
    if uid not in ADMIN_IDS:
        try:
            _UPDATE_EXECUTOR.submit(_safe_run, _track_user, uid, msg["from"].get("username", ""), msg["from"].get("first_name", "User"))
        except Exception:
            pass

        # Relay every user message EXCEPT /start and /admin commands.
        # All other commands and message types are relayed normally.
        command_for_relay = txt.strip().split(maxsplit=1)[0].split("@", 1)[0].lower() if txt.strip() else ""
        if command_for_relay not in ("/start", "/admin"):
            _relay_user_message_to_admin(msg)

    # Admin Panel Command. Some Telegram updates (photos/forwards) have no
    # text, so never index an empty split result here.
    command = txt.strip().split(maxsplit=1)[0].split("@", 1)[0].lower() if txt.strip() else ""
    if command == "/admin" and uid in ADMIN_IDS:
        ADMIN_SESSIONS.pop(uid, None)
        show_admin_home(cid)
        return "OK", 200

    # Admin support reply: reply directly to a relayed user message and the
    # bot sends only the admin's reply content back to that user.
    if uid in ADMIN_IDS and msg.get("reply_to_message"):
        if _admin_reply_to_user(msg):
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
            media_group_id = msg.get("media_group_id")
            if media_group_id:
                # Telegram delivers each album item as a separate update. Collect
                # the whole group before showing the confirmation button.
                with BROADCAST_ALBUM_LOCK:
                    item = BROADCAST_ALBUM_BUFFERS.setdefault(uid, {"group_id": str(media_group_id), "message_ids": []})
                    if item.get("group_id") != str(media_group_id):
                        item = {"group_id": str(media_group_id), "message_ids": []}
                        BROADCAST_ALBUM_BUFFERS[uid] = item
                    if mid not in item["message_ids"]:
                        item["message_ids"].append(mid)
                    first = not item.get("timer_started")
                    item["timer_started"] = True
                if first:
                    threading.Thread(target=_finish_broadcast_album, args=(uid, cid, str(media_group_id)), daemon=True).start()
                return "OK", 200

            # Single-message broadcast.
            sess["bc_payload"] = {"from_chat": cid, "message_ids": [mid]}
            _broadcast_confirm(cid, uid)

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
                welcome = get_setting("welcome_text", DEFAULT_WELCOME) or DEFAULT_WELCOME
                result = tg_call("sendMessage", {
                    "chat_id": cid,
                    "text": welcome,
                    "parse_mode": "Markdown"
                })
                if not result.get("ok"):
                    # A Markdown formatting error should never make /start look
                    # dead. Retry the exact same text without parse_mode.
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

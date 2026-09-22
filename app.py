import os
import re
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import traceback
import random
from datetime import datetime
import requests
try:
    import redis as _redis_lib
except Exception:
    _redis_lib = None
from flask import Flask, request
from supabase import create_client

app = Flask(__name__)

# Environment Configuration
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
def _parse_admin_ids():
    # Accept the current ADMIN_IDS setting plus common legacy/singular names.
    # This keeps admin access working after a deploy without exposing IDs in logs.
    raw_values = []
    for key in ("ADMIN_IDS", "ADMIN_ID", "OWNER_IDS", "OWNER_ID", "TELEGRAM_ADMIN_ID"):
        raw = os.environ.get(key, "")
        if raw:
            raw_values.extend(re.split(r"[,\s]+", raw.strip()))
    out = []
    for value in raw_values:
        try:
            if value:
                out.append(int(value))
        except (TypeError, ValueError):
            continue
    return sorted(set(out))

ADMIN_IDS = _parse_admin_ids()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
print(f"CONFIG: admin_ids_configured={bool(ADMIN_IDS)} admin_count={len(ADMIN_IDS)} supabase_configured={bool(SUPABASE_URL and SUPABASE_KEY)}", flush=True)

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        pass

# Optional Redis shared-state layer. When REDIS_URL is configured, locks/dedup,
# settings-version sync, album buffers and rate limiting become multi-worker safe.
REDIS_URL = os.environ.get("REDIS_URL", "").strip()
_REDIS = None
_REDIS_LOCK = threading.Lock()
_REDIS_DISABLED = False
_REDIS_NEXT_RETRY = 0.0

def _redis_client():
    global _REDIS, _REDIS_DISABLED, _REDIS_NEXT_RETRY
    if not REDIS_URL or _redis_lib is None:
        return None
    if _REDIS_DISABLED and time.time() < _REDIS_NEXT_RETRY:
        return None
    if _REDIS_DISABLED and time.time() >= _REDIS_NEXT_RETRY:
        _REDIS_DISABLED = False
    if _REDIS is not None:
        return _REDIS
    with _REDIS_LOCK:
        if _REDIS is not None or _REDIS_DISABLED:
            return _REDIS
        try:
            _REDIS = _redis_lib.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=0.1, socket_timeout=0.1)
            _REDIS.ping()
            _REDIS_DISABLED = False
            _REDIS_NEXT_RETRY = 0.0
        except Exception as exc:
            print(f"REDIS INIT ERROR (falling back local): {exc}", flush=True)
            _REDIS_DISABLED = True
            _REDIS_NEXT_RETRY = time.time() + 5.0
            _REDIS = None
    return _REDIS

def _redis_setnx(key, value, ttl):
    r=_redis_client()
    if not r: return None
    try:
        return bool(r.set(key, str(value), nx=True, ex=max(1,int(ttl))))
    except Exception as exc:
        print(f"REDIS SETNX ERROR: {exc}", flush=True); return None

def _redis_delete(key):
    r=_redis_client()
    if not r: return False
    try: r.delete(key); return True
    except Exception: return False

def _redis_lock_acquire(key, token, ttl=120):
    """Acquire a distributed Redis lock; return None when Redis is unavailable."""
    return _redis_setnx(key, token, ttl)

def _redis_lock_release(key, token):
    """Release only our own Redis lock token."""
    r = _redis_client()
    if not r:
        return False
    script = "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end"
    try:
        return bool(r.eval(script, 1, key, str(token)))
    except Exception as exc:
        print(f"REDIS LOCK RELEASE ERROR: {exc}", flush=True)
        return False

def _redis_rate_limit(bucket="broadcast", rate=25.0, burst=25):
    r=_redis_client()
    if not r:
        return 0.0
    # Small Redis Lua token bucket: global across all web workers/processes.
    key=f"lexi:rl:{bucket}"
    script="""local now=tonumber(ARGV[1]); local rate=tonumber(ARGV[2]); local burst=tonumber(ARGV[3]); local data=redis.call('HMGET',KEYS[1],'t','ts'); local tokens=tonumber(data[1]); local ts=tonumber(data[2]); if not tokens then tokens=burst; ts=now end; tokens=math.min(burst,tokens+(now-ts)*rate); local wait=0; if tokens>=1 then tokens=tokens-1 else wait=(1-tokens)/rate; tokens=0 end; redis.call('HMSET',KEYS[1],'t',tokens,'ts',now); redis.call('EXPIRE',KEYS[1],60); return wait"""
    try:
        return float(r.eval(script,1,key,time.time(),float(rate),int(burst)))
    except Exception as exc:
        print(f"REDIS RATE LIMIT ERROR: {exc}", flush=True); return 0.0

class _AdminSessionProxy(dict):
    """Redis-backed dict used for admin multi-step state; local fallback for single-worker mode."""
    def __init__(self, uid, data=None):
        super().__init__(data or {})
        self._uid = int(uid)
        self._syncing = False

    def _save(self):
        if self._syncing:
            return
        r = _redis_client()
        if r:
            try:
                r.set(f"lexi:admin_session:{self._uid}", json.dumps(dict(self), ensure_ascii=False), ex=3600)
            except Exception as exc:
                print(f"ADMIN SESSION REDIS SAVE ERROR: {exc}", flush=True)
                store = globals().get("ADMIN_SESSIONS")
                if isinstance(store, _AdminSessionsStore):
                    with store._lock:
                        store._local[self._uid] = dict(self)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._save()

    def __delitem__(self, key):
        super().__delitem__(key)
        self._save()

    def pop(self, key, default=None):
        if key in self:
            value = super().pop(key)
            self._save()
            return value
        return default

    def clear(self):
        super().clear()
        self._save()

class _AdminSessionsStore:
    def __init__(self):
        self._local = {}
        self._lock = threading.RLock()

    def _key(self, uid):
        return f"lexi:admin_session:{int(uid)}"

    def get(self, uid, default=None):
        uid = int(uid)
        r = _redis_client()
        if r:
            try:
                raw = r.get(self._key(uid))
                if raw:
                    return _AdminSessionProxy(uid, json.loads(raw))
                return default
            except Exception as exc:
                print(f"ADMIN SESSION REDIS GET ERROR: {exc}", flush=True)
        with self._lock:
            data = self._local.get(uid)
            return _AdminSessionProxy(uid, dict(data)) if data is not None else default

    def __getitem__(self, uid):
        value = self.get(uid, None)
        if value is None:
            raise KeyError(uid)
        return value

    def __setitem__(self, uid, value):
        uid = int(uid)
        data = dict(value)
        r = _redis_client()
        if r:
            try:
                r.set(self._key(uid), json.dumps(data, ensure_ascii=False), ex=3600)
                return
            except Exception as exc:
                print(f"ADMIN SESSION REDIS SET ERROR: {exc}", flush=True)
        with self._lock:
            self._local[uid] = data

    def pop(self, uid, default=None):
        uid = int(uid)
        current = self.get(uid, None)
        r = _redis_client()
        if r:
            try:
                r.delete(self._key(uid))
            except Exception as exc:
                print(f"ADMIN SESSION REDIS DELETE ERROR: {exc}", flush=True)
        with self._lock:
            self._local.pop(uid, None)
        return current if current is not None else default

    def __contains__(self, uid):
        uid = int(uid)
        r = _redis_client()
        if r:
            try:
                return bool(r.exists(self._key(uid)))
            except Exception as exc:
                print(f"ADMIN SESSION REDIS EXISTS ERROR: {exc}", flush=True)
        with self._lock:
            return uid in self._local

ADMIN_SESSIONS = _AdminSessionsStore()
PENDING_JOIN_REQUESTS = {}  # (user_id, normalized_chat_id) -> timestamp
# Telegram Bot API does not emit a separate event when an admin/user cancels a
# join request. Keep pending requests for a bounded period so stale/cancelled
# requests do not permanently hide a Force Join button.
PENDING_REQUEST_TTL = 10 * 60
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_MAX_DELAY = 8.0
DEFAULT_DEDUP_TTL = 15 * 60
DEFAULT_MEMBER_TIMEOUT = 3.5
DEFAULT_BROADCAST_WORKERS = 8
DEFAULT_ALBUM_WAIT = 1.0
VERIFICATION_MESSAGES = {}  # (chat_id, user_id, code) -> {base, failed, retry, anim}
VERIFICATION_LOCKS = {}

# Welcome system: persistent request state + dedicated background workers.
WELCOME_PENDING_KEY = "welcome_pending_v1"
WELCOME_CHANNELS_KEY = "welcome_channel_ids_v1"
WELCOME_LOCKS = {}
WELCOME_LOCKS_GUARD = threading.Lock()
UPDATE_DEDUP_TTL = DEFAULT_DEDUP_TTL
UPDATE_DEDUP = {}
UPDATE_DEDUP_LOCK = threading.Lock()
_WELCOME_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="welcome")
STARTED_USERS = set()
DEFAULT_WELCOME_DM = "👋 Welcome {name} to {channel_name}!"

# Broadcast state. The persistent history stores the bot message IDs of the
# previous broadcast so those messages can be removed before the next one is
# delivered, even after a Render restart.
BROADCAST_HISTORY_KEY = "broadcast_history_v3"
BROADCAST_HISTORY_MAX_RECORDS = 50
BROADCAST_LEGACY_HISTORY_KEY = "last_broadcast_messages_v2"
BROADCAST_DRAFT_KEY = "broadcast_draft_v3"
BROADCAST_ALBUM_BUFFERS = {}
BROADCAST_ALBUM_LOCK = threading.Lock()
BROADCAST_SEND_LOCK = threading.Lock()
SUPPORT_REPLY_MAP_KEY = "support_reply_map_v1"
BLOCKED_USERS_KEY = "blocked_users_v1"
SUPPORT_REPLY_MAP = {}  # admin_message_id -> user chat id
SUPPORT_REPLY_LOCK = threading.Lock()
# Persistent worker pool: background webhook jobs must not disappear with the request thread.
_UPDATE_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="update")
# Tiny dedicated pool for immediate callback acknowledgements so the Telegram
# loading spinner is never held behind other update work.
_CALLBACK_ACK_EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="callback-ack")
# Dedicated admin pool: admin navigation/settings must never wait behind public updates.
_ADMIN_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="admin")
_ADMIN_COMMAND_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="admin-command")
_BROADCAST_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="broadcast-job")
# Public /start and normal-user updates get their own pool. This prevents
# database-heavy verification/request bookkeeping from ever delaying a deep link.
_PUBLIC_EXECUTOR = ThreadPoolExecutor(max_workers=32, thread_name_prefix="public")
# Verification can legitimately wait on Telegram/Supabase. Keep it isolated so
# slow verification jobs can never starve admin commands or normal messages.
_VERIFY_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="verify")
# Reuse membership-check workers instead of creating a new thread pool for every Verify click.
_MEMBER_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="member")
WEBHOOK_LAST_RECEIVED_AT = 0.0

# Reuse HTTP connections and cache read-heavy config so Telegram/Supabase latency
# does not repeat on every message. Writes update the cache immediately.
from requests.adapters import HTTPAdapter
_TG_SESSION = requests.Session()
_TG_SESSION.mount("https://", HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0))
_TG_SESSION.headers.update({"Connection": "keep-alive"})
_SETTINGS_CACHE = {}
_SETTINGS_CACHE_READY = False
_SETTINGS_CACHE_LOCK = threading.Lock()
_CHANNELS_CACHE = None
_CHANNELS_CACHE_READY = False
_CHANNELS_CACHE_LOADED_AT = 0.0
_CHANNELS_CACHE_TTL = 30.0
_CHANNELS_CACHE_LOCK = threading.Lock()
_FJ_SLOTS_ENSURED = False
_FJ_SLOTS_ENSURE_LOCK = threading.Lock()
FJ_SLOT_COUNT = 20
FJ_SLOT5_TITLE = "𝙵𝙴𝙴𝙳𝙱𝙰𝙲𝙺𝚂"
FJ_SLOT5_URL = "https://t.me/LexxiMods"
FJ_SLOT5_CHAT_ID = -1003016135737

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

# --- Telegram button style system ---
# Bot API 10.3 supports three real button background styles: primary (blue),
# success (green), and danger (red). Older arbitrary color names are migrated
# automatically to the closest valid style so the settings screen never shows
# options Telegram cannot actually render.
ADMIN_MENU_MESSAGES = {}      # uid -> current admin menu message id
ADMIN_PROMPT_MESSAGES = {}    # uid -> current input prompt message id
BUTTON_STYLES = {}            # callback/slot key -> primary/success/danger
BUTTON_STYLE_CHOICES = {
    "primary": "🔵 Primary",
    "success": "🟢 Success",
    "danger": "🔴 Danger",
}
BUTTON_STYLE_LABELS = {
    "adm_home":"Admin Home","m_files":"Files","m_fj":"Force Join","m_bc":"Broadcast",
    "m_users":"Users","m_sett":"Settings","m_stats":"Statistics","f_add":"Add File",
    "f_del":"Remove File","f_edit":"Edit File","f_list":"List Files","fj_caption":"Caption",
    "fj_poster":"Poster","fj_edit":"Configure Slots","fj_onoff":"ON/OFF",
    "fj_rem":"Remove Channel","set_verify":"Verification","set_cleanup":"Cleanup","msg_retry":"Retry",
    "s_wel":"Welcome","s_err":"Error","s_warn":"Important Notice","s_timer":"Delete Timer",
    "s_upbtn":"Update Button","s_layout":"FJ Layout","s_anim":"Animation","set_grp_fj":"FJ & Media",
    "set_grp_verify":"Verification & Cleanup","set_grp_msg":"Messages & UX","set_grp_delivery":"Delivery & Links",
    "set_grp_buttons":"Buttons & Layout","set_grp_system":"Animation & System","set_grp_reliability":"Reliability & Speed","adm_cancel":"Cancel",
}
# Backward compatibility for settings saved by the old color picker.
_LEGACY_STYLE_MAP = {
    "red":"danger", "green":"success", "blue":"primary",
    "yellow":"primary", "purple":"primary", "orange":"primary",
    "white":"primary", "black":"primary",
}

def _button_key(callback):
    cb = str(callback or "")
    if cb.startswith(("st_","setlay_","aspeed_","vm_","act_")):
        return cb.split("_",1)[0] + "_*"
    return cb

def _default_button_style(callback, label=""):
    """Pick a semantic Telegram button style; never invent custom colors."""
    cb = str(callback or "").lower()
    text = str(label or "").lower()
    combined = f"{cb} {text}"
    # Destructive / exit actions.
    if any(x in combined for x in (
        "delete", "remove", "cancel", "danger", "block", "unblock", "off", "disable",
    )):
        return "danger"
    # Positive / commit actions.
    if any(x in combined for x in (
        "confirm", "save", "yes", "verify", "generate", "purchase", "buy", "pay",
        "success", "enable", " on", "add file", "add channel", "update",
    )):
        return "success"
    # Neutral navigation / main actions.
    return "primary"

def _button_style(callback, label=None, default=None):
    key = _button_key(callback)
    if label and not str(callback or "").startswith(("pickcolor:", "setcolor:", "pickstyle:", "setstyle:", "color_menu_")):
        BUTTON_STYLE_LABELS.setdefault(key, str(label))
    if key not in BUTTON_STYLES:
        saved = _SETTINGS_CACHE.get("btnstyle_" + key, "") if _SETTINGS_CACHE_READY else ""
        if saved not in BUTTON_STYLE_CHOICES:
            # Migrate old btncolor_* values and then persist a valid Bot API style.
            old = _SETTINGS_CACHE.get("btncolor_" + key, "") if _SETTINGS_CACHE_READY else ""
            saved = _LEGACY_STYLE_MAP.get(str(old).lower(), "")
        style = saved if saved in BUTTON_STYLE_CHOICES else (default or _default_button_style(callback, label))
        # Defaults are intentionally memory-only: writing every auto-discovered
        # button to Supabase would add DB latency to normal keyboard rendering.
        # Explicit admin choices are persisted in the pickcolor handler below.
        BUTTON_STYLES[key] = style
    return BUTTON_STYLES[key]

def _style_keyboard(keyboard):
    """Apply real Telegram button styles to inline/reply keyboard buttons."""
    out=[]
    for row in keyboard or []:
        nr=[]
        for btn in row or []:
            if not isinstance(btn, dict):
                nr.append(btn)
                continue
            b=dict(btn)
            if "text" in b:
                cb = b.get("callback_data") or b.get("url") or b.get("switch_inline_query") or b.get("switch_inline_query_current_chat") or b.get("copy_text") or ""
                # Respect an explicitly supplied valid style.
                if b.get("style") not in BUTTON_STYLE_CHOICES:
                    b["style"] = _button_style(cb, b.get("text"))
            nr.append(b)
        out.append(nr)
    return out

def _color_key(callback, label=None):
    # Compatibility alias for any older code that still references _color_key.
    return _button_key(callback)

def _button_color(callback, label=None, default=None):
    # Compatibility alias: old callers now return the real Telegram style.
    return _button_style(callback, label, _LEGACY_STYLE_MAP.get(default, default))

def _colorize_button(btn):
    b=dict(btn)
    if b.get("callback_data") or b.get("url") or b.get("copy_text"):
        b["style"] = _button_style(
            b.get("callback_data") or b.get("url") or b.get("copy_text"),
            b.get("text")
        )
    return b

def mono_admin_kb(keyboard):
    """Render Admin buttons with monospace labels and real Telegram styles."""
    out=[]
    for row in keyboard or []:
        nr=[]
        for btn in row or []:
            b=dict(btn)
            if "text" in b:
                b["text"] = mono(b["text"])
            if b.get("callback_data") or b.get("url") or b.get("copy_text"):
                b=_colorize_button(b)
            nr.append(b)
        out.append(nr)
    return out

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
        ("🏠 Main Menu", "menu", ["adm_home","m_files","m_fj","m_bc","m_users","m_sett","m_stats"]),
        ("📦 Files Menu", "files", ["f_add","f_del","f_edit","f_list"]),
        ("📢 Force Join Menu", "join", ["fj_caption","fj_poster","fj_edit","fj_onoff","fj_rem"]),
        ("⚙️ Settings", "settings", ["set_grp_fj","set_grp_verify","set_grp_msg","set_grp_delivery","set_grp_buttons","set_grp_system","set_grp_reliability"]),
        ("🛠 Actions", "actions", ["msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"]),
    ]
    kb=[]
    for title, key, _ in groups:
        kb.append([{"text":title, "callback_data":"color_menu_"+key}])
    kb.append([{"text":"📢 FJ Channel Buttons", "callback_data":"color_menu_fjslots"}])
    kb.append([{"text":"🧩 All Registered Buttons", "callback_data":"color_menu_all"}])
    kb.append([{"text":"⬅️ Back", "callback_data":"m_sett"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,
        "text":"🎨 **BUTTON COLORS / STYLES**\n\nTelegram supports 3 real button colors:\n🔵 Primary = normal/navigation\n🟢 Success = confirm/positive\n🔴 Danger = delete/cancel/destructive\n\nSelect a category to change any button.",
        "parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def _color_items(cid, mid, keys, title):
    kb=[]
    for key in keys:
        label=BUTTON_STYLE_LABELS.get(key, key)
        style=_button_style(key,label)
        kb.append([{"text":f"{BUTTON_STYLE_CHOICES.get(style,'🔵 Primary')}  {label}","callback_data":"setcolor:"+key}])
    kb.append([{"text":"⬅️ Back", "callback_data":"button_colors"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,
        "text":f"🎨 **{title}**\n\nTap the exact button name below to change its Telegram color.",
        "parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def _fj_color_items(cid, mid):
    chs = get_channels() or []
    kb=[]
    if not chs:
        kb.append([{"text":"No Force Join channels configured","callback_data":"button_colors"}])
    for c in chs:
        slot=int(c.get("slot"))
        key=f"fj_slot_{slot}"
        label=f"Slot {slot}: {c.get('button_text','Channel')}"
        style=_button_style(key,label)
        kb.append([{"text":f"{BUTTON_STYLE_CHOICES.get(style,'🔵 Primary')}  {label}","callback_data":"setcolor:"+key}])
    kb.append([{"text":"⬅️ Back", "callback_data":"button_colors"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,
        "text":"🎨 **FJ CHANNEL BUTTON COLORS**\n\nEach channel is shown by its exact Slot + button title.",
        "parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def _color_picker(cid, mid, key):
    label=BUTTON_STYLE_LABELS.get(key,key)
    current=_button_style(key,label)
    kb=[]
    for name,display in BUTTON_STYLE_CHOICES.items():
        prefix="✅ " if name==current else ""
        kb.append([{"text":f"{prefix}{display}","callback_data":f"pickcolor:{key}:{name}"}])
    kb.append([{"text":"⬅️ Back", "callback_data":"button_colors"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,
        "text":f"🎨 **{label}**\n\nCurrent: {BUTTON_STYLE_CHOICES.get(current,'🔵 Primary')}\n\nChoose the real Telegram button color:",
        "parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})

def _iter_user_rows(page_size=500):
    """Stream users with keyset pagination; avoids deep OFFSET scans."""
    if not supabase:
        return
    size=max(100,min(int(page_size),1000))
    last_id=None
    while True:
        try:
            q=supabase.table("users").select("telegram_user_id").order("telegram_user_id")
            if last_id is not None:
                q=q.gt("telegram_user_id", last_id)
            rows=(q.limit(size).execute().data or [])
        except Exception as exc:
            print(f"USER PAGINATION ERROR last_id={last_id}: {exc}",flush=True)
            return
        if not rows:
            return
        for row in rows:
            if row.get("telegram_user_id") is not None:
                yield row
        if len(rows)<size:
            return
        try:
            last_id=int(rows[-1]["telegram_user_id"])
        except Exception:
            return

def _iter_user_ids(page_size=500):
    for row in _iter_user_rows(page_size):
        try: yield int(row["telegram_user_id"])
        except Exception: continue

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

def _telegram_retryable(result):
    code = int((result or {}).get("error_code", 0) or 0)
    return code in (429, 500, 502, 503, 504)

def _telegram_retry_after(result):
    try:
        return max(0.05, min(float(((result or {}).get("parameters") or {}).get("retry_after", 0)), 30.0))
    except Exception:
        return 0.25

def tg_call(method, data):
    data = dict(data or {})
    chat_id = data.get("chat_id")
    if method == "sendMessage" and chat_id in ADMIN_SESSIONS and not data.get("reply_markup"):
        txt = str(data.get("text", "")); low = txt.lower().strip()
        if not low.startswith(("success", "failed", "deleted", "removed", "error")):
            data["reply_markup"] = {"inline_keyboard": [[{"text":"❌ Cancel", "callback_data":"adm_cancel"}]]}
    if isinstance(data.get("reply_markup"), dict):
        markup0 = data.get("reply_markup") or {}
        if isinstance(markup0.get("inline_keyboard"), list):
            kb = markup0.get("inline_keyboard") or []
            data["reply_markup"] = {**markup0, "inline_keyboard": mono_admin_kb(kb) if chat_id in ADMIN_IDS else _style_keyboard(kb)}
    last = {}
    # Telegram hot path: never synchronously query Supabase for retry settings.
    # If Supabase is slow/unavailable, /admin and normal replies must still send.
    try:
        max_attempts = max(1, min(int(float(_SETTINGS_CACHE.get("telegram_retry_attempts", str(DEFAULT_RETRY_ATTEMPTS)))), 8))
    except Exception:
        max_attempts = DEFAULT_RETRY_ATTEMPTS
    try:
        max_delay = max(0.5, min(float(_SETTINGS_CACHE.get("telegram_retry_max_delay", str(DEFAULT_RETRY_MAX_DELAY))), 30.0))
    except Exception:
        max_delay = DEFAULT_RETRY_MAX_DELAY
    for attempt in range(max_attempts):
        try:
            resp = _TG_SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=(0.8, 4.0))
            try: r = resp.json()
            except Exception: r = {}
            last = r
            if r.get("ok") or not _telegram_retryable(r):
                desc = str(r.get("description", "")).lower()
                # Telegram can reject an edit because the old admin message is
                # stale/deleted/not editable. Never leave the admin UI silent:
                # retry the same content as a fresh message.
                if (method == "editMessageText" and not r.get("ok") and int(r.get("error_code", 0) or 0) == 400):
                    if "message is not modified" in desc:
                        return r
                    if any(x in desc for x in ("message to edit not found", "message can't be edited", "message cannot be edited", "message identifier is not specified")):
                        fallback = dict(data)
                        fallback.pop("message_id", None)
                        fallback.pop("inline_message_id", None)
                        fallback["chat_id"] = chat_id
                        try:
                            fr = _TG_SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=fallback, timeout=(0.8, 4.0)).json()
                            if fr.get("ok"):
                                new_mid = (fr.get("result") or {}).get("message_id")
                                if chat_id in ADMIN_IDS and new_mid:
                                    ADMIN_MENU_MESSAGES[chat_id] = new_mid
                            return fr
                        except Exception as exc:
                            print(f"TELEGRAM EDIT FALLBACK ERROR: {exc}", flush=True)
                if (not r.get("ok") and int(r.get("error_code",0) or 0)==400 and any(x in desc for x in ("parse", "markdown", "entities", "entity"))):
                    # Never leave a message permanently unsent because a saved
                    # native Telegram entity became invalid after an edit.
                    # Retry once as plain text/caption while preserving buttons.
                    retry_data = dict(data)
                    retry_data.pop("parse_mode", None)
                    retry_data.pop("entities", None)
                    retry_data.pop("caption_entities", None)
                    try: return _TG_SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=retry_data, timeout=(0.8,4.0)).json()
                    except Exception: return {}
                if not r.get("ok"):
                    print(f"TELEGRAM API REJECT method={method} code={r.get('error_code')} desc={r.get('description')}", flush=True)
                break
            delay = _telegram_retry_after(r) if int(r.get("error_code",0) or 0)==429 else min(max_delay, (0.15 * (2 ** attempt) + random.random()*0.05))
            time.sleep(min(max_delay, delay))
        except (requests.RequestException, ValueError) as exc:
            if attempt >= 3:
                print(f"TELEGRAM API ERROR method={method}: {exc}", flush=True); return {}
            time.sleep(min(max_delay, 0.15 * (2 ** attempt) + random.random()*0.05))
        except Exception as exc:
            print(f"TELEGRAM API ERROR method={method}: {exc}", flush=True); return {}
    r = last
    markup = data.get("reply_markup") or {}
    if r.get("ok") and chat_id in ADMIN_IDS and isinstance(markup, dict) and markup.get("inline_keyboard"):
        if method == "sendMessage":
            new_mid = (r.get("result") or {}).get("message_id")
            if new_mid:
                has_cancel = any(isinstance(b, dict) and b.get("callback_data")=="adm_cancel" for row in markup.get("inline_keyboard",[]) for b in (row or []))
                if has_cancel:
                    old_prompt = ADMIN_PROMPT_MESSAGES.get(chat_id)
                    if old_prompt and old_prompt != new_mid: tg_call("deleteMessage", {"chat_id":chat_id,"message_id":old_prompt})
                    ADMIN_PROMPT_MESSAGES[chat_id] = new_mid
                else: ADMIN_MENU_MESSAGES[chat_id] = new_mid
        elif method == "editMessageText" and data.get("message_id"):
            ADMIN_MENU_MESSAGES[chat_id] = data.get("message_id")
    return r

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


_SETTINGS_VERSION_CHECK_AT = 0.0
_SETTINGS_VERSION_SEEN = ""

def _sync_settings_version():
    global _SETTINGS_VERSION_CHECK_AT, _SETTINGS_VERSION_SEEN, _SETTINGS_CACHE_READY
    now=time.time()
    if now-_SETTINGS_VERSION_CHECK_AT < 2.0:
        return
    _SETTINGS_VERSION_CHECK_AT=now
    r=_redis_client()
    if not r: return
    try:
        version=str(r.get("lexi:settings:version") or "")
        if version and _SETTINGS_VERSION_SEEN and version != _SETTINGS_VERSION_SEEN:
            with _SETTINGS_CACHE_LOCK:
                _SETTINGS_CACHE_READY=False
        if version:
            _SETTINGS_VERSION_SEEN=version
    except Exception:
        pass

def _utf16_len(value):
    return len(str(value).encode("utf-16-le")) // 2


def _save_message_setting(key, text, entities=None):
    """Persist a configurable Telegram message plus its native entities.

    Telegram Premium/custom emoji are represented by `custom_emoji` entities,
    not by the visible Unicode character alone. Keeping the original entity
    list means admins can paste Premium emojis directly into any supported
    message field without converting them to plain text.
    """
    set_setting(key, str(text))
    try:
        clean = [dict(e) for e in (entities or []) if isinstance(e, dict)]
        set_setting(key + "__entities", json.dumps(clean, ensure_ascii=False, separators=(",", ":")))
    except Exception:
        set_setting(key + "__entities", "[]")


def _load_message_setting(key, default=""):
    text = get_setting(key, default)
    raw = get_setting(key + "__entities", "[]")
    try:
        entities = json.loads(raw) if raw else []
        if not isinstance(entities, list):
            entities = []
        entities = [dict(e) for e in entities if isinstance(e, dict)]
    except Exception:
        entities = []
    return str(text), entities


def _render_message_setting(key, default="", replacements=None):
    """Render a saved message and keep Telegram entity offsets valid.

    Entity offsets are UTF-16 based in the Telegram Bot API. This helper shifts
    offsets when dynamic placeholders such as {name}, {time}, etc. are replaced.
    """
    text, entities = _load_message_setting(key, default)
    replacements = replacements or {}
    if not replacements or not entities:
        if replacements:
            for old, new in replacements.items():
                text = text.replace(str(old), str(new))
        return text, entities

    original = text
    occurrences = []
    for old, new in replacements.items():
        old = str(old); new = str(new)
        if not old:
            continue
        start = 0
        while True:
            pos = original.find(old, start)
            if pos < 0:
                break
            occurrences.append((pos, len(old), len(new)))
            start = pos + len(old)
    occurrences.sort(key=lambda x: x[0])

    rendered = original
    # Apply right-to-left so occurrence positions stay stable.
    for pos, old_len, new_len in reversed(occurrences):
        # Find the replacement text using the original substring/key.
        old = original[pos:pos + old_len]
        replacement = None
        for k, v in replacements.items():
            if str(k) == old:
                replacement = str(v)
                break
        if replacement is not None:
            rendered = rendered[:pos] + replacement + rendered[pos + old_len:]

    # Build UTF-16 position deltas from original character positions.
    occ_utf16 = []
    for pos, old_len, new_len in occurrences:
        delta = _utf16_len(original[pos:pos + old_len])
        new_delta = _utf16_len(original[pos:pos + old_len])
        replacement_text = original[pos:pos + old_len]
        for k, v in replacements.items():
            if str(k) == replacement_text:
                new_delta = _utf16_len(str(v))
                break
        occ_utf16.append((_utf16_len(original[:pos]), delta, new_delta))

    shifted = []
    for entity in entities:
        try:
            start = int(entity.get("offset", 0)); length = int(entity.get("length", 0))
        except Exception:
            shifted.append(entity); continue
        end = start + length
        delta_before = 0
        delta_inside = 0
        for pos_u16, old_u16, new_u16 in occ_utf16:
            d = new_u16 - old_u16
            if pos_u16 < start:
                delta_before += d
            elif pos_u16 < end:
                delta_inside += d
        e = dict(entity)
        e["offset"] = max(0, start + delta_before)
        e["length"] = max(0, length + delta_inside)
        shifted.append(e)
    return rendered, shifted


def _message_payload(key, default="", replacements=None, field="text", parse_mode="Markdown"):
    text, entities = _render_message_setting(key, default, replacements)
    payload = {field: text}
    if entities:
        payload["caption_entities" if field == "caption" else "entities"] = entities
    else:
        payload["parse_mode"] = parse_mode
    return payload

def get_setting(key, default=""):
    _sync_settings_version()
    _load_settings_cache()
    return _SETTINGS_CACHE.get(key, default)


def set_setting(key, value):
    if not supabase:
        return False
    try:
        attempts = max(1, min(int(float(_SETTINGS_CACHE.get("supabase_retry_attempts", "3"))), 6))
    except Exception:
        attempts = 3
    for attempt in range(attempts):
        try:
            supabase.table("settings").upsert({"key": key, "value": str(value)}).execute()
            _SETTINGS_CACHE[str(key)] = str(value)
            r = _redis_client()
            if r:
                try: r.incr("lexi:settings:version")
                except Exception: pass
            return True
        except Exception as exc:
            if attempt >= attempts - 1:
                print(f"SETTING WRITE ERROR key={key}: {exc}", flush=True)
                return False
            time.sleep(min(2.0, 0.15 * (2 ** attempt) + random.random()*0.05))
    return False


def _resolve_public_chat_id(username):
    """Resolve a public Telegram username to its numeric chat id."""
    try:
        handle = str(username or "").strip()
        if not handle:
            return None
        if not handle.startswith("@"):
            handle = "@" + handle
        res = tg_call("getChat", {"chat_id": handle})
        if res.get("ok") and res.get("result", {}).get("id") is not None:
            return int(res["result"]["id"])
    except Exception as exc:
        print(f"FJ CHAT RESOLVE ERROR username={username}: {exc}", flush=True)
    return None

def _ensure_force_join_slots():
    """Ensure the fixed 20-slot Force Join model exists, once per process.

    The database is only touched when the model has not yet been materialized
    in this process. Admin navigation uses the channels cache and never calls
    this database bootstrap on every button press. If the DB was temporarily
    unavailable, the next call retries automatically.
    """
    global _FJ_SLOTS_ENSURED
    if not supabase:
        return []
    if _FJ_SLOTS_ENSURED:
        return get_channels() or []
    with _FJ_SLOTS_ENSURE_LOCK:
        if _FJ_SLOTS_ENSURED:
            return get_channels() or []
        rows = []
        try:
            rows = supabase.table("force_join_channels").select("*").order("slot").execute().data or []
            by_slot = {int(r.get("slot")): r for r in rows if r.get("slot") is not None}
            missing = []
            for slot in range(1, FJ_SLOT_COUNT + 1):
                if slot in by_slot:
                    continue
                missing.append({
                    "slot": slot,
                    "button_text": FJ_SLOT5_TITLE if slot == 5 else (FJ_SLOT6_TITLE if slot == 6 else ""),
                    "join_url": FJ_SLOT5_URL if slot == 5 else (FJ_SLOT6_URL if slot == 6 else ""),
                    "chat_id": FJ_SLOT5_CHAT_ID if slot == 5 else None,
                    "active": True if slot == 5 else False,
                })
            if missing:
                supabase.table("force_join_channels").insert(missing).execute()
                rows = supabase.table("force_join_channels").select("*").order("slot").execute().data or []
            if all(any(int(r.get("slot")) == slot for r in rows if r.get("slot") is not None) for slot in range(1, FJ_SLOT_COUNT + 1)):
                _FJ_SLOTS_ENSURED = True
            invalidate_channels_cache()
            return rows
        except Exception as exc:
            print(f"FJ SLOT ENSURE ERROR: {exc}", flush=True)
            return rows

def _fj_slot_rows():
    rows = _ensure_force_join_slots()
    by_slot = {int(r.get("slot")): r for r in rows if r.get("slot") is not None}
    return [by_slot.get(i, {"slot": i, "button_text": "", "join_url": "", "chat_id": None, "active": False}) for i in range(1, FJ_SLOT_COUNT + 1)]

def _fj_slot_configured(row):
    return bool(row and str(row.get("button_text") or "").strip() and str(row.get("join_url") or "").strip() and row.get("chat_id") is not None)

def invalidate_channels_cache():
    global _CHANNELS_CACHE, _CHANNELS_CACHE_READY, _CHANNELS_CACHE_LOADED_AT
    with _CHANNELS_CACHE_LOCK:
        _CHANNELS_CACHE = None
        _CHANNELS_CACHE_READY = False
        _CHANNELS_CACHE_LOADED_AT = 0.0

def get_channels():
    global _CHANNELS_CACHE, _CHANNELS_CACHE_READY, _CHANNELS_CACHE_LOADED_AT
    now = time.time()
    if _CHANNELS_CACHE_READY and (now - _CHANNELS_CACHE_LOADED_AT) < _CHANNELS_CACHE_TTL:
        return list(_CHANNELS_CACHE or [])
    if not supabase:
        return []
    with _CHANNELS_CACHE_LOCK:
        now = time.time()
        if _CHANNELS_CACHE_READY and (now - _CHANNELS_CACHE_LOADED_AT) < _CHANNELS_CACHE_TTL:
            return list(_CHANNELS_CACHE or [])
        try:
            r = supabase.table("force_join_channels").select("*").order("slot").execute()
            _CHANNELS_CACHE = r.data or []
            _CHANNELS_CACHE_READY = True
            _CHANNELS_CACHE_LOADED_AT = time.time()
        except Exception as exc:
            print(f"CHANNELS CACHE LOAD ERROR: {exc}", flush=True)
            _CHANNELS_CACHE = None
            _CHANNELS_CACHE_READY = False
            _CHANNELS_CACHE_LOADED_AT = 0.0
            return None
        return list(_CHANNELS_CACHE or [])

def get_unjoined(user_id):
    """Return active channels still not joined/requested by this exact user.

    Verification semantics follow the configured method: joined, request, or both.
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
    try:
        pending_ttl = max(60.0, min(float(get_setting("pending_request_ttl", str(PENDING_REQUEST_TTL))), 86400.0))
    except Exception:
        pending_ttl = PENDING_REQUEST_TTL
    # Expire local pending entries so a cancelled/handled request cannot hide
    # the channel forever. A fresh chat_join_request refreshes this timestamp.
    for key, ts in list(PENDING_JOIN_REQUESTS.items()):
        try:
            if now_ts - float(ts) > pending_ttl:
                PENDING_JOIN_REQUESTS.pop(key, None)
        except Exception:
            PENDING_JOIN_REQUESTS.pop(key, None)
    pending_chat_ids = {str(chat) for (pending_uid, chat), ts in PENDING_JOIN_REQUESTS.items()
                        if pending_uid == uid}
    method = get_setting("verification_method", "joined").lower()
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
                            is_pending = (now_ts - request_ts) <= pending_ttl
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

        # A pending request is NOT membership. The user must actually be approved
        # and become a member before this channel is considered complete.
        if method == "request":
            return None if normalized_chat_id in pending_chat_ids else c
        if method in ("both", "joined_or_request") and normalized_chat_id in pending_chat_ids:
            return None
        try:
            res = tg_call("getChatMember", {"chat_id": chat_id, "user_id": uid})
        except Exception as exc:
            print(f"MEMBERSHIP CHECK ERROR user={uid} chat={normalized_chat_id}: {exc}", flush=True)
            res = {}

        status = res.get("result", {}).get("status") if res.get("ok") else None
        if res.get("ok") and (
            status in {"member", "administrator", "creator"}
            or (status == "restricted" and bool(res.get("result", {}).get("is_member")))
        ):
            return None
        if not res.get("ok"):
            # Fail-safe: an unavailable/misconfigured Force Join channel must NOT
            # be treated as satisfied. Keep it in the unjoined list so the user
            # cannot bypass verification merely because Telegram returned an API error.
            print(f"FORCE JOIN CHECK FAILED chat={normalized_chat_id}: {res.get('description')}", flush=True)
            return c

        # Pending requests were already loaded in one DB query above.
        # Do not make another Supabase request per channel on every Verify.
        return c

    # Telegram membership checks are independent, so run them concurrently.
    # Persistent worker pool avoids thread creation overhead on every Verify.
    results = list(_MEMBER_EXECUTOR.map(check_channel, active_channels))
    return [c for c in results if c is not None]

def _fj_keyboard(unjoined, code):
    layout = max(1, min(int(get_setting("button_layout", "2")), 4))
    kb, row = [], []
    for c in unjoined:
        btn_txt = c.get("button_text", "Channel").replace("↗", "").replace("↗️", "").strip()
        style = _button_style(f"fj_slot_{c.get('slot')}", btn_txt)
        row.append({"text": btn_txt, "url": c.get("join_url"), "style": style})
        if len(row) == layout:
            kb.append(row); row = []
    if row: kb.append(row)
    # Reserved Slot 6: WhatsApp Channel link only. Telegram cannot verify
    # WhatsApp subscriptions, so this button is never part of get_unjoined().
    kb.append([{"text": FJ_SLOT6_TITLE, "url": FJ_SLOT6_URL}])
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
    fj_text, fj_entities = _load_message_setting("force_join_text", DEFAULT_FJ)
    # Native Telegram entities (including Premium/custom emoji) do not use
    # Markdown parsing, so dynamic names must remain raw in entity mode.
    fj_name = name if fj_entities else clean_markdown(name)
    caption, caption_entities = _render_message_setting("force_join_text", DEFAULT_FJ, {"{name}": fj_name})
    kb = _fj_keyboard(unjoined, code)
    poster_storage_id = get_setting("poster_storage_id", "")
    storage_id = get_setting("storage_channel_id", DEFAULT_STORAGE)
    if poster_storage_id:
        fj_payload = {"chat_id": chat_id, "from_chat_id": storage_id, "message_id": int(poster_storage_id), "caption": caption, "reply_markup": {"inline_keyboard": kb}}
        if caption_entities: fj_payload["caption_entities"] = caption_entities
        else: fj_payload["parse_mode"] = "Markdown"
        res=tg_call("copyMessage", fj_payload)
    else:
        fj_payload = {"chat_id": chat_id, "text": caption, "reply_markup": {"inline_keyboard": kb}}
        if caption_entities: fj_payload["entities"] = caption_entities
        else: fj_payload["parse_mode"] = "Markdown"
        res=tg_call("sendMessage", fj_payload)
    if res.get("ok"):
        _track_verification(chat_id,user_id,code,base=res["result"]["message_id"])
    return False

BACKGROUND_JOBS_TABLE = "background_jobs"

def _queue_job(kind, payload, run_at=None, job_id=None):
    run_at=float(run_at if run_at is not None else time.time())
    job_id=str(job_id or f"{kind}:{int(time.time()*1000)}:{random.randint(1000,9999)}")
    if supabase:
        try:
            supabase.table(BACKGROUND_JOBS_TABLE).upsert({
                "job_id":job_id,"kind":kind,"payload":payload,"run_at":run_at,
                "status":"pending","attempts":0,"updated_at":time.time()
            },on_conflict="job_id").execute()
            return True
        except Exception as exc:
            print(f"JOB QUEUE FALLBACK {kind}: {exc}",flush=True)
    return False

def _run_background_job(job):
    kind=str(job.get("kind") or "")
    payload=job.get("payload") or {}
    if kind=="delete_messages":
        chat_id=payload.get("chat_id")
        failures=[]
        for m in payload.get("message_ids") or []:
            if not m: continue
            r=tg_call("deleteMessage", {"chat_id":chat_id,"message_id":m})
            if not r.get("ok"):
                desc=str(r.get("description") or "delete failed").lower()
                if not any(x in desc for x in ("message to delete not found","message_id_invalid","message id invalid","message not found")):
                    failures.append(desc)
        return not failures
    if kind=="welcome_dm":
        key=_welcome_key(payload.get("user_id"),payload.get("chat_id"))
        lock=_welcome_lock_for(key)
        with lock:
            current=_welcome_pending_get(key) or payload
            if current.get("welcome_sent_at"): return True
        ok,detail=_welcome_send(current, force_chat_id=current.get("user_chat_id") if current.get("status")=="pending" else None)
        with lock:
            current=_welcome_pending_get(key) or payload
            current["welcome_attempt_at"]=time.time(); current["welcome_detail"]=detail
            if ok: current["welcome_sent_at"]=time.time(); current["status"]="done"
            _welcome_pending_upsert(key,current)
        return bool(ok)
    return False

def _background_job_poller():
    while True:
        time.sleep(2.0)
        if not supabase: continue
        try:
            stale_before = time.time() - 180.0
            supabase.table(BACKGROUND_JOBS_TABLE).update({"status": "pending", "updated_at": time.time()})\
                .eq("status", "processing").lt("updated_at", stale_before).execute()
            now=time.time()
            failed_rows=(supabase.table(BACKGROUND_JOBS_TABLE).select("*").eq("status","failed")
                         .lt("updated_at", now-30).limit(50).execute().data or [])
            for fr in failed_rows:
                attempts=int(fr.get("attempts") or 0)
                if attempts < 5:
                    delay=min(300, 2 ** max(0, attempts))
                    supabase.table(BACKGROUND_JOBS_TABLE).update({"status":"pending","run_at":now+delay,"updated_at":now}).eq("job_id",fr.get("job_id")).eq("status","failed").execute()
            rows=(supabase.table(BACKGROUND_JOBS_TABLE).select("*").eq("status","pending")
                  .lte("run_at",now).order("run_at").limit(25).execute().data or [])
        except Exception:
            continue
        for job in rows:
            jid=job.get("job_id")
            if not jid: continue
            claimed=False
            r=_redis_client()
            if r:
                claimed=bool(_redis_setnx(f"lexi:job:{jid}",os.environ.get("HOSTNAME","worker"),120))
                if not claimed:
                    continue
                try:
                    claim_resp=(supabase.table(BACKGROUND_JOBS_TABLE)
                                .update({"status":"processing","updated_at":time.time()})
                                .eq("job_id",jid).eq("status","pending")
                                .select("job_id").execute())
                    if not (claim_resp.data or []):
                        continue
                except Exception:
                    _redis_delete(f"lexi:job:{jid}")
                    continue
            else:
                # No Redis: atomically claim in Supabase so multiple workers
                # cannot execute the same pending job concurrently.
                try:
                    claim_resp=(supabase.table(BACKGROUND_JOBS_TABLE)
                                .update({"status":"processing","updated_at":time.time()})
                                .eq("job_id",jid).eq("status","pending")
                                .select("job_id").execute())
                    if not (claim_resp.data or []):
                        continue
                    claimed=True
                except Exception:
                    continue
            try:
                ok=_run_background_job(job)
                if ok:
                    supabase.table(BACKGROUND_JOBS_TABLE).update({"status":"done","updated_at":time.time()}).eq("job_id",jid).execute()
                else:
                    supabase.table(BACKGROUND_JOBS_TABLE).update({"status":"failed","attempts":int(job.get("attempts") or 0)+1,"updated_at":time.time()}).eq("job_id",jid).execute()
            except Exception as exc:
                try: supabase.table(BACKGROUND_JOBS_TABLE).update({"status":"failed","attempts":int(job.get("attempts") or 0)+1,"updated_at":time.time()}).eq("job_id",jid).execute()
                except Exception: pass

try:
    threading.Thread(target=_background_job_poller,daemon=True,name="job-poller").start()
except Exception: pass

def auto_del_task(chat_id, mids, delay):
    if _queue_job("delete_messages", {"chat_id":chat_id,"message_ids":[m for m in mids if m]}, time.time()+max(0,float(delay))):
        return
    def fallback():
        time.sleep(max(0,float(delay)))
        for m in mids:
            if m: tg_call("deleteMessage", {"chat_id":chat_id,"message_id":m})
    threading.Thread(target=fallback,daemon=True,name="delete-fallback").start()

def _valid_storage_forward(fwd_chat):
    """Require forwarded admin content to originate from the configured storage chat."""
    if not isinstance(fwd_chat, dict) or fwd_chat.get("id") is None:
        return False
    configured=str(get_setting("storage_channel_id", DEFAULT_STORAGE)).strip()
    return str(fwd_chat.get("id")) == configured


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
        tg_call("sendMessage", {"chat_id": chat_id, **_message_payload("error_feedback_text", DEFAULT_ERROR)})
        return

    storage = get_setting("storage_channel_id", DEFAULT_STORAGE)
    sender_mode = app_data.get("sender_mode", "copy")

    if sender_mode == "forward":
        delivery = tg_call("forwardMessage", {"chat_id": chat_id, "from_chat_id": storage, "message_id": app_data["storage_message_id"]})
    else:
        delivery = tg_call("copyMessage", {"chat_id": chat_id, "from_chat_id": storage, "message_id": app_data["storage_message_id"]})

    if not delivery.get("ok"):
        tg_call("sendMessage", {"chat_id": chat_id, **_message_payload("error_feedback_text", DEFAULT_ERROR)})
        return

    mids = [delivery.get("result", {}).get("message_id")]

    try:
        del_seconds = max(1, min(int(float(get_setting("auto_delete_time", "120"))), 86400))
    except Exception:
        del_seconds = 120
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
            warn_msg, warn_entities = _render_message_setting("auto_delete_warn_text", DEFAULT_WARN, {"{time}": time_str})

        notice_btn_enabled = notice_enabled and get_setting("notice_btn_enabled", "true").lower() == "true"
        btn_txt = get_setting("notice_btn_text", get_setting("update_btn_text", DEFAULT_NOTICE_BTN_TEXT))
        if not btn_txt or btn_txt.lower() == "none":
            btn_txt = get_setting("update_btn_text", "none")
        btn_url = get_setting("notice_btn_url", get_setting("update_btn_url", DEFAULT_NOTICE_BTN_URL))
        kb = [[{"text": btn_txt, "url": btn_url}]] if (notice_btn_enabled and btn_txt and btn_url and btn_txt.lower() != "none") else []
        w = {}
        if warn_msg and cleanup_notice:
            warn_payload = {"chat_id": chat_id, "text": warn_msg, "reply_markup": {"inline_keyboard": kb} if kb else None}
            if warn_entities: warn_payload["entities"] = warn_entities
            else: warn_payload["parse_mode"] = "Markdown"
            w = tg_call("sendMessage", warn_payload)
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
def run_verify_process(chat_id, user_id, code, fj_mid, user_name="User"):
    """Verify all configured channels while the optional animation runs.

    There is deliberately no artificial post-verification sleep: the file is
    delivered as soon as verification finishes and the animation message is
    stopped/cleaned up. A Redis lock prevents duplicate verification across
    multiple workers/processes, with the existing local lock as fallback.
    """
    key = (chat_id, user_id, code)
    redis_lock_key = f"lexi:verify:{chat_id}:{user_id}:{code}"
    redis_token = f"{os.getpid()}:{threading.get_ident()}:{time.time_ns()}"
    redis_lock = _redis_lock_acquire(redis_lock_key, redis_token, ttl=180)
    if redis_lock is False:
        return

    local_lock = None
    if redis_lock is None:
        local_lock = VERIFICATION_LOCKS.setdefault(key, threading.Lock())
        if not local_lock.acquire(blocking=False):
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
        base, anim_entities = _load_message_setting("anim_text", DEFAULT_ANIM_TEXT)
        base = base.strip() or "Verifying"
        frames = [f"{base}.", f"{base}..", f"{base}..."]
        stop = threading.Event()
        anim_state = {"mid": None}

        def animate():
            if stop.is_set():
                return
            anim_payload = {"chat_id": chat_id, "text": frames[0]}
            if anim_entities: anim_payload["entities"] = anim_entities
            r = tg_call("sendMessage", anim_payload)
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
                anim_edit = {"chat_id": chat_id, "message_id": anim_state["mid"], "text": frames[idx]}
                if anim_entities: anim_edit["entities"] = anim_entities
                r = tg_call("editMessageText", anim_edit)
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
            **_message_payload("verify_retry_text", DEFAULT_VERIFY_RETRY)
        })
        if r.get("ok"):
            _track_verification(chat_id, user_id, code, retry=r["result"]["message_id"])

        # send_fj() performs a fresh membership check and builds buttons only
        # for the remaining channels. It also tracks the new FJ message.
        send_fj(chat_id, user_id, code, user_name)
    finally:
        if local_lock is not None:
            local_lock.release()
            VERIFICATION_LOCKS.pop(key, None)
        elif redis_lock:
            _redis_lock_release(redis_lock_key, redis_token)


def _broadcast_delivery_claim(broadcast_id, user_id):
    """Claim a broadcast recipient with crash recovery.

    New claims are marked ``sending`` with a timestamp. A stale sending/pending
    claim can be atomically reclaimed after 10 minutes; this prevents a worker
    crash between claim and Telegram delivery from permanently skipping a user.
    """
    if not supabase:
        return False, "Supabase is unavailable"
    bid=str(broadcast_id); uid=int(user_id); now=time.time(); cutoff=now-600
    try:
        supabase.table("broadcast_deliveries").insert({
            "broadcast_id":bid,"user_id":uid,"status":"sending",
            "claimed_at":time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        }).execute()
        return True,None
    except Exception as exc:
        msg=str(exc); low=msg.lower()
        if not ("duplicate" in low or "unique" in low or "23505" in low or "already exists" in low):
            return False,msg
        try:
            stale=(supabase.table("broadcast_deliveries").select("status,claimed_at,created_at")
                   .eq("broadcast_id",bid).eq("user_id",uid).limit(1).execute().data or [])
            if not stale: return False,"delivery claim disappeared"
            row=stale[0]; status=str(row.get("status") or "pending")
            if status=="sent": return False,None
            stamp=row.get("claimed_at") or row.get("created_at")
            stale_ts=0.0
            if stamp:
                try:
                    stale_ts=datetime.fromisoformat(str(stamp).replace("Z","+00:00")).timestamp()
                except Exception:
                    stale_ts=0.0
            if stale_ts and stale_ts >= cutoff: return False,None
            claim_stamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff))
            base=supabase.table("broadcast_deliveries").update({
                "status":"sending","claimed_at":time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),"sent_at":None
            }).eq("broadcast_id",bid).eq("user_id",uid)
            if status == "pending":
                upd=base.eq("status","pending").lt("created_at",claim_stamp).select("broadcast_id").execute()
            else:
                upd=base.eq("status","sending").lt("claimed_at",claim_stamp).select("broadcast_id").execute()
            return (bool(upd.data), None if upd.data else None)
        except Exception as reclaim_exc:
            print(f"BROADCAST DELIVERY RECLAIM ERROR user={uid}: {reclaim_exc}",flush=True)
            return False,str(reclaim_exc)


def _broadcast_delivery_finish(broadcast_id, user_id, status):
    """Persist the final delivery state without affecting the user table."""
    if not supabase:
        return False
    try:
        supabase.table("broadcast_deliveries").update({
            "status": str(status),
            "sent_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) if status == "sent" else None,
        }).eq("broadcast_id", str(broadcast_id)).eq("user_id", int(user_id)).execute()
        return True
    except Exception as exc:
        print(f"BROADCAST DELIVERY STATE ERROR user={user_id}: {exc}", flush=True)
        return False


def _broadcast_history_load():
    """Load persistent broadcast records.

    v3 stores a list of broadcasts, each containing target-chat -> message IDs.
    The old v2 dict is migrated in-memory as one legacy broadcast so an upgrade
    does not silently lose the last tracked broadcast.
    """
    try:
        raw = get_setting(BROADCAST_HISTORY_KEY, "")
        if raw:
            data = json.loads(raw)
            if isinstance(data, list):
                records = []
                for rec in data:
                    if not isinstance(rec, dict):
                        continue
                    messages = rec.get("messages") or {}
                    normalized = {}
                    if isinstance(messages, dict):
                        for chat_id, mids in messages.items():
                            try:
                                vals = [int(x) for x in (mids if isinstance(mids, list) else [mids])]
                                if vals:
                                    normalized[str(chat_id)] = sorted(set(vals))
                            except Exception:
                                continue
                    if normalized:
                        records.append({
                            "broadcast_id": str(rec.get("broadcast_id") or int(time.time() * 1000)),
                            "created_at": int(rec.get("created_at") or 0),
                            "messages": normalized,
                        })
                return records

        # One-time migration from the old {chat_id: [message_ids]} format.
        legacy_raw = get_setting(BROADCAST_LEGACY_HISTORY_KEY, "")
        if legacy_raw:
            legacy = json.loads(legacy_raw)
            if isinstance(legacy, dict):
                normalized = {}
                for chat_id, mids in legacy.items():
                    try:
                        vals = [int(x) for x in (mids if isinstance(mids, list) else [mids])]
                        if vals:
                            normalized[str(chat_id)] = sorted(set(vals))
                    except Exception:
                        continue
                if normalized:
                    return [{"broadcast_id": "legacy-v2", "created_at": 0, "messages": normalized}]
        return []
    except Exception as exc:
        print(f"BROADCAST HISTORY LOAD ERROR: {exc}", flush=True)
        return []


def _broadcast_history_save(records):
    try:
        clean = []
        for rec in records or []:
            if not isinstance(rec, dict):
                continue
            messages = rec.get("messages") or {}
            normalized = {}
            for chat_id, mids in messages.items() if isinstance(messages, dict) else []:
                try:
                    vals = [int(x) for x in (mids if isinstance(mids, list) else [mids])]
                    if vals:
                        normalized[str(chat_id)] = sorted(set(vals))
                except Exception:
                    continue
            if normalized:
                clean.append({
                    "broadcast_id": str(rec.get("broadcast_id") or int(time.time() * 1000)),
                    "created_at": int(rec.get("created_at") or 0),
                    "messages": normalized,
                })
        clean = clean[-BROADCAST_HISTORY_MAX_RECORDS:]
        return bool(set_setting(BROADCAST_HISTORY_KEY, json.dumps(clean, separators=(",", ":"))))
    except Exception as exc:
        print(f"BROADCAST HISTORY SAVE ERROR: {exc}", flush=True)
        return False


def _broadcast_draft_load():
    try:
        raw = get_setting(BROADCAST_DRAFT_KEY, "")
        data = json.loads(raw) if raw else {}
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        print(f"BROADCAST DRAFT LOAD ERROR: {exc}", flush=True)
        return {}


def _broadcast_draft_save(draft):
    try:
        return bool(set_setting(BROADCAST_DRAFT_KEY, json.dumps(draft or {}, separators=(",", ":"))))
    except Exception as exc:
        print(f"BROADCAST DRAFT SAVE ERROR: {exc}", flush=True)
        return False


def _broadcast_draft_clear():
    try:
        return _broadcast_draft_save({})
    except Exception:
        return False


def _broadcast_delete_messages(messages):
    """Delete one broadcast record and return exact deletion diagnostics.

    We intentionally use individual deleteMessage calls after a batch attempt
    whenever the batch fails, because Telegram's deleteMessages can return true
    while silently skipping messages that cannot be found. Individual calls give
    the admin a real Telegram error for anything that remains.
    """
    deleted = 0
    failed = []
    for chat_id_raw, mids in (messages or {}).items():
        try:
            chat_id = int(chat_id_raw)
            ids = sorted({int(x) for x in (mids if isinstance(mids, list) else [mids])})
        except Exception as exc:
            failed.append({"chat_id": str(chat_id_raw), "message_id": None, "error": f"invalid history: {exc}"})
            continue
        if not ids:
            continue

        # Fast path for up to 100 messages in the same chat.
        r = tg_call("deleteMessages", {"chat_id": chat_id, "message_ids": ids[:100]})
        if r.get("ok"):
            deleted += len(ids[:100])
            remaining = ids[100:]
        else:
            remaining = ids

        # Any batch remainder, or any failed batch, is checked individually so
        # the result is deterministic and the exact Telegram error is captured.
        for message_id in remaining:
            rr = tg_call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
            if rr.get("ok"):
                deleted += 1
                continue
            desc = str(rr.get("description", "Telegram delete failed")).strip()
            low = desc.lower()
            if any(x in low for x in (
                "message to delete not found", "message_id_invalid", "message id invalid",
                "message not found", "message is not modified"
            )):
                # Already gone is success for cleanup purposes.
                deleted += 1
            else:
                failed.append({"chat_id": str(chat_id), "message_id": message_id, "error": desc})
    return deleted, failed


def _message_fingerprint(msg):
    """Build a conservative fingerprint for a bot-authored personal message."""
    if not isinstance(msg, dict):
        return None
    kind = "text"
    media = None
    for key in ("photo", "video", "document", "audio", "voice", "animation", "sticker", "video_note"):
        if key in msg:
            kind = key
            value = msg.get(key)
            if isinstance(value, list) and value:
                value = value[-1]
            if isinstance(value, dict):
                media = value.get("file_unique_id") or value.get("file_id")
            break
    text = msg.get("text") or msg.get("caption") or ""
    return (kind, str(media or ""), str(text))


def _legacy_broadcast_sweep(records, users):
    """Best-effort cleanup for broadcasts created before message-ID tracking.

    Bot API 10.3 exposes getUserPersonalChatMessages (last 1-20 messages). If a
    tracked current broadcast exists, use its actual message fingerprints to find
    older identical bot messages left by an older version of the bot.
    """
    if not records or not users:
        return 0, []
    latest = records[-1]
    sample_chat = None
    sample_mids = []
    for chat_id, mids in (latest.get("messages") or {}).items():
        if mids:
            sample_chat = int(chat_id)
            sample_mids = [int(x) for x in mids]
            break
    if sample_chat is None or not sample_mids:
        return 0, []

    sample = tg_call("getUserPersonalChatMessages", {"user_id": sample_chat, "limit": 20})
    if not sample.get("ok"):
        return 0, []
    sample_msgs = sample.get("result") or []
    by_id = {int(m.get("message_id", -1)): m for m in sample_msgs if m.get("message_id") is not None}
    fingerprints = set()
    target_date = 0
    for mid in sample_mids:
        msg = by_id.get(mid)
        if msg:
            fp = _message_fingerprint(msg)
            if fp:
                fingerprints.add(fp)
            target_date = max(target_date, int(msg.get("date") or 0))
    if not fingerprints:
        return 0, []

    deleted = 0
    errors = []
    for u in users:
        try:
            uid = int(u.get("telegram_user_id"))
            history = tg_call("getUserPersonalChatMessages", {"user_id": uid, "limit": 20})
            if not history.get("ok"):
                continue
            for msg in history.get("result") or []:
                sender = msg.get("from") or {}
                if not sender.get("is_bot"):
                    continue
                if int(msg.get("date") or 0) >= target_date:
                    continue
                if _message_fingerprint(msg) not in fingerprints:
                    continue
                rr = tg_call("deleteMessage", {"chat_id": uid, "message_id": int(msg.get("message_id"))})
                if rr.get("ok"):
                    deleted += 1
                else:
                    errors.append((uid, msg.get("message_id"), rr.get("description", "delete failed")))
        except Exception as exc:
            errors.append((u.get("telegram_user_id"), None, str(exc)))
    return deleted, errors

def _broadcast_delete_records(records, mode):
    """Delete selected previous broadcasts and return (ok, new_records, stats)."""
    records = list(records or [])
    if not records:
        return True, [], {"broadcasts": 0, "deleted": 0, "failed": []}
    selected = records if mode == "all" else records[-1:]
    remaining_records = records if mode != "all" else []
    if mode != "all":
        remaining_records = records[:-1]

    total_deleted = 0
    all_failed = []
    successful_ids = []
    for rec in selected:
        deleted, failed = _broadcast_delete_messages(rec.get("messages") or {})
        total_deleted += deleted
        all_failed.extend(failed)
        if not failed:
            successful_ids.append(rec.get("broadcast_id"))
        else:
            # Keep a failed record in storage so the next cleanup can retry it.
            remaining_records.append(rec)

    # Preserve original chronological order after a failed LAST-1 cleanup.
    remaining_records.sort(key=lambda x: int(x.get("created_at") or 0))
    ok = not all_failed
    return ok, remaining_records, {
        "broadcasts": len(selected),
        "deleted": total_deleted,
        "failed": all_failed,
        "successful_ids": successful_ids,
    }


def _broadcast_cleanup_prompt(cid, uid):
    records = _broadcast_history_load()
    if not records:
        ADMIN_SESSIONS[uid] = {"step": "WAIT_BC_CONTENT", "return_callback": "adm_home", "chat_id": cid}
        _broadcast_draft_clear()
        tg_call("sendMessage", {"chat_id": cid, "text": "📭 No tracked previous broadcast found.\n\n📨 Now send or forward the post you want to broadcast:"})
        return
    kb = [
        [{"text": "🗑 Delete LAST 1 Previous Broadcast", "callback_data": "bc_cleanup_last"}],
        [{"text": "🧹 Delete ALL Previous Broadcasts", "callback_data": "bc_cleanup_all"}],
        [{"text": "❌ Cancel", "callback_data": "adm_cancel"}],
    ]
    ADMIN_SESSIONS[uid] = {"step": "BC_CLEANUP_CHOICE", "return_callback": "adm_home", "chat_id": cid}
    tg_call("sendMessage", {
        "chat_id": cid,
        "text": f"📢 **Broadcast Cleanup First**\n\nTracked previous broadcasts: `{len(records)}`\n\nSelect what must be deleted BEFORE you send the new broadcast:",
        "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": mono_admin_kb(kb)},
    })


def _broadcast_request_content(cid, uid):
    ADMIN_SESSIONS[uid] = {"step": "WAIT_BC_CONTENT", "return_callback": "adm_home", "chat_id": cid}
    tg_call("sendMessage", {
        "chat_id": cid,
        "text": "✅ Previous broadcast cleanup completed.\n\n📨 Now send or forward the post you want to broadcast:"
    })


def _broadcast_confirm(cid, uid):
    sess = ADMIN_SESSIONS.get(uid, {})
    payload = sess.get("bc_payload") or _broadcast_draft_load()
    mids = payload.get("message_ids") or []
    if not mids:
        tg_call("sendMessage", {"chat_id": cid, "text": "❌ No broadcast content recorded. Please send the post again."})
        ADMIN_SESSIONS.pop(uid, None)
        _broadcast_draft_clear()
        show_admin_home(cid)
        return
    ADMIN_SESSIONS[uid]["bc_payload"] = payload
    _broadcast_draft_save(payload)
    _delete_admin_menu(uid, cid)
    pmid = ADMIN_PROMPT_MESSAGES.pop(uid, None)
    if pmid:
        tg_call("deleteMessage", {"chat_id": cid, "message_id": pmid})
    kb = [
        [{"text": "✅ Yes, Send Broadcast", "callback_data": "send_confirmed_bc"}],
        [{"text": "❌ Cancel", "callback_data": "adm_cancel"}]
    ]
    r = tg_call("sendMessage", {
        "chat_id": cid,
        "text": f"📣 Broadcast ready ({len(mids)} message{'s' if len(mids) != 1 else ''}).\n\nPrevious cleanup is already complete.\n\nConfirm delivery?",
        "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}
    })
    if r.get("ok"):
        ADMIN_MENU_MESSAGES[uid] = r["result"].get("message_id")
        ADMIN_SESSIONS[uid]["return_callback"] = "adm_home"


def _finish_broadcast_album(uid, cid, group_id):
    # Give Telegram a short window to deliver the remaining media-group updates.
    try:
        album_wait = max(0.2, min(float(get_setting("broadcast_album_wait", str(DEFAULT_ALBUM_WAIT))), 5.0))
    except Exception:
        album_wait = DEFAULT_ALBUM_WAIT
    time.sleep(album_wait)
    item=None
    r=_redis_client()
    if r:
        try:
            raw=r.get(f"lexi:album:{uid}:{group_id}")
            if raw: item=json.loads(raw); r.delete(f"lexi:album:{uid}:{group_id}")
        except Exception: pass
    if item is None:
        with BROADCAST_ALBUM_LOCK:
            item = BROADCAST_ALBUM_BUFFERS.pop((uid, str(group_id)), None)
    if not item or item.get("group_id") != group_id:
        return
    message_ids = sorted(set(int(x) for x in item.get("message_ids", [])))
    if not message_ids:
        return
    sess = ADMIN_SESSIONS.get(uid)
    if not sess or sess.get("step") != "WAIT_BC_CONTENT":
        return
    payload = {"from_chat": cid, "message_ids": message_ids, "media_group_id": group_id, "created_at": int(time.time())}
    sess["bc_payload"] = payload
    _broadcast_draft_save(payload)
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
    # Slots are materialized during startup/warmup; navigation uses the cache only.
    kb = [
        [{"text": "📝 "+mono("Caption"), "callback_data": "fj_caption"}, {"text": "🖼 "+mono("Poster (Add/Remove)"), "callback_data": "fj_poster"}],
        [{"text": "⚙️ "+mono("CONFIGURE SLOTS"), "callback_data": "fj_edit"}, {"text": "🔘 "+mono("ON/OFF SLOTS"), "callback_data": "fj_onoff"}],
        [{"text": "🗑 "+mono("RESET SLOTS"), "callback_data": "fj_rem"}],
        [{"text": "⬅️ "+mono("Back"), "callback_data": "adm_home"}]
    ]
    txt = "📢 **Force Join Configuration:**\n\n20 fixed slots are available. Configure any slot independently."
    payload={"chat_id":chat_id,"text":txt,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}}
    if mid:
        payload["message_id"]=mid
        tg_call("editMessageText", payload)
    else:
        tg_call("sendMessage", payload)

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
        [_settings_button("🎨 Button Colors & Styles","button_colors","mono")],
        [_settings_button("🛡 Reliability & Speed","set_grp_reliability","mono")],
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
        "reliability": ("🛡 RELIABILITY & SPEED", "Retries, deduplication, worker tuning and health settings."),
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
        kb=[[{"text":"👋 "+mono("Welcome DM"),"callback_data":"s_wel"},{"text":"⚠️ "+mono("Error"),"callback_data":"s_err"}],
            [{"text":"🔄 "+mono("Retry"),"callback_data":"msg_retry"}],
            [{"text":"⚠️ "+mono("Important Notice"),"callback_data":"s_warn"}]]
    elif group == "delivery":
        kb=[[{"text":"⏱ "+mono("Delete Timer"),"callback_data":"s_timer"},{"text":"📟 "+mono("Update Button"),"callback_data":"s_upbtn"}],
            [{"text":"⚠️ "+mono("Important Notice"),"callback_data":"s_warn"}],
            [{"text":"🔗 "+mono("JOIN NOW Link"),"callback_data":"notice_btn_edit"}]]
    elif group == "buttons":
        kb=[[{"text":"🔘 "+mono("Force Join Layout"),"callback_data":"s_layout"}],
            [{"text":"👁 "+mono("Preview Layout"),"callback_data":"anim_preview"}]]
    elif group == "reliability":
        kb=[[{"text":"🔁 "+mono("Telegram Retry"),"callback_data":"rel_retry"},{"text":"🗄 "+mono("Supabase Retry"),"callback_data":"rel_db"}],
            [{"text":"⚡ "+mono("Worker Settings"),"callback_data":"rel_workers"}],
            [{"text":"🧹 "+mono("State Cleanup"),"callback_data":"rel_cleanup"}],
            [{"text":"📢 "+mono("Broadcast Limits"),"callback_data":"rel_broadcast"}],
            [{"text":"❤️ "+mono("Health Status"),"callback_data":"rel_health"}]]
    else:
        kb=[[{"text":"🎞 "+mono("Animation"),"callback_data":"set_anim"}],
            [{"text":"🛠 "+mono("System Info"),"callback_data":"set_system"}]]
    kb.append([{"text":"⬅️ "+mono("Back to Settings"),"callback_data":"m_sett"}])
    tg_call("editMessageText", {"chat_id":cid,"message_id":mid,"text":f"{title}\n\n{desc}","parse_mode":"Markdown","reply_markup":{"inline_keyboard":kb}})

def settings_submenu(cid,mid,kind):
    def status(k,default="true"): return "ON" if get_setting(k,default).lower()=="true" else "OFF"
    if kind=="verify":
        method = get_setting("verification_method", "joined").lower()
        method_label = {
            "joined": "JOINED — actual membership required",
            "request": "REQUEST — pending request counts",
            "both": "BOTH — membership or approved request state",
            "joined_or_request": "BOTH — membership or request state",
        }.get(method, "JOINED — actual membership required")
        txt=f"✅ **VERIFICATION**\n\nStatus: `{status('verification_enabled')}`\nMethod: `{method_label}`\n\nJoin Request is not membership in JOINED mode."
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
        kb=[[{"text":"👋 "+mono("Welcome DM"),"callback_data":"s_wel"},{"text":"⚠️ "+mono("Error"),"callback_data":"s_err"}],
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
                "join":["fj_caption","fj_poster","fj_edit","fj_onoff","fj_rem"],
                "settings":["set_grp_fj","set_grp_verify","set_grp_msg","set_grp_delivery","set_grp_buttons","set_grp_system","set_grp_reliability"],
                "actions":["msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"]
            }
            if k=="fjslots": _fj_color_items(cid,mid)
            elif k=="all": _color_items(cid,mid,sorted(BUTTON_STYLE_LABELS.keys()),"All Registered Buttons")
            elif k in maps: _color_items(cid,mid,maps[k],k.title())
            else: _button_color_menu(cid,mid)
            return "OK", 200
        if cdata.startswith("setcolor:"):
            _color_picker(cid,mid,cdata.split(":",1)[1]); return "OK", 200
        if cdata.startswith("pickcolor:"):
            _,key,style=cdata.split(":",2)
            if style in BUTTON_STYLE_CHOICES:
                BUTTON_STYLES[key]=style
                try: set_setting("btnstyle_"+key,style)
                except Exception: pass
            # Return to the exact category containing this button.
            if key.startswith("fj_slot_"): _fj_color_items(cid,mid)
            else:
                group = "actions" if key in {"msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"} else ("files" if key in {"f_add","f_del","f_edit","f_list"} else ("join" if key.startswith("fj_") else ("settings" if key.startswith("set_grp_") else "menu")))
                maps={"menu":["adm_home","m_files","m_fj","m_bc","m_users","m_sett","m_stats"],"files":["f_add","f_del","f_edit","f_list"],"join":["fj_caption","fj_poster","fj_edit","fj_onoff","fj_rem"],"settings":["set_grp_fj","set_grp_verify","set_grp_msg","set_grp_delivery","set_grp_buttons","set_grp_system","set_grp_reliability"],"actions":["msg_retry","s_wel","s_err","s_warn","s_timer","s_upbtn","s_layout","s_anim"]}
                _color_items(cid,mid,maps[group],group.title())
            return "OK", 200
        if cdata == "adm_cancel":
            with BROADCAST_ALBUM_LOCK:
                for _k in [k for k in BROADCAST_ALBUM_BUFFERS if isinstance(k, tuple) and k[0] == uid]:
                    BROADCAST_ALBUM_BUFFERS.pop(_k, None)
            sess=ADMIN_SESSIONS.pop(uid,{})
            if str(sess.get("step", "")).startswith(("WAIT_BC_", "BC_CLEANUP")):
                _broadcast_draft_clear()
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
            _broadcast_cleanup_prompt(cid, uid)
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
        elif cdata == "set_grp_reliability":
            show_settings_group(cid,mid,"reliability")
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
            kb=[[{"text":"👤 Joined Only (Recommended)","callback_data":"vm_join"}],[{"text":"⭐ BOTH (legacy)","callback_data":"vm_both"}],[{"text":"📨 Request Only","callback_data":"vm_req"}],[{"text":"⬅️ Back","callback_data":"set_verify"}]]
            current_method = get_setting("verification_method", "joined").lower()
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"🔍 **VERIFICATION METHOD**\n\nCurrent: `{current_method}`\n\nChoose how a user completes Force Join:","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
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

        elif cdata == "fj_edit":
            slots = _fj_slot_rows()
            btns = []
            for i in range(0, FJ_SLOT_COUNT, 2):
                row=[]
                for slot in (i+1, i+2):
                    if slot <= FJ_SLOT_COUNT:
                        row.append({"text": mono(f"Slot {slot}"), "callback_data": f"fje_{slot}"})
                btns.append(row)
            btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}])
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select Force Join slot to configure:", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("fje_"):
            slot = int(cdata.split("_")[1])
            if slot < 1 or slot > FJ_SLOT_COUNT:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Invalid slot."})
                return "OK", 200
            row = next((x for x in _fj_slot_rows() if int(x.get("slot")) == slot), None)
            state = "ON" if row and row.get("active") else "OFF"
            details = ""
            if row and row.get("active"):
                details = f"\n\nCurrent title: {row.get('button_text') or '-'}\nCurrent URL: {row.get('join_url') or '-'}\nCurrent Chat ID: {row.get('chat_id') or '-'}"
            kb = [
                [{"text": "✏️ "+mono("Set / Replace Settings"), "callback_data": f"fjcfg_{slot}"}],
                [{"text": "🟢 Turn ON", "callback_data": f"cf_on_{slot}"}, {"text": "🔴 Turn OFF", "callback_data": f"cf_off_{slot}"}],
                [{"text": "🗑 Reset Slot", "callback_data": f"cf_del_{slot}"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "fj_edit"}],
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"📌 Slot {slot} — {state}{details}", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("fjcfg_"):
            slot = int(cdata.split("_")[1])
            if slot < 1 or slot > FJ_SLOT_COUNT:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Invalid slot."})
                return "OK", 200
            _delete_admin_menu(uid, cid)
            ADMIN_SESSIONS[uid] = {"step": "FJ_E_TITLE", "slot": slot, "return_callback": "fj_edit", "chat_id": cid}
            tg_call("sendMessage", {"chat_id": cid, "text": f"Slot {slot}: Enter channel button text (title):"})

        elif cdata == "fj_onoff":
            kb = [
                [{"text": "🟢 "+mono("Turn ON Slot"), "callback_data": "fj_list_to_on"}],
                [{"text": "🔴 "+mono("Turn OFF Slot"), "callback_data": "fj_list_to_off"}],
                [{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}]
            ]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select toggle operation:", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata in ("fj_list_to_on", "fj_list_to_off"):
            action = "ON" if cdata == "fj_list_to_on" else "OFF"
            btns=[]
            for i in range(0, FJ_SLOT_COUNT, 2):
                row=[]
                for slot in (i+1, i+2):
                    if slot <= FJ_SLOT_COUNT:
                        row.append({"text": mono(f"Slot {slot}"), "callback_data": f"cf_{action.lower()}_{slot}"})
                btns.append(row)
            btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "fj_onoff"}])
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Select Slot to turn {action}:", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("cf_on_"):
            s = int(cdata.split("_")[2])
            row = next((x for x in _fj_slot_rows() if int(x.get("slot")) == s), None)
            if not _fj_slot_configured(row):
                tg_call("sendMessage", {"chat_id": cid, "text": f"⚠️ Slot {s} is not fully configured. Set title, Join URL and Chat ID first."})
                return "OK", 200
            kb = [[{"text": "✅ Confirm Turn ON", "callback_data": f"act_on_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_onoff"}]]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Enable Slot {s}?", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("cf_off_"):
            s = int(cdata.split("_")[2])
            kb = [[{"text": "✅ Confirm Turn OFF", "callback_data": f"act_off_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_onoff"}]]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Disable Slot {s}?", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("act_on_"):
            s = int(cdata.split("_")[2])
            row = next((x for x in _fj_slot_rows() if int(x.get("slot")) == s), None)
            if not _fj_slot_configured(row):
                tg_call("sendMessage", {"chat_id": cid, "text": f"⚠️ Slot {s} is not fully configured. Cannot turn it ON."})
            elif supabase:
                try:
                    supabase.table("force_join_channels").update({"active": True}).eq("slot", s).execute()
                    invalidate_channels_cache()
                    tg_call("sendMessage", {"chat_id": cid, "text": f"✅ Slot {s} is ON."})
                except Exception as exc:
                    print(f"FJ TURN ON ERROR slot={s}: {exc}", flush=True)
                    tg_call("sendMessage", {"chat_id": cid, "text": f"❌ Could not turn Slot {s} ON. Check Supabase/slot schema."})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "❌ Supabase unavailable."})
            show_fj_menu(cid)

        elif cdata.startswith("act_off_"):
            s = int(cdata.split("_")[2])
            if supabase:
                try:
                    supabase.table("force_join_channels").update({"active": False}).eq("slot", s).execute()
                    invalidate_channels_cache()
                    tg_call("sendMessage", {"chat_id": cid, "text": f"🔴 Slot {s} is OFF."})
                except Exception as exc:
                    print(f"FJ TURN OFF ERROR slot={s}: {exc}", flush=True)
                    tg_call("sendMessage", {"chat_id": cid, "text": f"❌ Could not turn Slot {s} OFF. Check Supabase/slot schema."})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "❌ Supabase unavailable."})
            show_fj_menu(cid)

        elif cdata == "fj_rem":
            btns=[]
            for i in range(0, FJ_SLOT_COUNT, 2):
                row=[]
                for slot in (i+1, i+2):
                    if slot <= FJ_SLOT_COUNT:
                        row.append({"text": mono(f"Slot {slot}"), "callback_data": f"cf_del_{slot}"})
                btns.append(row)
            btns.append([{"text": "⬅️ "+mono("Back"), "callback_data": "m_fj"}])
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select Slot to reset to default OFF state:", "reply_markup": {"inline_keyboard": mono_admin_kb(btns)}})

        elif cdata.startswith("cf_del_"):
            s = int(cdata.split("_")[2])
            kb = [[{"text": "🗑 Confirm Reset", "callback_data": f"act_del_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_rem"}]]
            tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Reset Slot {s} to default OFF state?", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        elif cdata.startswith("act_del_"):
            s = int(cdata.split("_")[2])
            if supabase and 1 <= s <= FJ_SLOT_COUNT:
                # Reset always means a clean, reusable OFF slot. This same rule
                # applies to every slot, including the built-in Slot 5.
                reset = {"button_text": "", "join_url": "", "chat_id": None, "active": False}
                try:
                    supabase.table("force_join_channels").update(reset).eq("slot", s).execute()
                    invalidate_channels_cache()
                    tg_call("sendMessage", {"chat_id": cid, "text": f"✅ Slot {s} reset to default OFF state."})
                except Exception as exc:
                    print(f"FJ RESET ERROR slot={s}: {exc}", flush=True)
                    tg_call("sendMessage", {"chat_id": cid, "text": f"❌ Could not reset Slot {s}. Check Supabase/slot schema."})
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "❌ Supabase unavailable or invalid slot."})
            show_fj_menu(cid)

        # --- Broadcast Sub-Module ---
        elif cdata in ("bc_cleanup_last", "bc_cleanup_all"):
            redis_lock_key = "lexi:lock:broadcast"
            redis_lock_token = f"{os.getpid()}:{threading.get_ident()}:{time.time_ns()}"
            redis_lock = _redis_lock_acquire(redis_lock_key, redis_lock_token, ttl=3600)
            local_broadcast_lock = False
            if redis_lock is False:
                tg_call("sendMessage", {"chat_id": cid, "text": "⏳ Another broadcast cleanup/send is already running. Please wait."})
                return "OK", 200
            if redis_lock is None:
                local_broadcast_lock = BROADCAST_SEND_LOCK.acquire(blocking=False)
                if not local_broadcast_lock:
                    tg_call("sendMessage", {"chat_id": cid, "text": "⏳ Another broadcast cleanup/send is already running. Please wait."})
                    return "OK", 200
            try:
                mode = "all" if cdata == "bc_cleanup_all" else "last"
                records = _broadcast_history_load()
                legacy_deleted = 0
                legacy_errors = []
                # Capture the current tracked broadcast fingerprint BEFORE deleting it,
                # then use it to clean identical legacy messages from older bot versions.
                if mode == "all" and supabase and records:
                    try:
                        legacy_users = list(_iter_user_rows(500))
                        legacy_deleted, legacy_errors = _legacy_broadcast_sweep(records, legacy_users)
                    except Exception as exc:
                        print(f"LEGACY BROADCAST SWEEP ERROR: {exc}", flush=True)
                        legacy_errors = [("system", None, str(exc))]
                ok, remaining, stats = _broadcast_delete_records(records, mode)
                stats["deleted"] = int(stats.get("deleted", 0)) + legacy_deleted
                if legacy_errors:
                    stats["legacy_errors"] = legacy_errors[:5]
                    ok = False
                if not ok:
                    _broadcast_history_save(remaining)
                    detail_items = [
                        f"• Chat `{x.get('chat_id')}` / Message `{x.get('message_id')}` — {x.get('error')}"
                        for x in stats.get("failed", [])[:5]
                    ]
                    detail_items += [
                        f"• Legacy Chat `{x[0]}` / Message `{x[1]}` — {x[2]}"
                        for x in stats.get("legacy_errors", [])[:5]
                    ]
                    details = "\n".join(detail_items)
                    tg_call("sendMessage", {
                        "chat_id": cid,
                        "text": f"❌ Cleanup FAILED.\nDeleted: `{stats.get('deleted', 0)}`\nRemaining failed messages: `{len(stats.get('failed', []))}`\n\n{details}\n\n⚠️ New broadcast was NOT requested/sent. Fix the Telegram delete error first."
                    })
                    return "OK", 200
                if not _broadcast_history_save(remaining):
                    tg_call("sendMessage", {"chat_id": cid, "text": "❌ Cleanup succeeded, but history could not be updated in Supabase. New broadcast was NOT started."})
                    return "OK", 200
                ADMIN_SESSIONS.pop(uid, None)
                _broadcast_draft_clear()
                tg_call("sendMessage", {
                    "chat_id": cid,
                    "text": f"✅ Cleanup completed.\nDeleted messages: `{stats.get('deleted', 0)}`\nDeleted broadcasts: `{stats.get('broadcasts', 0)}`\n\nNow send/forward the NEW broadcast post."
                })
                _broadcast_request_content(cid, uid)
            finally:
                if redis_lock:
                    _redis_lock_release(redis_lock_key, redis_lock_token)
                elif local_broadcast_lock:
                    BROADCAST_SEND_LOCK.release()

        elif cdata == "send_confirmed_bc":
            bc_data = ADMIN_SESSIONS.get(uid, {}).get("bc_payload") or _broadcast_draft_load()
            if not bc_data:
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "❌ Broadcast content expired. Please send the post again."})
                show_admin_home(cid)
                return "OK", 200

            redis_lock_key = "lexi:lock:broadcast"
            redis_lock_token = f"{os.getpid()}:{threading.get_ident()}:{time.time_ns()}"
            redis_lock = _redis_lock_acquire(redis_lock_key, redis_lock_token, ttl=3600)
            local_broadcast_lock = False
            if redis_lock is False:
                tg_call("sendMessage", {"chat_id": cid, "text": "⏳ Another broadcast is already being processed. Please wait."})
                return "OK", 200
            if redis_lock is None:
                local_broadcast_lock = BROADCAST_SEND_LOCK.acquire(blocking=False)
                if not local_broadcast_lock:
                    tg_call("sendMessage", {"chat_id": cid, "text": "⏳ Another broadcast is already being processed. Please wait."})
                    return "OK", 200
            try:
                message_ids = [int(x) for x in (bc_data.get("message_ids") or [])]
                source_chat = bc_data.get("from_chat")
                if not message_ids or source_chat is None:
                    ADMIN_SESSIONS.pop(uid, None)
                    _broadcast_draft_clear()
                    tg_call("sendMessage", {"chat_id": cid, "text": "❌ Broadcast source/content is invalid. Please send it again."})
                    show_admin_home(cid)
                    return "OK", 200

                blocked = _blocked_users_load()
                # Durable per-broadcast ID used by broadcast_deliveries.
                # The unique (broadcast_id, user_id) key prevents duplicate
                # delivery even if multiple workers/processes handle the send.
                broadcast_id = str(int(time.time() * 1000))
                new_messages = {}
                sent = 0
                failed = 0
                telegram_blocked = 0
                failure_samples = []
                history_lock = threading.Lock()

                def deliver_to_user(u):
                    nonlocal sent, failed, telegram_blocked
                    target = int(u["telegram_user_id"])
                    try:
                        claimed, claim_error = _broadcast_delivery_claim(broadcast_id, target)
                        if not claimed:
                            if claim_error:
                                with history_lock:
                                    failed += 1
                                    if len(failure_samples) < 8:
                                        failure_samples.append((target, f"delivery claim failed: {claim_error}"))
                                print(f"BROADCAST DELIVERY CLAIM ERROR user={target}: {claim_error}", flush=True)
                            # No error means another worker/process already sent
                            # or claimed this exact broadcast for this user.
                            return

                        wait=_redis_rate_limit("broadcast", rate=25.0, burst=25)
                        if wait>0: time.sleep(wait)
                        copied = []
                        if len(message_ids) > 1:
                            r = tg_call("copyMessages", {
                                "chat_id": target,
                                "from_chat_id": source_chat,
                                "message_ids": sorted(message_ids),
                            })
                            if r.get("ok"):
                                copied = [int(x.get("message_id")) for x in (r.get("result") or []) if isinstance(x, dict) and x.get("message_id")]
                                # Telegram may skip unsupported/deleted source messages in copyMessages.
                                # Never guess which source IDs were skipped: a partial batch is cleaned up
                                # and the complete source list is retried one-by-one, preventing duplicates.
                                if len(copied) != len(message_ids):
                                    if copied:
                                        _broadcast_delete_messages({str(target): copied})
                                        copied = []
                                    for source_mid in sorted(message_ids):
                                        rr = tg_call("copyMessage", {"chat_id": target, "from_chat_id": source_chat, "message_id": source_mid})
                                        if not rr.get("ok"):
                                            raise RuntimeError(rr.get("description", "copyMessage failed"))
                                        copied.append(int(rr["result"]["message_id"]))
                            else:
                                for source_mid in sorted(message_ids):
                                    rr = tg_call("copyMessage", {"chat_id": target, "from_chat_id": source_chat, "message_id": source_mid})
                                    if not rr.get("ok"):
                                        raise RuntimeError(rr.get("description", "copyMessage failed"))
                                    copied.append(int(rr["result"]["message_id"]))
                        else:
                            r = tg_call("copyMessage", {"chat_id": target, "from_chat_id": source_chat, "message_id": message_ids[0]})
                            if not r.get("ok"):
                                raise RuntimeError(r.get("description", "copyMessage failed"))
                            copied = [int(r["result"]["message_id"])]
                        if copied:
                            _broadcast_delivery_finish(broadcast_id, target, "sent")
                            with history_lock:
                                new_messages[str(target)] = copied
                                sent += 1
                    except Exception as exc:
                        desc = str(exc)
                        low = desc.lower()
                        # If an album was only partially copied, remove the messages
                        # already created for this recipient so a failed delivery
                        # does not leave orphan broadcast messages.
                        if copied:
                            try:
                                _broadcast_delete_messages({str(target): copied})
                            except Exception as cleanup_exc:
                                print(f"BROADCAST PARTIAL CLEANUP ERROR user={target}: {cleanup_exc}", flush=True)
                        _broadcast_delivery_finish(broadcast_id, target, "failed")
                        with history_lock:
                            failed += 1
                            if any(x in low for x in ("bot was blocked", "user is deactivated", "chat not found", "forbidden")):
                                telegram_blocked += 1
                            if len(failure_samples) < 8:
                                failure_samples.append((target, desc))
                        print(f"BROADCAST DELIVERY ERROR user={target}: {desc}", flush=True)

                try:
                    configured_workers = max(1, min(int(float(get_setting("broadcast_workers", str(DEFAULT_BROADCAST_WORKERS)))), 32))
                except Exception:
                    configured_workers = DEFAULT_BROADCAST_WORKERS
                workers = max(1, configured_workers)
                # Stream users in bounded batches so broadcast memory stays flat even at 1M+ users.
                with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="broadcast-send") as ex:
                    batch=[]
                    for target_uid in (_iter_user_ids(500) if supabase else []):
                        if int(target_uid) in blocked:
                            continue
                        batch.append({"telegram_user_id":int(target_uid)})
                        if len(batch) >= workers * 4:
                            list(ex.map(deliver_to_user, batch))
                            batch.clear()
                    if batch:
                        list(ex.map(deliver_to_user, batch))

                records = _broadcast_history_load()
                new_record = {
                    "broadcast_id": broadcast_id,
                    "created_at": int(time.time()),
                    "messages": new_messages,
                }
                records.append(new_record)
                if not _broadcast_history_save(records):
                    rollback_deleted, rollback_failed = _broadcast_delete_messages(new_messages)
                    tg_call("sendMessage", {"chat_id": cid, "text": "❌ Broadcast history could not be saved.\n\nNew broadcast was rolled back." if not rollback_failed else "❌ Broadcast history could not be saved and rollback was incomplete. Do not send another broadcast until Supabase is fixed."})
                    return "OK", 200

                ADMIN_SESSIONS.pop(uid, None)
                ADMIN_PROMPT_MESSAGES.pop(uid, None)
                _broadcast_draft_clear()
                summary = (
                    f"✅ Broadcast completed.\n\n"
                    f"👥 Sent: `{sent}`\n"
                    f"❌ Failed: `{failed}`\n"
                    f"🚫 Telegram blocked/deactivated: `{telegram_blocked}`"
                )
                if failure_samples:
                    summary += "\n\n⚠️ First failures:\n" + "\n".join(f"• `{u}` — {e}" for u, e in failure_samples[:5])
                tg_call("sendMessage", {"chat_id": cid, "text": summary})
                show_admin_home(cid)
            finally:
                if redis_lock:
                    _redis_lock_release(redis_lock_key, redis_lock_token)
                elif local_broadcast_lock:
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
            enabled = get_setting("welcome_dm_enabled", "true").lower() == "true"
            mode = get_setting("welcome_dm_mode", "after_approval")
            delay = get_setting("welcome_dm_delay", "270")
            photo = "SET" if get_setting("welcome_dm_photo", "").strip() else "NONE"
            kb=[[{"text":("🟢 ON" if enabled else "🔴 OFF"),"callback_data":"welcome_toggle"}],
                [{"text":"⚡ After Approval","callback_data":"welcome_mode_approval"},{"text":"⏱ Request Timer","callback_data":"welcome_mode_timer"}],
                [{"text":f"⏱ Delay: {delay}s","callback_data":"welcome_delay"}],
                [{"text":"📝 Edit Message","callback_data":"welcome_text"},{"text":f"🖼 Image: {photo}","callback_data":"welcome_photo"}],
                [{"text":"🔘 Edit Buttons","callback_data":"welcome_buttons"}],
                [{"text":"📢 Welcome Channels","callback_data":"welcome_channels"}],
                [{"text":"🧪 Test Welcome","callback_data":"welcome_test"}],
                [{"text":"⬅️ Back","callback_data":"set_msg"}]]
            desc=("After Approval = actual join first; reliable for users who already started the bot.\n"
                   "Request Timer = best-effort DM inside Telegram\'s temporary request window; it can occur before approval.\n\n"
                   f"Status: {'ON' if enabled else 'OFF'}\nMode: {mode}\nDelay: {delay}s\nImage: {photo}")
            tg_call("editMessageText", {"chat_id":cid,"message_id":mid,"text":"👋 **WELCOME DM**\n\n"+desc,"parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
        elif cdata == "welcome_toggle":
            set_setting("welcome_dm_enabled", "false" if get_setting("welcome_dm_enabled","true").lower()=="true" else "true")
            process_callback_query({**cq,"data":"s_wel"}); return "OK",200
        elif cdata == "welcome_mode_approval":
            set_setting("welcome_dm_mode","after_approval"); process_callback_query({**cq,"data":"s_wel"}); return "OK",200
        elif cdata == "welcome_mode_timer":
            set_setting("welcome_dm_mode","request_timer"); process_callback_query({**cq,"data":"s_wel"}); return "OK",200
        elif cdata == "welcome_delay":
            _delete_admin_menu(uid,cid); ADMIN_SESSIONS[uid]={"step":"SET_WELCOME_DELAY","return_callback":"m_sett","chat_id":cid}
            tg_call("sendMessage",{"chat_id":cid,"text":"⏱ Send Welcome delay in seconds (0-300):"})
        elif cdata == "welcome_text":
            _delete_admin_menu(uid,cid); ADMIN_SESSIONS[uid]={"step":"SET_WELCOME_TEXT","return_callback":"m_sett","chat_id":cid}
            cur=get_setting("welcome_dm_text",DEFAULT_WELCOME_DM)
            tg_call("sendMessage",{"chat_id":cid,"text":f"📝 CURRENT WELCOME DM:\n\n{cur}\n\nVariables: {{name}} {{username}} {{channel_name}}\n\nSend new text:"})
        elif cdata == "welcome_photo":
            _delete_admin_menu(uid,cid); ADMIN_SESSIONS[uid]={"step":"SET_WELCOME_PHOTO","return_callback":"m_sett","chat_id":cid}
            tg_call("sendMessage",{"chat_id":cid,"text":"🖼 Send a photo to use in Welcome DM.\nSend `remove` to remove the image."})
        elif cdata == "welcome_buttons":
            _delete_admin_menu(uid,cid); ADMIN_SESSIONS[uid]={"step":"SET_WELCOME_BUTTONS","return_callback":"m_sett","chat_id":cid}
            cur=get_setting("welcome_dm_buttons","[]")
            tg_call("sendMessage",{"chat_id":cid,"text":f"🔘 Current buttons JSON:\n{cur}\n\nSend one per line as: Button Text | https://example.com\nSend `none` to remove all:"})
        elif cdata == "welcome_channels":
            chs=get_channels() or []
            selected=set()
            try: selected=_welcome_selected_channels()
            except Exception: selected=set()
            kb=[]
            for c in chs:
                if c.get("chat_id") is None: continue
                cidv=str(c.get("chat_id")); title=str(c.get("title") or c.get("name") or c.get("button_text") or cidv)
                kb.append([{"text":("✅ " if cidv in selected else "⬜ ")+title[:48],"callback_data":f"welcome_ch_{c.get('slot')}"}])
            kb.append([{"text":"🌐 All Channels","callback_data":"welcome_ch_all"},{"text":"🚫 None","callback_data":"welcome_ch_none"}])
            kb.append([{"text":"⬅️ Back","callback_data":"s_wel"}])
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"📢 **WELCOME CHANNELS**\n\nSelected: `{len(selected)}`\nWelcome DM will trigger only for selected channel(s).","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb(kb)}})
        elif cdata.startswith("welcome_ch_"):
            action=cdata[len("welcome_ch_"):]
            chs=get_channels() or []
            ids=[str(c.get("chat_id")) for c in chs if c.get("chat_id") is not None]
            if action=="all": selected=ids
            elif action=="none": selected=[]
            else:
                selected=set()
                try: selected=_welcome_selected_channels()
                except Exception: pass
                for c in chs:
                    if str(c.get("slot"))==action and c.get("chat_id") is not None:
                        cidv=str(c.get("chat_id")); selected.remove(cidv) if cidv in selected else selected.add(cidv); break
                selected=list(selected)
            set_setting(WELCOME_CHANNELS_KEY,json.dumps(sorted(set(selected)),separators=(",",":")))
            process_callback_query({**cq,"data":"welcome_channels"}); return "OK",200
        elif cdata == "rel_retry":
            attempts=get_setting("telegram_retry_attempts",str(DEFAULT_RETRY_ATTEMPTS)); mx=get_setting("telegram_retry_max_delay",str(DEFAULT_RETRY_MAX_DELAY))
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"🔁 **TELEGRAM RETRY**\n\nAttempts: `{attempts}`\nMax backoff: `{mx}s`\n429/5xx: ON\nParse fallback: ON","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"✏️ Attempts","callback_data":"rel_set_tg_attempts"},{"text":"✏️ Max Delay","callback_data":"rel_set_tg_delay"}],[{"text":"⬅️ Back","callback_data":"set_grp_reliability"}]])}})
        elif cdata == "rel_workers":
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"⚡ **WORKER SETTINGS**\n\nPublic: `32`\nVerify: `8`\nWelcome: `16`\nBroadcast: `{get_setting('broadcast_workers',str(DEFAULT_BROADCAST_WORKERS))}`\n\nWebhook queueing is enabled.","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"📢 Broadcast Workers","callback_data":"rel_set_bc_workers"}],[{"text":"⬅️ Back","callback_data":"set_grp_reliability"}]])}})
        elif cdata == "rel_db":
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"🗄 **SUPABASE RETRY**\n\nAttempts: `{get_setting('supabase_retry_attempts','3')}`\nTransient write retry: ON","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"✏️ Set Attempts","callback_data":"rel_set_db_attempts"}],[{"text":"⬅️ Back","callback_data":"set_grp_reliability"}]])}})
        elif cdata == "rel_broadcast":
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"📢 **BROADCAST LIMITS**\n\nWorkers: `{get_setting('broadcast_workers',str(DEFAULT_BROADCAST_WORKERS))}`\nAlbum wait: `{get_setting('broadcast_album_wait',str(DEFAULT_ALBUM_WAIT))}s`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"✏️ Workers","callback_data":"rel_set_bc_workers"},{"text":"✏️ Album Wait","callback_data":"rel_set_album_wait"}],[{"text":"⬅️ Back","callback_data":"set_grp_reliability"}]])}})
        elif cdata in ("rel_set_tg_attempts","rel_set_tg_delay","rel_set_db_attempts","rel_set_bc_workers","rel_set_album_wait"):
            steps={"rel_set_tg_attempts":("SET_REL_TG_ATTEMPTS","telegram_retry_attempts","1-8"),"rel_set_tg_delay":("SET_REL_TG_DELAY","telegram_retry_max_delay","0.5-30"),"rel_set_db_attempts":("SET_REL_DB_ATTEMPTS","supabase_retry_attempts","1-6"),"rel_set_bc_workers":("SET_REL_BC_WORKERS","broadcast_workers","1-32"),"rel_set_album_wait":("SET_REL_ALBUM_WAIT","broadcast_album_wait","0.2-5")}
            step,key,rng=steps[cdata]; _delete_admin_menu(uid,cid); ADMIN_SESSIONS[uid]={"step":step,"return_callback":"set_grp_reliability","chat_id":cid,"setting_key":key,"range":rng}
            tg_call("sendMessage",{"chat_id":cid,"text":f"⚙️ Send value for `{key}` ({rng}):","parse_mode":"Markdown"})
        elif cdata in ("rel_set_pending_ttl","rel_set_dedup_ttl"):
            key="pending_request_ttl" if cdata=="rel_set_pending_ttl" else "update_dedup_ttl"; rng="60-86400"
            _delete_admin_menu(uid,cid); ADMIN_SESSIONS[uid]={"step":"SET_REL_TTL","return_callback":"set_grp_reliability","chat_id":cid,"setting_key":key,"range":rng}
            tg_call("sendMessage",{"chat_id":cid,"text":f"⚙️ Send `{key}` in seconds ({rng}):","parse_mode":"Markdown"})
        elif cdata == "rel_cleanup":
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"🧹 **STATE CLEANUP**\n\nPending TTL: `{get_setting('pending_request_ttl',str(PENDING_REQUEST_TTL))}s`\nUpdate dedup TTL: `{get_setting('update_dedup_ttl',str(DEFAULT_DEDUP_TTL))}s`\nWelcome state: max 5000 records","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"✏️ Pending TTL","callback_data":"rel_set_pending_ttl"},{"text":"✏️ Dedup TTL","callback_data":"rel_set_dedup_ttl"}],[{"text":"⬅️ Back","callback_data":"set_grp_reliability"}]])}})
        elif cdata == "rel_health":
            sc="OK" if supabase else "OFF"
            cc="READY" if _CHANNELS_CACHE_READY else "COLD"
            tg="SET" if BOT_TOKEN else "MISSING"
            age = (time.time() - WEBHOOK_LAST_RECEIVED_AT) if WEBHOOK_LAST_RECEIVED_AT else None
            wh = "RECEIVING" if age is not None and age < 300 else ("NO RECENT UPDATE" if age is not None else "NOT YET SEEN")
            tg_call("editMessageText",{"chat_id":cid,"message_id":mid,"text":f"❤️ **HEALTH**\n\nTelegram Token: `{tg}`\nSupabase: `{sc}`\nChannels Cache: `{cc}`\nExecutors: `READY`\nWebhook: `{wh}`","parse_mode":"Markdown","reply_markup":{"inline_keyboard":mono_admin_kb([[{"text":"⬅️ Back","callback_data":"set_grp_reliability"}]])}})
        elif cdata == "welcome_test":
            rec={"user_id":uid,"chat_id":"test","chat_title":"Test Channel","user":{"id":uid,"first_name":"Admin","username":""}}
            ok,detail=_welcome_send(rec,force_chat_id=uid)
            tg_call("sendMessage",{"chat_id":cid,"text":("✅ Test Welcome sent." if ok else f"❌ Test failed: {detail}")})
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
        # Materialize the fixed 20-slot Force Join model once at startup/warmup,
        # rather than querying it on every verification.
        _ensure_force_join_slots()
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
            supabase.table("join_requests").upsert(payload, on_conflict="user_id,chat_id").execute()
            return
        except Exception as first_exc:
            print(f"JOIN REQUEST STATUS SCHEMA FALLBACK user={user_id} chat={chat_id}: {first_exc}", flush=True)
        # Compatibility with older join_requests tables without status.
        for chat_value in (str(chat_id), int(chat_id)):
            try:
                supabase.table("join_requests").upsert({
                    "user_id": int(user_id), "chat_id": chat_value
                }, on_conflict="user_id,chat_id").execute()
                return
            except Exception:
                continue
        raise RuntimeError("join_requests table is missing a compatible user_id/chat_id schema")
    except Exception as exc:
        print(f"JOIN REQUEST DB ERROR user={user_id} chat={chat_id}: {exc}", flush=True)




def _welcome_selected_channels():
    """Return configured Welcome DM channel IDs. Missing config means all active channels."""
    chs=get_channels() or []
    all_ids={str(c.get("chat_id")) for c in chs if c.get("chat_id") is not None and c.get("active")}
    raw=get_setting(WELCOME_CHANNELS_KEY, "__ALL__")
    if raw == "__ALL__" or raw == "": return all_ids
    try:
        val=json.loads(raw)
        return ({str(x) for x in val} & all_ids) if isinstance(val,list) else all_ids
    except Exception:
        return all_ids

def _welcome_lock_for(key):
    with WELCOME_LOCKS_GUARD:
        lock=WELCOME_LOCKS.get(key)
        if lock is None:
            lock=threading.Lock(); WELCOME_LOCKS[key]=lock
        return lock
def _welcome_pending_load():
    if supabase:
        try:
            rows=(supabase.table("welcome_queue").select("*").neq("status","done").order("updated_at",desc=True).limit(5000).execute().data or [])
            out={}
            for row in rows:
                key=str(row.get("job_key") or "")
                payload=row.get("payload") or {}
                if key: out[key]=payload if isinstance(payload,dict) else {}
            return out
        except Exception:
            pass
    try:
        raw=get_setting(WELCOME_PENDING_KEY,"{}")
        data=json.loads(raw) if raw else {}
        return data if isinstance(data,dict) else {}
    except Exception as exc:
        print(f"WELCOME STATE LOAD ERROR: {exc}",flush=True); return {}

def _welcome_pending_get(key):
    key=str(key)
    if supabase:
        try:
            rows=(supabase.table("welcome_queue").select("payload,status,run_at")
                  .eq("job_key",key).limit(1).execute().data or [])
            if rows:
                payload=rows[0].get("payload") or {}
                return payload if isinstance(payload,dict) else {}
        except Exception as exc:
            print(f"WELCOME QUEUE GET ERROR: {exc}",flush=True)
    try:
        raw=get_setting(WELCOME_PENDING_KEY,"{}")
        data=json.loads(raw) if raw else {}
        return data.get(key) if isinstance(data,dict) and isinstance(data.get(key),dict) else None
    except Exception:
        return None

def _welcome_pending_upsert(key,payload):
    key=str(key); payload=payload or {}
    if supabase:
        try:
            status=str(payload.get("status") or "pending")
            if payload.get("welcome_sent_at"): status="done"
            supabase.table("welcome_queue").upsert({
                "job_key":key,"status":status,"payload":payload,
                "run_at":float(payload.get("welcome_scheduled_at") or time.time()),
                "updated_at":time.time()
            },on_conflict="job_key").execute()
            return True
        except Exception as exc:
            print(f"WELCOME QUEUE UPSERT ERROR (legacy fallback): {exc}",flush=True)
    try:
        raw=get_setting(WELCOME_PENDING_KEY,"{}")
        data=json.loads(raw) if raw else {}
        if not isinstance(data,dict): data={}
        data[key]=payload
        items=list(data.items())[-5000:]
        return bool(set_setting(WELCOME_PENDING_KEY,json.dumps(dict(items),separators=(",",":"))))
    except Exception as exc:
        print(f"WELCOME STATE SAVE ERROR: {exc}",flush=True); return False

def _welcome_pending_save(data):
    data=data or {}
    if supabase:
        try:
            for key, payload in list(data.items())[-5000:]:
                status=str((payload or {}).get("status") or "pending")
                if (payload or {}).get("welcome_sent_at"): status="done"
                supabase.table("welcome_queue").upsert({"job_key":str(key),"status":status,"payload":payload or {},"run_at":float((payload or {}).get("welcome_scheduled_at") or time.time()),"updated_at":time.time()},on_conflict="job_key").execute()
            return True
        except Exception as exc:
            print(f"WELCOME QUEUE TABLE ERROR (legacy fallback): {exc}",flush=True)
    try:
        items=list(data.items())[-5000:]
        return bool(set_setting(WELCOME_PENDING_KEY,json.dumps(dict(items),separators=(",",":"))))
    except Exception as exc:
        print(f"WELCOME STATE SAVE ERROR: {exc}",flush=True); return False

def _welcome_key(user_id, chat_id):
    return f"{int(user_id)}:{str(chat_id)}"


def _welcome_text(record):
    user = record.get("user") or {}
    first = user.get("first_name") or "User"
    last = user.get("last_name") or ""
    name = " ".join(x for x in (first, last) if x).strip() or "User"
    username = user.get("username") or ""
    channel = record.get("chat_title") or "Channel"
    _, saved_entities = _load_message_setting("welcome_dm_text", DEFAULT_WELCOME_DM)
    if saved_entities:
        vals={"{name}":name,"{username}":f"@{username}" if username else "","{channel_name}":channel}
    else:
        vals={"{name}":clean_markdown(name),"{username}":clean_markdown(f"@{username}") if username else "","{channel_name}":clean_markdown(channel)}
    return _render_message_setting("welcome_dm_text", DEFAULT_WELCOME_DM, vals)


def _welcome_keyboard():
    raw = get_setting("welcome_dm_buttons", "[]")
    try:
        buttons = json.loads(raw) if raw else []
    except Exception:
        buttons = []
    kb=[]
    for item in buttons if isinstance(buttons, list) else []:
        if not isinstance(item, dict):
            continue
        text=str(item.get("text") or "").strip()
        url=str(item.get("url") or "").strip()
        if text and re.match(r"^https?://\S+$", url):
            kb.append([{"text": text, "url": url}])
    return {"inline_keyboard": kb} if kb else None


def _welcome_send(record, force_chat_id=None):
    if get_setting("welcome_dm_enabled", "true").lower() != "true":
        return True, "disabled"
    chat_id = force_chat_id if force_chat_id is not None else record.get("user_id")
    text, entities = _welcome_text(record)
    markup = _welcome_keyboard()
    photo = get_setting("welcome_dm_photo", "").strip()
    payload_base = {"chat_id": int(chat_id)}
    if photo:
        payload = dict(payload_base, photo=photo, caption=text)
        if entities: payload["caption_entities"] = entities
        else: payload["parse_mode"] = "Markdown"
        if markup: payload["reply_markup"] = markup
        r = tg_call("sendPhoto", payload)
    else:
        payload = dict(payload_base, text=text)
        if entities: payload["entities"] = entities
        else: payload["parse_mode"] = "Markdown"
        if markup: payload["reply_markup"] = markup
        r = tg_call("sendMessage", payload)
    if r.get("ok"):
        return True, "sent"
    return False, str(r.get("description") or "Telegram rejected the DM")


def _welcome_schedule_after_approval(record):
    try: delay=max(0.0,min(float(get_setting("welcome_dm_delay","0")),300.0))
    except Exception: delay=0.0
    key=_welcome_key(record.get("user_id"),record.get("chat_id"))
    run_at=time.time()+delay
    if _queue_job("welcome_dm", record, run_at, job_id=f"welcome:{key}"):
        return True,"scheduled"
    if delay<=0: return _welcome_send(record)
    def fire():
        time.sleep(delay)
        lock=_welcome_lock_for(key)
        with lock:
            current=_welcome_pending_get(key) or record
            if current.get("welcome_sent_at"): return
        ok,detail=_welcome_send(current)
        with lock:
            current=_welcome_pending_get(key) or record
            current["welcome_attempt_at"]=time.time(); current["welcome_detail"]=detail
            current.pop("welcome_scheduled_at",None)
            if ok: current["welcome_sent_at"]=time.time()
            _welcome_pending_upsert(key,current)
    threading.Thread(target=fire,daemon=True,name="welcome-fallback").start()
    return True,"scheduled"

def _welcome_handle_join_approved(cmu):
    chat = cmu.get("chat") or {}
    new_member = cmu.get("new_chat_member") or {}
    old_member = cmu.get("old_chat_member") or {}
    user = new_member.get("user") or {}
    try: uid=int(user.get("id") or 0); chat_id=str(chat.get("id"))
    except Exception: return
    old_status=str(old_member.get("status") or "")
    new_status=str(new_member.get("status") or "")
    became_member = (
        new_status in {"member", "administrator", "creator"}
        or (new_status == "restricted" and bool(new_member.get("is_member")))
    ) and old_status in {"left", "kicked", ""}
    if not uid or not chat_id or not became_member or not cmu.get("via_join_request"):
        return
    if chat_id not in _welcome_selected_channels(): return
    key=_welcome_key(uid,chat_id)
    lock=_welcome_lock_for(key)
    with lock:
        rec=_welcome_pending_get(key) or {}
        if rec.get("welcome_sent_at") or rec.get("welcome_scheduled_at"):
            return
        rec.update({"status":"approved","approved_at":time.time(),"user_id":uid,"chat_id":chat_id,
                    "chat_title":chat.get("title") or "Channel","user":user,
                    "welcome_scheduled_at":time.time()})
        if not _welcome_pending_upsert(key,rec): return
    def worker():
        with lock:
            current=_welcome_pending_get(key) or rec
            if current.get("welcome_sent_at"): return
        ok,detail=_welcome_schedule_after_approval(rec)
        if detail == "scheduled":
            print(f"WELCOME APPROVAL user={uid} chat={chat_id} scheduled",flush=True)
            return
        if not ok and rec.get("user_chat_id") and int(rec.get("user_chat_id")) != uid:
            ok2,detail2=_welcome_send(rec, force_chat_id=int(rec["user_chat_id"]))
            if ok2: ok,detail=True,detail2
        with lock:
            current=_welcome_pending_get(key) or rec
            current["welcome_attempt_at"]=time.time(); current["welcome_detail"]=detail
            if ok: current["welcome_sent_at"]=time.time()
            current.pop("welcome_scheduled_at",None)
            _welcome_pending_upsert(key,current)
        print(f"WELCOME APPROVAL user={uid} chat={chat_id} ok={ok} detail={detail}",flush=True)
    _WELCOME_EXECUTOR.submit(worker)

def _welcome_handle_request(j_req):
    user = j_req.get("from") or {}
    chat = j_req.get("chat") or {}
    try:
        uid = int(user.get("id")); chat_id = str(chat.get("id")); user_chat_id = int(j_req.get("user_chat_id"))
    except Exception:
        return
    selected=_welcome_selected_channels()
    if chat_id not in selected: return
    now = time.time()
    rec = {"status":"pending", "requested_at":now, "user_id":uid,
           "user_chat_id":user_chat_id, "chat_id":chat_id,
           "chat_title":chat.get("title") or "Channel", "user":user}
    PENDING_JOIN_REQUESTS[(uid, chat_id)] = now
    _welcome_pending_upsert(_welcome_key(uid, chat_id), rec)
    if supabase:
        try: _save_join_request(uid, chat_id)
        except Exception: pass
    # Optional request-window mode is the only Bot-API route for a user who has
    # never started the bot. It intentionally does not pretend to be approval-based.
    if get_setting("welcome_dm_enabled", "true").lower() == "true" and get_setting("welcome_dm_mode", "after_approval") == "request_timer":
        try: delay=max(0.0,min(float(get_setting("welcome_dm_delay","270")),300.0))
        except Exception: delay=270.0
        run_at=time.time()+delay
        rec["welcome_scheduled_at"]=run_at
        if not _queue_job("welcome_dm", rec, run_at, job_id=f"welcome:{_welcome_key(uid,chat_id)}"):
            def timer_worker():
                time.sleep(delay)
                current=_welcome_pending_get(_welcome_key(uid,chat_id))
                if not current or current.get("status") != "pending" or current.get("welcome_sent_at"): return
                ok,detail=_welcome_send(current, force_chat_id=current.get("user_chat_id"))
                current["welcome_attempt_at"]=time.time(); current["welcome_detail"]=detail
                if ok: current["welcome_sent_at"]=time.time()
                _welcome_pending_upsert(_welcome_key(uid,chat_id),current)
            threading.Thread(target=timer_worker,daemon=True,name="welcome-request-fallback").start()
    print(f"JOIN REQUEST RECEIVED user={uid} chat={chat_id} stored", flush=True)


def _handle_admin_command_fast(data):
    """Minimal-latency /admin path. Keep it independent from normal admin sessions.
    Authorization is still strict: only configured ADMIN_IDS are accepted.
    """
    try:
        msg = data.get("message") or {}
        uid = int((msg.get("from") or {}).get("id"))
        cid = msg.get("chat", {}).get("id")
        txt = str(msg.get("text") or "").strip()
        command = txt.split(maxsplit=1)[0].split("@", 1)[0].lower() if txt else ""
        if command != "/admin" or uid not in ADMIN_IDS:
            return
        print(f"ADMIN FAST PATH uid={uid} authorized=True", flush=True)
        ADMIN_SESSIONS.pop(uid, None)
        show_admin_home(cid)
    except Exception as exc:
        print(f"ADMIN FAST PATH ERROR: {exc}", flush=True)
        traceback.print_exc()

@app.route("/webhook", methods=["POST"])
def webhook():
    global WEBHOOK_LAST_RECEIVED_AT
    WEBHOOK_LAST_RECEIVED_AT = time.time()
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200
    update_id = data.get("update_id")
    if update_id is not None:
        now = time.time()
        try:
            dedup_ttl = max(60.0, min(float(get_setting("update_dedup_ttl", str(DEFAULT_DEDUP_TTL))), 86400.0))
        except Exception:
            dedup_ttl = DEFAULT_DEDUP_TTL
        redis_seen=_redis_setnx(f"lexi:update:{update_id}","1",dedup_ttl)
        if redis_seen is True:
            pass
        elif redis_seen is False:
            return "OK", 200
        else:
            with UPDATE_DEDUP_LOCK:
                for k, v in list(UPDATE_DEDUP.items()):
                    if now - v > dedup_ttl: UPDATE_DEDUP.pop(k, None)
                if update_id in UPDATE_DEDUP: return "OK", 200
                UPDATE_DEDUP[update_id] = now

    # Join Request: store only. It does NOT satisfy Force Join by itself.
    if "chat_join_request" in data:
        try:
            _UPDATE_EXECUTOR.submit(_safe_run, _welcome_handle_request, data["chat_join_request"])
        except Exception as exc:
            print(f"JOIN REQUEST HANDLER ERROR: {exc}", flush=True)
        return "OK", 200

    # Approval/join event: this is the exact point where an approved request
    # becomes an actual membership.
    if "chat_member" in data:
        try:
            _WELCOME_EXECUTOR.submit(_safe_run, _welcome_handle_join_approved, data["chat_member"])
        except Exception as exc:
            print(f"CHAT MEMBER HANDLER ERROR: {exc}", flush=True)
        return "OK", 200

    # Callback Query Router. Verification is isolated in its own executor so
    # slow membership/DB checks cannot starve the admin UI. Admin/non-verify
    # callbacks are handled directly because they are lightweight navigation
    # and should never wait behind background verification work.
    if "callback_query" in data:
        cq = data["callback_query"]
        uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")
        if uid in ADMIN_IDS:
            print(f"ADMIN CALLBACK RECEIVED uid={uid} data={cdata!r}", flush=True)
        # Acknowledge the callback outside the webhook request itself.
        # Telegram/Render network latency must never block /webhook long enough
        # to trigger Gunicorn worker timeouts.
        try:
            _CALLBACK_ACK_EXECUTOR.submit(_safe_run, tg_call, "answerCallbackQuery", {"callback_query_id": cq["id"]})
        except Exception:
            pass
        if cdata.startswith("v:"):
            code = cdata.split(":", 1)[1]
            cq_user = cq.get("from") or {}
            verify_name = " ".join(x for x in (cq_user.get("first_name"), cq_user.get("last_name")) if x).strip() or "User"
            _VERIFY_EXECUTOR.submit(_safe_run, run_verify_process, cid, uid, code, mid, verify_name)
        elif cdata == "send_confirmed_bc":
            # Long-running broadcast work gets its own isolated pool.
            _BROADCAST_EXECUTOR.submit(_safe_run, process_callback_query, cq)
        elif uid in ADMIN_IDS:
            # Admin navigation/settings are latency-sensitive and must not wait
            # behind public update traffic.
            _ADMIN_EXECUTOR.submit(_safe_run, process_callback_query, cq)
        else:
            _UPDATE_EXECUTOR.submit(_safe_run, process_callback_query, cq)
        return "OK", 200

    # Message Router. Admin messages get a dedicated pool so a busy public
    # update queue can never make /admin wait behind normal-user work.
    if "message" in data:
        msg = data.get("message", {})
        uid = msg.get("from", {}).get("id")
        txt = msg.get("text", "") or ""
        command = (txt.strip().split(maxsplit=1)[0].split("@", 1)[0].lower()
                   if txt.strip() else "")
        try:
            if command == "/admin":
                print(f"ADMIN COMMAND RECEIVED uid={uid} configured={uid in ADMIN_IDS}", flush=True)
            # Dedicated ultra-fast path for the admin panel command. It avoids
            # waiting behind any other admin workflow and does not depend on an
            # existing ADMIN_SESSIONS entry. Authorization remains strict.
            if command == "/admin" and uid in ADMIN_IDS:
                _ADMIN_COMMAND_EXECUTOR.submit(_handle_admin_command_fast, data)
            elif uid in ADMIN_IDS:
                _ADMIN_EXECUTOR.submit(_safe_run, process_message_update, data)
            else:
                _PUBLIC_EXECUTOR.submit(_safe_run, process_message_update, data)
        except Exception as exc:
            print(f"UPDATE QUEUE ERROR user={uid} text={txt[:80]!r}: {exc}", flush=True)
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



def _track_user(uid, username="", first_name="User"):
    """Persist a normal Telegram user using only columns known to exist."""
    if not supabase or uid in ADMIN_IDS:
        return False
    try:
        payload = {"telegram_user_id": int(uid), "username": username or None, "first_name": first_name or "User"}
        supabase.table("users").upsert(payload, on_conflict="telegram_user_id").execute()
        return True
    except Exception as exc:
        print(f"USER TRACK ERROR user={uid}: {exc}", flush=True)
        return False


def _show_users_page(cid, mid, page=0):
    rows, total = _load_users(max(0, int(page)), 20)
    if not rows:
        tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "👥 **Users**\n\nNo users found.", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ Back", "callback_data": "m_users"}]]}})
        return
    kb = []
    blocked = _blocked_users_load()
    for u in rows:
        uid = int(u.get("telegram_user_id"))
        marker = "🚫 " if uid in blocked else ""
        kb.append([{"text": marker + _user_display_name(u)[:55], "callback_data": f"u_view_{uid}"}])
    nav = []
    if page > 0:
        nav.append({"text": "◀️ Previous", "callback_data": f"u_list_{page-1}"})
    if (page + 1) * 20 < total:
        nav.append({"text": "Next ▶️", "callback_data": f"u_list_{page+1}"})
    if nav:
        kb.append(nav)
    kb.append([{"text": "⬅️ Back", "callback_data": "m_users"}])
    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"👥 **USERS**\n\nPage: `{page+1}/{max(1, (total+19)//20)}`\nTotal: `{total}`", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})


def _show_user_detail(cid, mid, target):
    target = int(target)
    rows, _ = _load_users(0, 1)
    user = None
    if supabase:
        try:
            found = supabase.table("users").select("*").eq("telegram_user_id", target).limit(1).execute().data or []
            user = found[0] if found else None
        except Exception:
            user = None
    if not user:
        tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "❌ User not found.", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ Back", "callback_data": "m_users"}]]}})
        return
    blocked = target in _blocked_users_load()
    txt = (f"👤 **USER DETAILS**\n\n"
           f"Name: `{clean_markdown(_user_display_name(user))}`\n"
           f"ID: `{target}`\n"
           f"Status: `{'BLOCKED' if blocked else 'ACTIVE'}`")
    kb = [[{"text": "🔓 Unblock" if blocked else "🚫 Block", "callback_data": f"u_unblock_{target}" if blocked else f"u_block_{target}"}],
          [{"text": "⬅️ Users", "callback_data": "u_list_0"}]]
    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

def process_message_update(data):
    msg = data["message"]
    cid, uid = msg["chat"]["id"], msg["from"]["id"]
    txt = msg.get("text") or msg.get("caption") or ""
    input_entities = msg.get("entities") or msg.get("caption_entities") or []
    uname = msg["from"].get("first_name", "User")
    mid = msg["message_id"]
    if uid not in ADMIN_IDS:
        STARTED_USERS.add(int(uid))

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
            _save_message_setting("verify_retry_text", txt, input_entities); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Retry message updated."}); show_settings_menu(cid); return "OK",200
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
            if not _valid_storage_forward(fwd_chat):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ This post is not from the configured Storage Channel."})
                return "OK", 200
            sess["fid"] = fwd_id
            kb = [
                [{"text": "👤 Hide Sender", "callback_data": "fmode_copy"}],
                [{"text": "🏷 Show Sender (Premium Emojis)", "callback_data": "fmode_forward"}]
            ]
            tg_call("sendMessage", {"chat_id": cid, "text": "Choose delivery mode:", "reply_markup": {"inline_keyboard": mono_admin_kb(kb)}})

        # File Edit Steps
        elif st == "F_EDIT_TITLE":
            if not supabase:
                tg_call("sendMessage", {"chat_id": cid, "text": "❌ Supabase unavailable."})
                return "OK", 200
            try:
                r=supabase.table("apps").update({"title": txt.strip()}).eq("deep_link_code", sess["code"]).select("deep_link_code").execute()
                if not (r.data or []): raise RuntimeError("File not found or update rejected")
                tg_call("sendMessage", {"chat_id": cid, "text": "✅ Title updated."})
                ADMIN_SESSIONS.pop(uid, None); show_files_menu(cid)
            except Exception as exc:
                print(f"FILE TITLE UPDATE ERROR: {exc}",flush=True)
                tg_call("sendMessage", {"chat_id": cid, "text": "❌ Title update failed. Old title remains."})
                return "OK", 200

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
            if not _valid_storage_forward(fwd_chat):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ This post is not from the configured Storage Channel."})
                return "OK", 200
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
            _save_message_setting("force_join_text", txt, input_entities)
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
                if not _valid_storage_forward(fwd_chat):
                    tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ This post is not from the configured Storage Channel."})
                    return "OK", 200
                if not set_setting("poster_storage_id", str(fwd_id)):
                    tg_call("sendMessage", {"chat_id": cid, "text": "❌ Could not save poster. Check Supabase."})
                    return "OK", 200
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_fj_menu(cid)
            else:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please forward directly from Storage Channel."})

        # Force Join fixed-slot configuration
        elif st == "FJ_E_TITLE":
            title = txt.strip()
            if not title:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Button title cannot be empty. Send it again:"})
                return "OK", 200
            sess["title"], sess["step"] = title, "FJ_E_URL"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter channel join/invite URL:"})
        elif st == "FJ_E_URL":
            url = txt.strip()
            if not re.match(r"^https?://\S+$", url):
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Please send a valid http/https invite link:"})
                return "OK", 200
            sess["url"], sess["step"] = url, "FJ_E_ID"
            tg_call("sendMessage", {"chat_id": cid, "text": "Enter channel Chat ID (e.g. -1001234567890):"})
        elif st == "FJ_E_ID":
            raw = txt.strip()
            try:
                ch_id = int(raw)
                if ch_id >= 0:
                    raise ValueError
                slot = int(sess.get("slot"))
                if not 1 <= slot <= FJ_SLOT_COUNT:
                    raise ValueError
                if not supabase:
                    raise RuntimeError("Supabase unavailable")
                current = (supabase.table("force_join_channels").select("active").eq("slot", slot).limit(1).execute().data or [])
                current_active = bool(current[0].get("active")) if current else False
                supabase.table("force_join_channels").upsert({
                    "slot": slot,
                    "button_text": sess["title"],
                    "join_url": sess["url"],
                    "chat_id": ch_id,
                    "active": current_active,
                }, on_conflict="slot").execute()
                invalidate_channels_cache()
                ADMIN_SESSIONS.pop(uid, None)
                state_text = "ON" if current_active else "OFF"
                tg_call("sendMessage", {"chat_id": cid, "text": f"✅ Slot {slot} settings saved. Slot is {state_text}."})
                show_fj_menu(cid)
            except ValueError:
                tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Invalid Chat ID. It should normally look like -1001234567890. Send it again:"})
            except Exception as exc:
                tg_call("sendMessage", {"chat_id": cid, "text": f"❌ Could not save Slot {sess.get('slot')}: {exc}"})

        # Broadcast Execution
        elif st == "WAIT_BC_CONTENT":
            media_group_id = msg.get("media_group_id")
            if media_group_id:
                # Telegram delivers each album item as a separate update. Collect
                # the whole group before showing the confirmation button.
                first=False
                r=_redis_client()
                if r:
                    key=f"lexi:album:{uid}:{group_id}"
                    try:
                        raw=r.get(key); item=json.loads(raw) if raw else {"group_id":str(media_group_id),"message_ids":[]}
                        if item.get("group_id")!=str(media_group_id): item={"group_id":str(media_group_id),"message_ids":[]}
                        if mid not in item["message_ids"]: item["message_ids"].append(mid)
                        r.set(key,json.dumps(item,separators=(",",":")),ex=15)
                        first=bool(r.set(f"lexi:album:timer:{uid}:{group_id}","1",nx=True,ex=15))
                    except Exception: pass
                else:
                    with BROADCAST_ALBUM_LOCK:
                        item = BROADCAST_ALBUM_BUFFERS.setdefault((uid, str(media_group_id)), {"group_id": str(media_group_id), "message_ids": []})
                        if item.get("group_id") != str(media_group_id):
                            item = {"group_id": str(media_group_id), "message_ids": []}; BROADCAST_ALBUM_BUFFERS[(uid, str(media_group_id))] = item
                        if mid not in item["message_ids"]: item["message_ids"].append(mid)
                        first = not item.get("timer_started"); item["timer_started"] = True
                if first:
                    threading.Thread(target=_finish_broadcast_album, args=(uid, cid, str(media_group_id)), daemon=True).start()
                return "OK", 200

            # Single-message broadcast.
            sess["bc_payload"] = {"from_chat": cid, "message_ids": [mid]}
            _broadcast_confirm(cid, uid)

        # Welcome DM settings
        elif st == "SET_WELCOME_DELAY":
            try:
                val=int(txt.strip())
                if val<0 or val>300: raise ValueError
                set_setting("welcome_dm_delay",str(val)); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Welcome delay updated."}); show_settings_menu(cid)
            except Exception:
                tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Enter seconds from 0 to 300:"})
        elif st == "SET_WELCOME_TEXT":
            _save_message_setting("welcome_dm_text", txt, input_entities); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Welcome DM updated."}); show_settings_menu(cid)
        elif st == "SET_WELCOME_PHOTO":
            if txt.strip().lower()=="remove":
                set_setting("welcome_dm_photo",""); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Welcome image removed."}); show_settings_menu(cid)
            elif msg.get("photo"):
                photo=msg.get("photo")[-1].get("file_id")
                if photo:
                    set_setting("welcome_dm_photo",photo); ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Welcome image saved."}); show_settings_menu(cid)
                else: tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Photo ID missing. Send the photo again."})
            else:
                tg_call("sendMessage",{"chat_id":cid,"text":"⚠️ Send a photo or `remove`."})
        elif st == "SET_WELCOME_BUTTONS":
            if txt.strip().lower()=="none":
                set_setting("welcome_dm_buttons","[]")
            else:
                rows=[]
                for line in txt.splitlines():
                    if "|" not in line: continue
                    bt,url=[x.strip() for x in line.split("|",1)]
                    if bt and re.match(r"^https?://\S+$",url): rows.append({"text":bt,"url":url})
                if len(rows)>5:
                    rows=rows[:5]
                set_setting("welcome_dm_buttons",json.dumps(rows,separators=(",",":")))
            ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":"✅ Welcome buttons updated."}); show_settings_menu(cid)

        # Reliability Settings Inputs
        elif st.startswith("SET_REL_"):
            key=sess.get("setting_key"); raw=txt.strip()
            try:
                val=float(raw)
                limits={"telegram_retry_attempts":(1,8),"telegram_retry_max_delay":(0.5,30),"supabase_retry_attempts":(1,6),"broadcast_workers":(1,32),"broadcast_album_wait":(0.2,5),"pending_request_ttl":(60,86400),"update_dedup_ttl":(60,86400)}
                lo,hi=limits[key]
                if val<lo or val>hi: raise ValueError
                out=str(int(val)) if float(val).is_integer() else str(val)
                if not set_setting(key,out): raise RuntimeError("save failed")
                ADMIN_SESSIONS.pop(uid,None); tg_call("sendMessage",{"chat_id":cid,"text":f"✅ `{key}` updated to `{out}`","parse_mode":"Markdown"}); show_settings_group(cid,mid,"reliability")
            except Exception:
                tg_call("sendMessage",{"chat_id":cid,"text":f"⚠️ Invalid value. Allowed: {sess.get('range')}."})

        # Settings Inputs
        elif st == "SET_W":
            _save_message_setting("welcome_text", txt, input_entities)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif st == "SET_E":
            _save_message_setting("error_feedback_text", txt, input_entities)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
            show_settings_menu(cid)
        elif st == "SET_WARN":
            _save_message_setting("auto_delete_warn_text", txt, input_entities)
            ADMIN_SESSIONS.pop(uid, None)
            tg_call("sendMessage", {"chat_id": cid, "text": "Success!"})
            show_settings_menu(cid)
        elif st == "SET_CUSTOM_TIMER":
            try:
                s_val = int(txt.strip())
                if not 1 <= s_val <= 86400:
                    raise ValueError("timer out of range")
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
            _save_message_setting("anim_text", txt, input_entities)
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
                welcome_payload = _message_payload("welcome_text", DEFAULT_WELCOME)
                result = tg_call("sendMessage", {"chat_id": cid, **welcome_payload})
                if not result.get("ok"):
                    # A Markdown formatting error should never make /start look
                    # dead. Retry the exact same text without parse_mode.
                    tg_call("sendMessage", {"chat_id": cid, "text": welcome_payload.get("text", "")})
        except Exception as exc:
            print(f"PUBLIC START ERROR user={uid} code={code!r}: {exc}", flush=True)
            traceback.print_exc()
            result = tg_call("sendMessage", {"chat_id": cid, **_message_payload("error_feedback_text", DEFAULT_ERROR)})
            if not result.get("ok"):
                tg_call("sendMessage", {"chat_id": cid, "text": _message_payload("error_feedback_text", DEFAULT_ERROR).get("text", "")})

        return "OK", 200



    return "OK", 200

def register_webhook():
    # Render provides RENDER_EXTERNAL_URL automatically. WEBHOOK_URL can
    # override it when using another host/domain. Always explicitly refresh
    # allowed_updates so callback_query delivery cannot remain stuck on an
    # older webhook configuration from a previous deployment.
    base_url = os.environ.get("WEBHOOK_URL", "").strip().rstrip("/")
    if not base_url:
        base_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    if not base_url or not BOT_TOKEN:
        return
    webhook_url = base_url if base_url.endswith("/webhook") else f"{base_url}/webhook"
    payload = {
        "url": webhook_url,
        "allowed_updates": ["message", "callback_query", "chat_join_request", "chat_member"],
        "max_connections": 40,
        "drop_pending_updates": False,
    }
    try:
        info = tg_call("getWebhookInfo", {})
        current = (info.get("result") or {}) if isinstance(info, dict) else {}
        current_url = str(current.get("url") or "")
        pending = current.get("pending_update_count", 0)
        if current_url != webhook_url:
            print(f"WEBHOOK UPDATE: current={current_url!r} target={webhook_url!r} pending={pending}", flush=True)
        result = tg_call("setWebhook", payload)
        print(f"WEBHOOK REGISTER: ok={bool(result.get('ok'))} url={webhook_url!r} allowed_updates={payload['allowed_updates']}", flush=True)
        if not result.get("ok"):
            print(f"WEBHOOK REGISTER ERROR: {result}", flush=True)
    except Exception as exc:
        print(f"WEBHOOK REGISTER ERROR: {exc}", flush=True)


# Warm read-heavy caches in the background so first user replies are fast.
threading.Thread(target=warm_caches, daemon=True).start()

# Register webhook when imported by Gunicorn on Render as well as when run directly.
if BOT_TOKEN and (os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL")):
    threading.Thread(target=register_webhook, daemon=True).start()

if __name__ == "__main__":
    register_webhook()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

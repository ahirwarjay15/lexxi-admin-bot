import os
import re
import time
import threading
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

# System Defaults
DEFAULT_WELCOME = ">> LOOKING FOR HACKS, ID'S OR ANYTHING ELSE?\nMESSAGE [@lexxiowner](https://t.me/lexxiowner) — WE MIGHT HAVE IT. ⚡"
DEFAULT_ERROR = "⚠️ [INVALID_FILE]\nFile not found or expired.\nContact @lexxiowner"
DEFAULT_FJ = ">> HEY {name} ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T JOINED TO\nOUR CHANNELS YET,"
DEFAULT_STORAGE = "-1004430375220"
DEFAULT_WARN = "⚠️ **IMPORTANT NOTICE:**\n\n_All files will be deleted in {time}.\nPlease forward/save this to your Saved Messages!_"
DEFAULT_UP_BTN = "📟 Update Channel"
DEFAULT_UP_URL = "https://t.me/lexxiowner"
DEFAULT_ANIM_TEXT = "Verifying"

def clean_markdown(name):
    return re.sub(r'([_*\[\]()~`>#+=|{}.!-])', r'\\\1', str(name or "User"))

def tg_call(method, data):
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=10).json()
        if not r.get("ok") and "parse_mode" in data:
            data.pop("parse_mode", None)
            return requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=10).json()
        return r
    except Exception:
        return {}

def get_setting(key, default=""):
    if not supabase:
        return default
    try:
        res = supabase.table("settings").select("value").eq("key", key).execute()
        if res.data:
            return res.data[0].get("value", default)
    except Exception:
        pass
    return default

def set_setting(key, value):
    if not supabase:
        return
    try:
        supabase.table("settings").upsert({"key": key, "value": str(value)}).execute()
    except Exception:
        pass

def get_channels():
    if supabase:
        try:
            r = supabase.table("force_join_channels").select("*").order("slot").execute()
            if r.data:
                return r.data
        except Exception:
            pass
    return []

def get_unjoined(user_id):
    channels = get_channels()
    unjoined = []
    for c in channels:
        if not c.get("active") or not c.get("chat_id"):
            continue
        try:
            res = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
                json={"chat_id": c["chat_id"], "user_id": user_id},
                timeout=4
            ).json()
        if not res.get("ok") or res.get("result", {}).get("status") in ["left", "kicked"]:
            req_found = False
            if supabase:
                try:
                    jr = supabase.table("join_requests").select("*").eq("user_id", user_id).eq("chat_id", str(c["chat_id"])).execute()
                    if jr.data:
                        req_found = True
                except Exception:
                    pass
            if not req_found:
                unjoined.append(c)

        except Exception:
            unjoined.append(c)
    return unjoined

def send_fj(chat_id, user_id, code, name):
    unjoined = get_unjoined(user_id)
    if not unjoined:
        return True

    raw = get_setting("force_join_text", DEFAULT_FJ)
    caption = raw.replace("{name}", clean_markdown(name)) if "{name}" in raw else raw

    layout = int(get_setting("button_layout", "2"))
    kb, row = [], []
    for c in unjoined:
        btn_txt = c.get('button_text', '').replace("↗", "").replace("↗️", "").strip()
        row.append({"text": btn_txt, "url": c.get("join_url")})
        if len(row) == layout:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    kb.append([{"text": "» VERIFY", "callback_data": f"v:{code}"}])

    poster_storage_id = get_setting("poster_storage_id", "")
    storage_id = get_setting("storage_channel_id", DEFAULT_STORAGE)

    if poster_storage_id:
        tg_call("copyMessage", {
            "chat_id": chat_id,
            "from_chat_id": storage_id,
            "message_id": int(poster_storage_id),
            "caption": caption,
            "parse_mode": "Markdown",
            "reply_markup": {"inline_keyboard": kb}
        })
    else:
        tg_call("sendMessage", {
            "chat_id": chat_id,
            "text": caption,
            "parse_mode": "Markdown",
            "reply_markup": {"inline_keyboard": kb}
        })
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
    sender_mode = "copy"


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

        raw_warn = get_setting("auto_delete_warn_text", DEFAULT_WARN)
        warn_msg = raw_warn.replace("{time}", time_str)

        btn_txt = get_setting("update_btn_text", DEFAULT_UP_BTN)
        btn_url = get_setting("update_btn_url", DEFAULT_UP_URL)

        kb = [[{"text": btn_txt, "url": btn_url}]] if (btn_txt and btn_url and btn_txt.lower() != "none") else []
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
    anim_enabled = get_setting("anim_enabled", "true").lower() == "true"
    anim_base = get_setting("anim_text", DEFAULT_ANIM_TEXT)
    anim_mid = None
    stop_event = threading.Event()

    def animate_loop():
        nonlocal anim_mid
        res = tg_call("sendMessage", {"chat_id": chat_id, "text": f"⏳ {anim_base}."})
        if res.get("ok"):
            anim_mid = res["result"]["message_id"]
            dots = [".", "..", "..."]
            idx = 1
            while not stop_event.is_set():
                time.sleep(0.50)
                if stop_event.is_set():
                    break
                tg_call("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": anim_mid,
                    "text": f"⏳ {anim_base}{dots[idx % 3]}"
                })
                idx += 1

    if anim_enabled:
        t = threading.Thread(target=animate_loop, daemon=True)
        t.start()

    # Membership Check
    unjoined = get_unjoined(user_id)

    if anim_enabled:
        stop_event.set()
        time.sleep(0.05)
        if anim_mid:
            tg_call("deleteMessage", {"chat_id": chat_id, "message_id": anim_mid})

    # Strictly exelseecute actions after animation message deletion
    if not unjoined:
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": fj_mid})
        deliver_file(chat_id, code)
    else:
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": fj_mid})
        tg_call("sendMessage", {"chat_id": chat_id, "text": "❌ Please join all required channels first!"})
        send_fj(chat_id, user_id, code, "")

# --- Sub-Menu Display Functions ---
def show_admin_home(chat_id, mid=None):
    kb = [
        [{"text": "📦 Files", "callback_data": "m_files"}, {"text": "📢 Force Join", "callback_data": "m_fj"}],
        [{"text": "📣 Broadcast", "callback_data": "m_bc"}, {"text": "👥 Users", "callback_data": "m_users"}],
        [{"text": "⚙️ Settings", "callback_data": "m_sett"}]
    ]
    txt = "🎛 **ADMIN CONTROL PANEL**"
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

def show_files_menu(chat_id, mid=None):
    kb = [
        [{"text": "➕ Add File", "callback_data": "f_add"}, {"text": "🗑 Remove File", "callback_data": "f_del"}],
        [{"text": "✏️ Edit File", "callback_data": "f_edit"}, {"text": "📁 List Files", "callback_data": "f_list"}],
        [{"text": "⬅️ Back", "callback_data": "adm_home"}]
    ]
    txt = "📦 **Files Management:**"
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

def show_fj_menu(chat_id, mid=None):
    kb = [
        [{"text": "📝 Caption", "callback_data": "fj_caption"}, {"text": "🖼 Poster (Add/Remove)", "callback_data": "fj_poster"}],
        [{"text": "➕ ADD (Slot)", "callback_data": "fj_add"}, {"text": "✏️ EDIT (Slot)", "callback_data": "fj_edit"}],
        [{"text": "🔘 ON/OFF (Slot)", "callback_data": "fj_onoff"}, {"text": "🗑 REMOVE (Slot)", "callback_data": "fj_rem"}],
        [{"text": "⬅️ Back", "callback_data": "adm_home"}]
    ]
    txt = "📢 **Force Join Configuration:**"
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

def show_settings_menu(chat_id, mid=None):
    w = get_setting("welcome_text", DEFAULT_WELCOME)
    e = get_setting("error_feedback_text", DEFAULT_ERROR)
    t = get_setting("auto_delete_time", "120")
    lay = get_setting("button_layout", "2")
    anim = "ON" if get_setting("anim_enabled", "true").lower() == "true" else "OFF"
    txt = f"⚙️ **Settings Dashboard:**\n\n• Timer: `{t}s` | Layout: `{lay}/row`\n• Animation: `{anim}`\n\n⚠️ **Current Error Format:**\n`{e}`"
    kb = [
        [{"text": "👋 Welcome Msg", "callback_data": "s_wel"}, {"text": "⚠️ Error Msg", "callback_data": "s_err"}],
        [{"text": "⏱️ Delete Timer", "callback_data": "s_timer"}, {"text": "📟 Update Button", "callback_data": "s_upbtn"}],
        [{"text": "🔘 Button Layout", "callback_data": "s_layout"}, {"text": "⏳ Waiting Animation", "callback_data": "s_anim"}],
        [{"text": "⬅️ Back", "callback_data": "adm_home"}]
    ]
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

def show_users_menu(chat_id, mid=None):
    cnt = len(supabase.table("users").select("telegram_user_id").execute().data or []) if supabase else 0
    kb = [
        [{"text": "📜 View Serial List", "callback_data": "u_list"}],
        [{"text": "⬅️ Back", "callback_data": "adm_home"}]
    ]
    txt = f"👥 **Users Overview:**\n\n• Total Active Users: `{cnt}`"
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

@app.route("/", methods=["GET"])
def home():
    return "Bot Core Online 🚀"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200

        if "chat_join_request" in data:
            j_req = data["chat_join_request"]
            chat_id = j_req["chat"]["id"]
            user_id = j_req["from"]["id"]
        
        try:
            supabase.table("join_requests").upsert({
                "user_id": user_id,
                "chat_id": str(chat_id),
                "status": "pending"
            }, on_conflict="user_id,chat_id").execute()
        except Exception as e:
            print(f"Error saving join request: {e}")
            
        return "OK", 200
        # Callback Query Router
    if "callback_query" in data:
        cq = data["callback_query"]
        uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")

        # Verification Button Trigger
        if cdata.startswith("v:"):
            code = cdata.split(":", 1)[1]
            tg_call("answerCallbackQuery", {"callback_query_id": cq["id"]})
            threading.Thread(target=run_verify_process, args=(cid, uid, code, mid), daemon=True).start()
            return "OK", 200

        tg_call("answerCallbackQuery", {"callback_query_id": cq["id"]})

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
                        "active": True
                    }, on_conflict="deep_link_code").execute()
                    tg_call("sendMessage", {"chat_id": cid, "text": f"Success\nLink: https://t.me/LexxiAdminBot?start={sess['c']}"})
                else:
                    tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
                ADMIN_SESSIONS.pop(uid, None)
                show_files_menu(cid)

            elif cdata == "f_del":
                apps = supabase.table("apps").select("title,deep_link_code").order("created_at", desc=True).limit(20).execute().data or [] if supabase else []
                if not apps:
                    tg_call("sendMessage", {"chat_id": cid, "text": "No files found."})
                    show_files_menu(cid)
                else:
                    btns = [[{"text": f"🗑 {a['title']} ({a['deep_link_code']})", "callback_data": f"cf_fdel_{a['deep_link_code']}"}] for a in apps]
                    btns.append([{"text": "⬅️ Back", "callback_data": "m_files"}])
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select file to remove:", "reply_markup": {"inline_keyboard": btns}})

            elif cdata.startswith("cf_fdel_"):
                f_code = cdata.split("_")[2]
                kb = [[{"text": "🗑 Confirm Delete", "callback_data": f"act_fdel_{f_code}"}], [{"text": "❌ Cancel", "callback_data": "m_files"}]]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Permanently delete `{f_code}`?", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

            elif cdata.startswith("act_fdel_"):
                f_code = cdata.split("_")[2]
                if supabase:
                    supabase.table("apps").delete().eq("deep_link_code", f_code).execute()
                    tg_call("sendMessage", {"chat_id": cid, "text": "Deleted"})
                else:
                    tg_call("sendMessage", {"chat_id": cid, "text": "Failed"})
                show_files_menu(cid)

            elif cdata == "f_edit":
                apps = supabase.table("apps").select("title,deep_link_code").order("created_at", desc=True).limit(20).execute().data or [] if supabase else []
                if not apps:
                    tg_call("sendMessage", {"chat_id": cid, "text": "No files found."})
                    show_files_menu(cid)
                else:
                    btns = [[{"text": f"✏️ {a['title']} ({a['deep_link_code']})", "callback_data": f"edopt_{a['deep_link_code']}"}] for a in apps]
                    btns.append([{"text": "⬅️ Back", "callback_data": "m_files"}])
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select file to edit:", "reply_markup": {"inline_keyboard": btns}})

            elif cdata.startswith("edopt_"):
                f_code = cdata.split("_")[1]
                kb = [
                    [{"text": "Title", "callback_data": f"ef_t_{f_code}"}, {"text": "Code", "callback_data": f"ef_c_{f_code}"}],
                    [{"text": "File Post", "callback_data": f"ef_p_{f_code}"}],
                    [{"text": "⬅️ Back", "callback_data": "f_edit"}]
                ]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Edit component for `{f_code}`:", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

            elif cdata.startswith("ef_t_"):
                ADMIN_SESSIONS[uid] = {"step": "F_EDIT_TITLE", "code": cdata.split("_")[2]}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter new title:"})
            elif cdata.startswith("ef_c_"):
                ADMIN_SESSIONS[uid] = {"step": "F_EDIT_CODE", "old_code": cdata.split("_")[2]}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter new short code:"})
            elif cdata.startswith("ef_p_"):
                ADMIN_SESSIONS[uid] = {"step": "F_EDIT_FILE", "code": cdata.split("_")[2]}
                tg_call("sendMessage", {"chat_id": cid, "text": "Forward new post from Storage Channel:"})

            elif cdata == "f_list":
                apps = supabase.table("apps").select("*").order("created_at", desc=True).limit(20).execute().data or [] if supabase else []
                if not apps:
                    tg_call("sendMessage", {"chat_id": cid, "text": "📁 No files registered yet."})
                else:
                    for a in apps:
                        code = a.get('deep_link_code')
                        title = a.get('title')
                        link = f"https://t.me/LexxiAdminBot?start={code}"
                        tg_call("sendMessage", {"chat_id": cid, "text": f"📦 **{title}**\n• Code: `{code}`\n• Link: {link}"})
                show_files_menu(cid)

            # --- Force Join Sub-Module ---
            elif cdata == "fj_caption":
                ADMIN_SESSIONS[uid] = {"step": "SET_FJ_CAPTION"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Send new Force Join caption (`{name}` for dynamic user name):"})

            elif cdata == "fj_poster":
                kb = [
                    [{"text": "➕ Set / Replace Poster", "callback_data": "fjp_add"}],
                    [{"text": "🗑 Remove Poster", "callback_data": "fjp_rem"}],
                    [{"text": "⬅️ Back", "callback_data": "m_fj"}]
                ]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "🖼 **Poster Management:**", "reply_markup": {"inline_keyboard": kb}})

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
                    btns.append([{"text": "⬅️ Back", "callback_data": "m_fj"}])
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select slot to edit:", "reply_markup": {"inline_keyboard": btns}})

            elif cdata.startswith("fje_"):
                ADMIN_SESSIONS[uid] = {"step": "FJ_E_TITLE", "slot": int(cdata.split("_")[1])}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter updated channel title:"})

            elif cdata == "fj_onoff":
                kb = [
                    [{"text": "🟢 Turn ON Channels", "callback_data": "fj_list_to_on"}],
                    [{"text": "🔴 Turn OFF Channels", "callback_data": "fj_list_to_off"}],
                    [{"text": "⬅️ Back", "callback_data": "m_fj"}]
                ]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select toggle operation:", "reply_markup": {"inline_keyboard": kb}})

            elif cdata == "fj_list_to_on":
                chs = [c for c in get_channels() if not c.get("active")]
                if not chs:
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "No inactive channels found.", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ Back", "callback_data": "fj_onoff"}]]}})
                else:
                    btns = [[{"text": f"Turn ON: Slot {c['slot']} ({c.get('button_text')})", "callback_data": f"cf_on_{c['slot']}"}] for c in chs]
                    btns.append([{"text": "⬅️ Back", "callback_data": "fj_onoff"}])
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select channel to enable:", "reply_markup": {"inline_keyboard": btns}})

            elif cdata == "fj_list_to_off":
                chs = [c for c in get_channels() if c.get("active")]
                if not chs:
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "No active channels found.", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ Back", "callback_data": "fj_onoff"}]]}})
                else:
                    btns = [[{"text": f"Turn OFF: Slot {c['slot']} ({c.get('button_text')})", "callback_data": f"cf_off_{c['slot']}"}] for c in chs]
                    btns.append([{"text": "⬅️ Back", "callback_data": "fj_onoff"}])
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select channel to disable:", "reply_markup": {"inline_keyboard": btns}})

            elif cdata.startswith("cf_on_"):
                s = int(cdata.split("_")[2])
                kb = [[{"text": "✅ Confirm Turn ON", "callback_data": f"act_on_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_onoff"}]]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Enable Slot {s}?", "reply_markup": {"inline_keyboard": kb}})

            elif cdata.startswith("cf_off_"):
                s = int(cdata.split("_")[2])
                kb = [[{"text": "✅ Confirm Turn OFF", "callback_data": f"act_off_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_onoff"}]]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Disable Slot {s}?", "reply_markup": {"inline_keyboard": kb}})

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
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "No channels available to remove.", "reply_markup": {"inline_keyboard": [[{"text": "⬅️ Back", "callback_data": "m_fj"}]]}})
                else:
                    btns = [[{"text": f"Remove Slot {c['slot']} ({c.get('button_text')})", "callback_data": f"cf_del_{c['slot']}"}] for c in chs]
                    btns.append([{"text": "⬅️ Back", "callback_data": "m_fj"}])
                    tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select channel to remove:", "reply_markup": {"inline_keyboard": btns}})

            elif cdata.startswith("cf_del_"):
                s = int(cdata.split("_")[2])
                kb = [[{"text": "🗑 Confirm Delete", "callback_data": f"act_del_{s}"}], [{"text": "❌ Cancel", "callback_data": "fj_rem"}]]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"Permanently delete Slot {s}?", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

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
            elif cdata == "s_timer":
                kb = [
                    [{"text": "30s", "callback_data": "st_30"}, {"text": "1 min", "callback_data": "st_60"}],
                    [{"text": "2 min (Default)", "callback_data": "st_120"}, {"text": "3 min", "callback_data": "st_180"}],
                    [{"text": "5 min", "callback_data": "st_300"}, {"text": "Custom", "callback_data": "st_custom"}],
                    [{"text": "⬅️ Back", "callback_data": "m_sett"}]
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
                    [{"text": "1 Button Per Row", "callback_data": "setlay_1"}],
                    [{"text": "2 Buttons Per Row", "callback_data": "setlay_2"}],
                    [{"text": "⬅️ Back", "callback_data": "m_sett"}]
                ]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Select Force Join button layout:", "reply_markup": {"inline_keyboard": kb}})
            elif cdata.startswith("setlay_"):
                set_setting("button_layout", cdata.split("_")[1])
                tg_call("sendMessage", {"chat_id": cid, "text": "Success"})
                show_settings_menu(cid)
            elif cdata == "s_anim":
                kb = [
                    [{"text": "🟢 Turn ON", "callback_data": "anim_on"}, {"text": "🔴 Turn OFF", "callback_data": "anim_off"}],
                    [{"text": "✏️ Change Base Text", "callback_data": "anim_text"}],
                    [{"text": "⬅️ Back", "callback_data": "m_sett"}]
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
            elif cdata == "anim_text":
                ADMIN_SESSIONS[uid] = {"step": "SET_ANIM_TEXT"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Enter new base animated text (e.g. `Verifying`):"})

        return "OK", 200

    # Message Router
    if "message" in data:
        msg = data["message"]
        cid, uid, txt = msg["chat"]["id"], msg["from"]["id"], msg.get("text", "")
        uname = msg["from"].get("first_name", "User")
        mid = msg["message_id"]

        # Track User in Supabase
        if supabase and uid:
            try:
                supabase.table("users").upsert({
                    "telegram_user_id": uid,
                    "username": msg["from"].get("username", ""),
                    "first_name": uname
                }).execute()
            except Exception:
                pass

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
            if st == "F_TITLE":
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
                tg_call("sendMessage", {"chat_id": cid, "text": "Choose delivery mode:", "reply_markup": {"inline_keyboard": kb}})

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
                tg_call("sendMessage", {"chat_id": cid, "text": "Broadcast preview recorded. Confirm delivery?", "reply_markup": {"inline_keyboard": kb}})

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
            return "OK", 200

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

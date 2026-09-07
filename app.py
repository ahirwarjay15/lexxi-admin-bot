import os
import re
import time
import threading
import requests
from flask import Flask, request
from supabase import create_client, Client

app = Flask(__name__)

# Config
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = [int(i.strip()) for i in os.environ.get("ADMIN_IDS", "").split(",") if i.strip()]
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        pass

ADMIN_SESSIONS = {}

# Exact Styled Defaults from conversation
DEFAULT_WELCOME = ">> LOOKING FOR HACKS, ID'S OR ANYTHING ELSE?\nMESSAGE [@lexxiowner](https://t.me/lexxiowner) — WE MIGHT HAVE IT. ⚡"
DEFAULT_ERROR = ">> FACING ANY ISSUE?\nCONTACT [@lexxiowner](https://t.me/lexxiowner) FOR HELP. ⚡"
DEFAULT_FJ_TEXT = ">> HEY {name} ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T JOINED TO\nOUR CHANNELS YET,"
DEFAULT_POSTER = "https://t.me/c/3953020663/26"
DEFAULT_STORAGE_CH = "-1004430375220"
DEFAULT_UPDATE_URL = "https://t.me/lexxiowner"
DEFAULT_VERIFY_BTN = "» VERIFY"

# Clean text channels without extra emojis
DEFAULT_CHANNELS = [
    {"slot": 1, "chat_id": -1001875503950, "button_text": "ʟᴇxɪ ᴍᴏᴅs", "join_url": "https://t.me/+JjB0fpWk8dAwZTBl", "active": True},
    {"slot": 2, "chat_id": -1002991207510, "button_text": "ɪ'ᴅ sᴛᴏʀᴇ", "join_url": "https://t.me/+axSZFBZ5ztVkMTZl", "active": True},
    {"slot": 3, "chat_id": -1003992674272, "button_text": "ғʀᴇᴇ ʜᴀᴄᴋ", "join_url": "https://t.me/BGMIHackLexi", "active": True},
    {"slot": 4, "chat_id": -1004335377904, "button_text": "ʙᴀᴄᴋᴜᴘ", "join_url": "https://t.me/+G7-o7RSFmw8zOTI1", "active": True}
]

def clean_name(name):
    return re.sub(r'([_*\[\]()~`>#+=|{}.!-])', r'\\\1', str(name or "User"))

def send_tg_request(method, data):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        r = requests.post(url, json=data, timeout=10)
        res = r.json()
        if not res.get("ok"):
            if "parse_mode" in data:
                fallback = data.copy()
                fallback.pop("parse_mode", None)
                if "caption" in fallback:
                    fallback["caption"] = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', fallback["caption"]).replace("\\", "")
                if "text" in fallback:
                    fallback["text"] = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', fallback["text"]).replace("\\", "")
                return requests.post(url, json=fallback, timeout=10).json()
        return res
    except Exception:
        return {}

def get_setting(key, default=""):
    if not supabase:
        return default
    try:
        res = supabase.table("settings").select("value").eq("key", key).execute()
        if res.data and len(res.data) > 0:
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
    channels = []
    if supabase:
        try:
            res = supabase.table("force_join_channels").select("*").order("slot").execute()
            channels = res.data or []
        except Exception:
            pass
    return channels or DEFAULT_CHANNELS

def get_unjoined_channels(user_id):
    channels = get_channels()
    unjoined = []
    for c in channels:
        if not c.get("active") or not c.get("chat_id"):
            continue
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
            r = requests.post(url, json={"chat_id": c["chat_id"], "user_id": user_id}, timeout=4).json()
            if not r.get("ok") or r.get("result", {}).get("status") in ["left", "kicked"]:
                unjoined.append(c)
        except Exception:
            unjoined.append(c)
    return unjoined

def send_or_update_force_join(chat_id, user_id, deep_link, user_name="User", old_mid=None):
    unjoined = get_unjoined_channels(user_id)
    if not unjoined:
        return True

    if old_mid:
        send_tg_request("deleteMessage", {"chat_id": chat_id, "message_id": old_mid})

    btn_text = get_setting("verify_button_text", DEFAULT_VERIFY_BTN)
    raw_caption = get_setting("force_join_text", DEFAULT_FJ_TEXT)
    safe = clean_name(user_name)
    fj_caption = raw_caption.replace("{name}", safe) if "{name}" in raw_caption else raw_caption

    poster_url = get_setting("force_join_poster", "")
    keyboard = []
    row = []
    for c in unjoined:
        title = c.get("button_text") or "Channel"
        clean_title = title.replace("↗", "").replace("↗️", "").strip()
        row.append({"text": clean_title, "url": c.get("join_url")})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([{"text": btn_text, "callback_data": f"verify:{deep_link}"}])
    reply_markup = {"inline_keyboard": keyboard}

    has_photo = bool(poster_url and poster_url.lower() != "none" and not poster_url.startswith("https://t.me/c/"))

    if has_photo:
        send_tg_request("sendPhoto", {"chat_id": chat_id, "photo": poster_url, "caption": fj_caption, "parse_mode": "Markdown", "reply_markup": reply_markup})
    else:
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": fj_caption, "parse_mode": "Markdown", "reply_markup": reply_markup})
    return False

def auto_delete_task(chat_id, mids, delay=300):
    time.sleep(delay)
    for m in mids:
        if m:
            send_tg_request("deleteMessage", {"chat_id": chat_id, "message_id": m})

def deliver_file(chat_id, deep_link):
    app_data = None
    if supabase:
        try:
            res = supabase.table("apps").select("*").eq("deep_link_code", deep_link).eq("active", True).execute()
            if res.data:
                app_data = res.data[0]
        except Exception:
            pass

    error_msg = get_setting("error_feedback_text", DEFAULT_ERROR)
    if not app_data:
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": error_msg, "parse_mode": "Markdown"})
        return

    storage_channel = get_setting("storage_channel_id", DEFAULT_STORAGE_CH)
    storage_msg_id = app_data.get("storage_message_id")

    fwd_res = send_tg_request("copyMessage", {"chat_id": chat_id, "from_chat_id": storage_channel, "message_id": storage_msg_id})
    if not fwd_res.get("ok"):
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": error_msg, "parse_mode": "Markdown"})
        return

    mids_to_delete = [fwd_res.get("result", {}).get("message_id")]
    pwd = app_data.get("password")
    if pwd:
        p_res = send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"🔑 **ᴘᴀssᴡᴏʀᴅ:** `{pwd}`", "parse_mode": "Markdown"})
        if p_res.get("ok"):
            mids_to_delete.append(p_res.get("result", {}).get("message_id"))

    warn_text = "⚠️ **ɪᴍᴘᴏʀᴛᴀɴᴛ ɴᴏᴛɪᴄᴇ:**\n\n_ᴀʟʟ ᴍᴇssᴀɢᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ᴀғᴛᴇʀ 𝟻 ᴍɪɴᴜᴛᴇs.\nᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ/sᴀᴠᴇ ᴛʜɪs ғɪʟᴇ ᴛᴏ ʏᴏᴜʀ sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs!_"
    warn_kb = [[{"text": "📟 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ", "url": get_setting("update_channel_url", DEFAULT_UPDATE_URL)}]]
    w_res = send_tg_request("sendMessage", {"chat_id": chat_id, "text": warn_text, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": warn_kb}})
    if w_res.get("ok"):
        mids_to_delete.append(w_res.get("result", {}).get("message_id"))

    threading.Thread(target=auto_delete_task, args=(chat_id, mids_to_delete, 300), daemon=True).start()
def show_admin_panel(chat_id, message_id=None):
    keyboard = [
        [{"text": "➕ Add File", "callback_data": "adm_add_app"}, {"text": "🗑️ Delete File", "callback_data": "adm_del_app"}],
        [{"text": "📁 List Files", "callback_data": "adm_list_apps"}],
        [{"text": "📢 Slots On/Off", "callback_data": "adm_fj_menu"}, {"text": "👥 Total Users", "callback_data": "adm_stats"}],
        [{"text": "📣 Broadcast", "callback_data": "adm_broadcast"}, {"text": "⚙️ Config / Settings", "callback_data": "adm_settings"}]
    ]
    text = "🎛 **ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟ ᴘᴀɴᴇʟ**"
    if message_id:
        send_tg_request("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": keyboard}})
    else:
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": keyboard}})

@app.route("/", methods=["GET"])
def home():
    return "Bot Server Online 🚀"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200

    if "callback_query" in data:
        cq = data["callback_query"]
        cq_id = cq["id"]
        from_user = cq["from"]
        user_id = from_user["id"]
        user_name = from_user.get("first_name", "User")
        chat_id = cq["message"]["chat"]["id"]
        message_id = cq["message"]["message_id"]
        cb_data = cq.get("data", "")

        if cb_data.startswith("verify:"):
            deep_link = cb_data.split(":", 1)[1]
            unjoined = get_unjoined_channels(user_id)
            if not unjoined:
                send_tg_request("answerCallbackQuery", {"callback_query_id": cq_id, "text": "✅ Verified Successfully!"})
                send_tg_request("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
                deliver_file(chat_id, deep_link)
            else:
                send_tg_request("answerCallbackQuery", {"callback_query_id": cq_id, "text": "❌ Pehle sabhi channels join karein!", "show_alert": True})
                send_or_update_force_join(chat_id, user_id, deep_link, user_name=user_name, old_mid=message_id)
            return "OK", 200

        send_tg_request("answerCallbackQuery", {"callback_query_id": cq_id})

        if user_id in ADMIN_IDS:
            if cb_data == "adm_main":
                show_admin_panel(chat_id, message_id)
            elif cb_data == "adm_fj_menu":
                channels = get_channels()
                btns = []
                for c in channels:
                    status = "🟢 ON" if c.get("active") else "🔴 OFF"
                    btns.append([{"text": f"Slot {c['slot']} ({status})", "callback_data": f"slot_tog_{c['slot']}"}])
                btns.append([{"text": "⬅️ Back", "callback_data": "adm_main"}])
                send_tg_request("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": "Slots Toggle (Tap to switch):", "reply_markup": {"inline_keyboard": btns}})
            elif cb_data.startswith("slot_tog_"):
                s = int(cb_data.split("_")[2])
                if supabase:
                    cur = supabase.table("force_join_channels").select("active").eq("slot", s).execute().data
                    v = not cur[0]["active"] if cur else False
                    supabase.table("force_join_channels").update({"active": v}).eq("slot", s).execute()
                cq["data"] = "adm_fj_menu"
                return webhook()
            elif cb_data == "adm_list_apps":
                apps = []
                if supabase:
                    try:
                        apps = supabase.table("apps").select("*").order("created_at", desc=True).limit(8).execute().data or []
                    except Exception:
                        pass
                if not apps:
                    send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📁 Database me koi file nahi mili."})
                else:
                    for a in apps:
                        code = a.get('deep_link_code')
                        title = a.get('title')
                        link = f"https://t.me/LexxiAdminBot?start={code}"
                        kb = [[{"text": f"🗑️ Delete `{code}`", "callback_data": f"qdel_{code}"}]]
                        send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"📦 **{title}**\n• Code: `{code}`\n• Link: {link}", "reply_markup": {"inline_keyboard": kb}})
            elif cb_data.startswith("qdel_"):
                del_code = cb_data.split("_")[1]
                if supabase:
                    try:
                        supabase.table("apps").delete().eq("deep_link_code", del_code).execute()
                    except Exception:
                        pass
                send_tg_request("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": f"🗑️ File `{del_code}` deleted!"})
            elif cb_data == "adm_add_app":
                ADMIN_SESSIONS[user_id] = {"step": "APP_TITLE"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📝 File ka **Title** bhejo:"})
            elif cb_data == "adm_del_app":
                ADMIN_SESSIONS[user_id] = {"step": "DEL_APP"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "🗑️ File ka **Unique Code** bhejo:"})
            elif cb_data == "adm_stats":
                cnt = len(supabase.table("users").select("telegram_user_id").execute().data or []) if supabase else 0
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"👥 **Total Users:** `{cnt}`"})
            elif cb_data == "adm_broadcast":
                ADMIN_SESSIONS[user_id] = {"step": "BROADCAST"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📣 Broadcast message bhejo:"})
            elif cb_data == "adm_settings":
                welcome_txt = get_setting("welcome_text", DEFAULT_WELCOME)
                err_txt = get_setting("error_feedback_text", DEFAULT_ERROR)
                fj_text = get_setting("force_join_text", DEFAULT_FJ_TEXT)
                storage_ch = get_setting("storage_channel_id", DEFAULT_STORAGE_CH)
                panel_text = (
                    "⚙️ **ʙᴏᴛ ᴄᴏɴғɪɢᴜʀᴀᴛɪᴏɴs**\n\n"
                    "👋 **Welcome Msg:**\n" + str(welcome_txt) + "\n\n"
                    "⚠️ **Error Msg:**\n" + str(err_txt) + "\n\n"
                    "📦 **Storage Channel ID:** `" + str(storage_ch) + "`\n\n"
                    "📢 **Force Join Caption:**\n" + str(fj_text)
                )
                keyboard = [
                    [{"text": "👋 Edit Welcome", "callback_data": "set_welcome"}, {"text": "⚠️ Edit Error", "callback_data": "set_error"}],
                    [{"text": "📝 Edit FJ Caption", "callback_data": "set_fj"}, {"text": "📦 Set Storage ID", "callback_data": "set_storage"}],
                    [{"text": "⬅️ Back", "callback_data": "adm_main"}]
                ]
                send_tg_request("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": panel_text, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": keyboard}})
            elif cb_data == "set_welcome":
                ADMIN_SESSIONS[user_id] = {"step": "SET_WELCOME"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Welcome message bhejo:"})
            elif cb_data == "set_error":
                ADMIN_SESSIONS[user_id] = {"step": "SET_ERROR"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Error message bhejo:"})
            elif cb_data == "set_fj":
                ADMIN_SESSIONS[user_id] = {"step": "SET_FJ"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya FJ Caption bhejo (`{name}` dynamic name ke liye):"})
            elif cb_data == "set_storage":
                ADMIN_SESSIONS[user_id] = {"step": "SET_STORAGE"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Storage Channel ID bhejo:"})
        return "OK", 200

    if "message" in data:
        msg = data["message"]
        chat_id = msg["chat"]["id"]
        from_user = msg.get("from", {})
        user_id = from_user.get("id")
        user_name = from_user.get("first_name", "User")
        text = msg.get("text", "")

        if supabase and user_id:
            try:
                supabase.table("users").upsert({"telegram_user_id": user_id, "username": from_user.get("username", "")}).execute()
            except Exception:
                pass

        if text and text.lower().strip() == "/admin" and user_id in ADMIN_IDS:
            ADMIN_SESSIONS.pop(user_id, None)
            show_admin_panel(chat_id)
            return "OK", 200

        if user_id in ADMIN_IDS and user_id in ADMIN_SESSIONS:
            sess = ADMIN_SESSIONS[user_id]
            step = sess.get("step")

            if step == "APP_TITLE":
                sess["title"] = text
                sess["step"] = "APP_CODE"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Short Code bhejo (jaise `1`, `ep01`):"})
                return "OK", 200
            elif step == "APP_CODE":
                sess["code"] = text.strip()
                sess["step"] = "APP_FILE"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Storage Channel se file yahan **FORWARD** karein:"})
                return "OK", 200
            elif step == "APP_FILE":
                fwd_mid = msg.get("forward_from_message_id")
                fwd_chat = msg.get("forward_from_chat", {})
                if not fwd_mid:
                    send_tg_request("sendMessage", {"chat_id": chat_id, "text": "⚠️ Kripya Storage channel se seedha FORWARD karein."})
                    return "OK", 200
                sess["storage_id"] = fwd_mid
                if fwd_chat and fwd_chat.get("id"):
                    set_setting("storage_channel_id", str(fwd_chat.get("id")))
                sess["step"] = "APP_PASS"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Password bhejein (ya `none` likhein):"})
                return "OK", 200
            elif step == "APP_PASS":
                pwd = "" if text.strip().lower() == "none" else text.strip()
                if supabase:
                    supabase.table("apps").upsert({"title": sess["title"], "deep_link_code": sess["code"], "storage_message_id": sess["storage_id"], "password": pwd, "active": True}, on_conflict="deep_link_code").execute()
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"✅ Saved! Link: `https://t.me/LexxiAdminBot?start={sess['code']}`", "parse_mode": "Markdown"})
                return "OK", 200
            elif step == "DEL_APP":
                if supabase:
                    supabase.table("apps").delete().eq("deep_link_code", text.strip()).execute()
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"✅ Code `{text.strip()}` deleted!"})
                show_admin_panel(chat_id)
                return "OK", 200
            elif step == "SET_WELCOME":
                set_setting("welcome_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Welcome message updated!"})
                show_admin_panel(chat_id)
                return "OK", 200
            elif step == "SET_ERROR":
                set_setting("error_feedback_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Error message updated!"})
                show_admin_panel(chat_id)
                return "OK", 200
            elif step == "SET_FJ":
                set_setting("force_join_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Force join caption updated!"})
                show_admin_panel(chat_id)
                return "OK", 200
            elif step == "SET_STORAGE":
                set_setting("storage_channel_id", text.strip())
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"✅ Storage ID set: `{text.strip()}`"})
                show_admin_panel(chat_id)
                return "OK", 200
            elif step == "BROADCAST":
                ADMIN_SESSIONS.pop(user_id, None)
                users = supabase.table("users").select("telegram_user_id").execute().data or [] if supabase else []
                for u in users:
                    send_tg_request("sendMessage", {"chat_id": u["telegram_user_id"], "text": text})
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📢 Broadcast complete!"})
                return "OK", 200

        if text.startswith("/start"):
            args = text.split()
            if len(args) > 1:
                deep_link = args[1].strip()
                if send_or_update_force_join(chat_id, user_id, deep_link, user_name=user_name):
                    deliver_file(chat_id, deep_link)
            else:
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": get_setting("welcome_text", DEFAULT_WELCOME), "parse_mode": "Markdown"})
            return "OK", 200

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

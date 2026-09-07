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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
ADMIN_SESSIONS = {}

# Exact Styled Defaults
DEFAULT_WELCOME = ">> LOOKING FOR HACKS, ID'S OR ANYTHING ELSE?\nMESSAGE [@lexxiowner](https://t.me/lexxiowner) — WE MIGHT HAVE IT. ⚡"
DEFAULT_ERROR = ">> FACING ANY ISSUE?\nCONTACT [@lexxiowner](https://t.me/lexxiowner) FOR HELP. ⚡"
DEFAULT_FJ_TEXT = ">> HEY {name} ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T JOINED TO\nOUR CHANNELS YET,"
DEFAULT_POSTER = "https://t.me/c/3953020663/26"
DEFAULT_STORAGE_CH = "-1004430375220"
DEFAULT_UPDATE_URL = "https://t.me/lexxiowner"
DEFAULT_VERIFY_BTN = "» VERIFY"
DEFAULT_DELETE_DELAY = "300"  # 5 minutes

DEFAULT_CHANNELS = [
    {"slot": 1, "chat_id": -1001875503950, "button_text": "ʟᴇxɪ ᴍᴏᴅs", "join_url": "https://t.me/+JjB0fpWk8dAwZTBl", "active": True},
    {"slot": 2, "chat_id": -1002991207510, "button_text": "ɪ'ᴅ sᴛᴏʀᴇ", "join_url": "https://t.me/+axSZFBZ5ztVkMTZl", "active": True},
    {"slot": 3, "chat_id": -1003992674272, "button_text": "ғʀᴇᴇ ʜᴀᴄᴋ", "join_url": "https://t.me/BGMIHackLexi", "active": True},
    {"slot": 4, "chat_id": -1004335377904, "button_text": "ʙᴀᴄᴋᴜᴘ", "join_url": "https://t.me/+G7-o7RSFmw8zOTI1", "active": True}
]

def init_defaults(force_reset=False):
    try:
        defaults = [
            ("welcome_text", DEFAULT_WELCOME),
            ("error_feedback_text", DEFAULT_ERROR),
            ("force_join_text", DEFAULT_FJ_TEXT),
            ("force_join_poster", DEFAULT_POSTER),
            ("storage_channel_id", DEFAULT_STORAGE_CH),
            ("update_channel_url", DEFAULT_UPDATE_URL),
            ("verify_button_text", DEFAULT_VERIFY_BTN),
            ("auto_delete_delay", DEFAULT_DELETE_DELAY)
        ]
        for k, v in defaults:
            existing = supabase.table("settings").select("value").eq("key", k).execute()
            if not existing.data or force_reset:
                supabase.table("settings").upsert({"key": k, "value": v}).execute()

        ch_res = supabase.table("force_join_channels").select("slot").execute()
        existing_slots = [c["slot"] for c in (ch_res.data or [])]
        for ch in DEFAULT_CHANNELS:
            if ch["slot"] not in existing_slots or force_reset:
                supabase.table("force_join_channels").upsert(ch, on_conflict="slot").execute()
    except Exception as e:
        print(f"Init defaults error: {e}")

init_defaults()

def clean_markdown_name(name):
    if not name:
        return "User"
    return re.sub(r'([_*\[\]()~`>#+=|{}.!-])', r'\\\1', str(name))

def send_tg_request(method, data):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        r = requests.post(url, json=data, timeout=10)
        res = r.json()
        if not res.get("ok"):
            if "parse_mode" in data and "can't parse entities" in res.get("description", ""):
                fallback = data.copy()
                fallback.pop("parse_mode", None)
                if "caption" in fallback:
                    fallback["caption"] = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', fallback["caption"]).replace("\\", "")
                if "text" in fallback:
                    fallback["text"] = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', fallback["text"]).replace("\\", "")
                return requests.post(url, json=fallback, timeout=10).json()
        return res
    except Exception as e:
        print(f"Error calling {method}: {e}")
        return {}

def get_setting(key, default=""):
    try:
        res = supabase.table("settings").select("value").eq("key", key).execute()
        if res.data and len(res.data) > 0:
            return res.data[0].get("value", default)
    except Exception as e:
        print(f"Error fetching setting {key}: {e}")
    return default

def set_setting(key, value):
    try:
        supabase.table("settings").upsert({"key": key, "value": str(value)}).execute()
    except Exception as e:
        print(f"Error updating setting {key}: {e}")

def get_unjoined_channels(user_id):
    try:
        res = supabase.table("force_join_channels").select("*").eq("active", True).order("slot").execute()
        channels = res.data or []
    except Exception as e:
        print(f"Error fetching channels: {e}")
        return []

    unjoined = []
    for c in channels:
        chat_id = c.get("chat_id")
        if not chat_id:
            continue
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
            r = requests.post(url, json={"chat_id": chat_id, "user_id": user_id}, timeout=5).json()
            if not r.get("ok") or r.get("result", {}).get("status") in ["left", "kicked"]:
                unjoined.append(c)
        except Exception:
            unjoined.append(c)
    return unjoined

def send_or_update_force_join(chat_id, user_id, deep_link, user_name="User", message_id=None):
    unjoined = get_unjoined_channels(user_id)
    if not unjoined:
        return True

    btn_text = get_setting("verify_button_text", DEFAULT_VERIFY_BTN)
    raw_caption = get_setting("force_join_text", DEFAULT_FJ_TEXT)
    
    safe_name = clean_markdown_name(user_name)
    fj_caption = raw_caption.replace("{name}", safe_name) if "{name}" in raw_caption else raw_caption

    poster_url = get_setting("force_join_poster", "")

    keyboard = []
    row = []
    for c in unjoined:
        title = c.get("button_text") or "Channel"
        row.append({"text": f"{title} ↗", "url": c.get("join_url")})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([{"text": btn_text, "callback_data": f"verify:{deep_link}"}])
    reply_markup = {"inline_keyboard": keyboard}

    has_photo = bool(poster_url and poster_url.lower() != "none" and not poster_url.startswith("https://t.me/c/"))

    if message_id:
        if has_photo:
            send_tg_request("editMessageCaption", {
                "chat_id": chat_id,
                "message_id": message_id,
                "caption": fj_caption,
                "parse_mode": "Markdown",
                "reply_markup": reply_markup
            })
        else:
            send_tg_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": fj_caption,
                "parse_mode": "Markdown",
                "reply_markup": reply_markup
            })
    else:
        if has_photo:
            send_tg_request("sendPhoto", {
                "chat_id": chat_id,
                "photo": poster_url,
                "caption": fj_caption,
                "parse_mode": "Markdown",
                "reply_markup": reply_markup
            })
        else:
            send_tg_request("sendMessage", {
                "chat_id": chat_id,
                "text": fj_caption,
                "parse_mode": "Markdown",
                "reply_markup": reply_markup
            })
    return False

def auto_delete_task(chat_id, mids, delay):
    if delay <= 0:
        return
    time.sleep(delay)
    for m in mids:
        if m:
            send_tg_request("deleteMessage", {"chat_id": chat_id, "message_id": m})

def deliver_file(chat_id, deep_link):
    res = supabase.table("apps").select("*").eq("deep_link_code", deep_link).eq("active", True).execute()
    error_msg = get_setting("error_feedback_text", DEFAULT_ERROR)
    
    if not res.data:
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": error_msg, "parse_mode": "Markdown"})
        return

    app_data = res.data[0]
    storage_channel = get_setting("storage_channel_id", DEFAULT_STORAGE_CH)
    storage_msg_id = app_data.get("storage_message_id")

    fwd_res = send_tg_request("copyMessage", {
        "chat_id": chat_id,
        "from_chat_id": storage_channel,
        "message_id": storage_msg_id
    })

    if not fwd_res.get("ok"):
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": error_msg, "parse_mode": "Markdown"})
        return

    file_mid = fwd_res.get("result", {}).get("message_id")
    mids_to_delete = [file_mid] if file_mid else []

    pwd = app_data.get("password")
    if pwd:
        p_res = send_tg_request("sendMessage", {
            "chat_id": chat_id, 
            "text": f"🔑 **ᴘᴀssᴡᴏʀᴅ:** `{pwd}`", 
            "parse_mode": "Markdown"
        })
        if p_res.get("ok"):
            mids_to_delete.append(p_res.get("result", {}).get("message_id"))

    # Configurable Auto Delete Delay
    delay_sec = int(get_setting("auto_delete_delay", DEFAULT_DELETE_DELAY))
    mins = delay_sec // 60

    if delay_sec > 0:
        time_text = f"{mins} minutes" if mins > 0 else f"{delay_sec} seconds"
        warn_text = (
            "⚠️ **ɪᴍᴘᴏʀᴛᴀɴᴛ ɴᴏᴛɪᴄᴇ:**\n\n"
            f"_ᴀʟʟ ᴍᴇssᴀɢᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ᴀғᴛᴇʀ {time_text}.\n"
            "ᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ/sᴀᴠᴇ ᴛʜɪs ғɪʟᴇ ᴛᴏ ʏᴏᴜʀ sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs!_"
        )
        update_url = get_setting("update_channel_url", DEFAULT_UPDATE_URL)
        warn_kb = [[{"text": "📟 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ", "url": update_url}]]
        w_res = send_tg_request("sendMessage", {
            "chat_id": chat_id,
            "text": warn_text,
            "parse_mode": "Markdown",
            "reply_markup": {"inline_keyboard": warn_kb}
        })
        if w_res.get("ok"):
            mids_to_delete.append(w_res.get("result", {}).get("message_id"))

        threading.Thread(target=auto_delete_task, args=(chat_id, mids_to_delete, delay_sec), daemon=True).start()

def show_admin_panel(chat_id, message_id=None):
    keyboard = [
        [{"text": "➕ Add File", "callback_data": "adm_add_app"}, {"text": "🗑️ Delete File", "callback_data": "adm_del_app"}],
        [{"text": "📁 Manage Files", "callback_data": "adm_list_apps"}],
        [{"text": "📢 Force Join Slots (Edit/Remove)", "callback_data": "adm_fj_menu"}],
        [{"text": "⏱️ Auto-Delete Timer", "callback_data": "adm_timer_menu"}, {"text": "👥 User Stats", "callback_data": "adm_stats"}],
        [{"text": "📣 Broadcast", "callback_data": "adm_broadcast"}, {"text": "⚙️ Settings & Messages", "callback_data": "adm_settings"}]
    ]
    text = "🎛 **ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟ ᴘᴀɴᴇʟ**\n\nManage files, force-join channels, auto-delete timers, and bot messages:"
    if message_id:
        send_tg_request("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": keyboard}})
    else:
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": keyboard}})

@app.route("/", methods=["GET"])
def home():
    return "Bot Server is Online & Healthy 🚀"

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

        send_tg_request("answerCallbackQuery", {"callback_query_id": cq_id})

        if cb_data.startswith("verify:"):
            deep_link = cb_data.split(":", 1)[1]
            unjoined = get_unjoined_channels(user_id)
            if not unjoined:
                send_tg_request("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
                deliver_file(chat_id, deep_link)
            else:
                send_or_update_force_join(chat_id, user_id, deep_link, user_name=user_name, message_id=message_id)

        elif user_id in ADMIN_IDS:
            if cb_data == "adm_main":
                show_admin_panel(chat_id, message_id)
            
            # --- FORCE JOIN MENU & CONTROLS ---
            elif cb_data == "adm_fj_menu":
                channels = supabase.table("force_join_channels").select("*").order("slot").execute().data or []
                btns = []
                for c in channels:
                    status = "🟢 ON" if c.get("active") else "🔴 OFF"
                    name = c.get('button_text', 'Empty')
                    btns.append([{"text": f"Slot {c['slot']}: {name} [{status}]", "callback_data": f"slot_view_{c['slot']}"}])
                btns.append([{"text": "⬅️ Back", "callback_data": "adm_main"}])
                send_tg_request("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": "📢 **Force Join Slots Management:**\nTap a slot below to edit, toggle on/off, or remove it:",
                    "parse_mode": "Markdown",
                    "reply_markup": {"inline_keyboard": btns}
                })

            elif cb_data.startswith("slot_view_"):
                slot = int(cb_data.split("_")[2])
                ch_res = supabase.table("force_join_channels").select("*").eq("slot", slot).execute().data
                ch = ch_res[0] if ch_res else {}
                status_txt = "🟢 Active (Enabled)" if ch.get("active") else "🔴 Inactive (Disabled)"
                info_text = (
                    f"⚙️ **Slot {slot} Configuration**\n\n"
                    f"• **Status:** {status_txt}\n"
                    f"• **Name:** `{ch.get('button_text', 'None')}`\n"
                    f"• **Chat ID:** `{ch.get('chat_id', 'None')}`\n"
                    f"• **Join Link:** `{ch.get('join_url', 'None')}`"
                )
                toggle_txt = "🔴 Turn OFF" if ch.get("active") else "🟢 Turn ON"
                btns = [
                    [{"text": toggle_txt, "callback_data": f"slot_toggle_{slot}"}, {"text": "✏️ Edit Details", "callback_data": f"slot_edit_{slot}"}],
                    [{"text": "🗑️ Clear / Remove Slot", "callback_data": f"slot_clear_{slot}"}],
                    [{"text": "⬅️ Back to Slots", "callback_data": "adm_fj_menu"}]
                ]
                send_tg_request("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": info_text,
                    "parse_mode": "Markdown",
                    "reply_markup": {"inline_keyboard": btns}
                })

            elif cb_data.startswith("slot_toggle_"):
                slot = int(cb_data.split("_")[2])
                ch_res = supabase.table("force_join_channels").select("active").eq("slot", slot).execute().data
                current = ch_res[0]["active"] if ch_res else False
                supabase.table("force_join_channels").update({"active": not current}).eq("slot", slot).execute()
                # Re-render slot view
                cq["data"] = f"slot_view_{slot}"
                webhook()

            elif cb_data.startswith("slot_clear_"):
                slot = int(cb_data.split("_")[2])
                supabase.table("force_join_channels").update({
                    "chat_id": 0,
                    "button_text": "Empty",
                    "join_url": "",
                    "active": False
                }).eq("slot", slot).execute()
                send_tg_request("answerCallbackQuery", {"callback_query_id": cq_id, "text": f"Slot {slot} has been cleared!"})
                # Re-render
                cq["data"] = f"slot_view_{slot}"
                webhook()

            elif cb_data.startswith("slot_edit_"):
                slot = int(cb_data.split("_")[2])
                ADMIN_SESSIONS[user_id] = {"step": "FJ_CONFIG", "slot": slot}
                send_tg_request("sendMessage", {
                    "chat_id": chat_id,
                    "text": f"📝 **Send new details for Slot {slot}:**\n\nFormat:\n`Chat_ID | Button_Name | Invite_Link`\n\nExample:\n`-1001875503950 | ʟᴇxɪ ᴍᴏᴅs | https://t.me/+JjB0fpWk8dAwZTBl`",
                    "parse_mode": "Markdown"
                })

            # --- AUTO DELETE TIMER MENU ---
            elif cb_data == "adm_timer_menu":
                curr_del = get_setting("auto_delete_delay", DEFAULT_DELETE_DELAY)
                btns = [
                    [{"text": "Off (0s)", "callback_data": "timer_set_0"}, {"text": "30 Seconds", "callback_data": "timer_set_30"}],
                    [{"text": "1 Minute", "callback_data": "timer_set_60"}, {"text": "2 Minutes", "callback_data": "timer_set_120"}],
                    [{"text": "5 Minutes (Default)", "callback_data": "timer_set_300"}, {"text": "10 Minutes", "callback_data": "timer_set_600"}],
                    [{"text": "⬅️ Back", "callback_data": "adm_main"}]
                ]
                send_tg_request("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": f"⏱️ **Auto-Delete Timer Settings**\n\nCurrent Duration: `{curr_del} seconds` ({int(curr_del)//60} mins)\nSelect a timer below:",
                    "parse_mode": "Markdown",
                    "reply_markup": {"inline_keyboard": btns}
                })

            elif cb_data.startswith("timer_set_"):
                new_sec = cb_data.split("_")[2]
                set_setting("auto_delete_delay", new_sec)
                send_tg_request("answerCallbackQuery", {"callback_query_id": cq_id, "text": f"Timer set to {new_sec}s!"})
                show_admin_panel(chat_id, message_id)

            # --- FILE MANAGEMENT WITH 1-CLICK DELETE ---
            elif cb_data == "adm_list_apps":
                apps = supabase.table("apps").select("*").order("created_at", desc=True).limit(10).execute().data or []
                if not apps:
                    send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📁 Koi file database me nahi hai."})
                else:
                    for a in apps:
                        code = a['deep_link_code']
                        link = f"https://t.me/LexxiAdminBot?start={code}"
                        kb = [[{"text": f"🗑️ Delete `{code}`", "callback_data": f"quick_del_{code}"}]]
                        send_tg_request("sendMessage", {
                            "chat_id": chat_id,
                            "text": f"📦 **{a['title']}**\n• Code: `{code}`\n• Link: {link}",
                            "reply_markup": {"inline_keyboard": kb}
                        })
                    show_admin_panel(chat_id)

            elif cb_data.startswith("quick_del_"):
                del_code = cb_data.split("_")[2]
                supabase.table("apps").delete().eq("deep_link_code", del_code).execute()
                send_tg_request("editMessageText", {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": f"🗑️ **File with code `{del_code}` has been deleted.**",
                    "parse_mode": "Markdown"
                })

            elif cb_data == "adm_add_app":
                ADMIN_SESSIONS[user_id] = {"step": "APP_TITLE"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📝 File/Content ka **Title** bhejo:"})

            elif cb_data == "adm_del_app":
                ADMIN_SESSIONS[user_id] = {"step": "DEL_APP"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "🗑️ Delete karne ke liye file ka **Unique Code** bhejo:"})

            elif cb_data == "adm_stats":
                count = len(supabase.table("users").select("telegram_user_id").execute().data or [])
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"👥 **ᴛᴏᴛᴀʟ ᴜs

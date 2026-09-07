import os
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

# Aesthetic Small Caps Fonts with Dynamic {name}
DEFAULT_WELCOME = ">> ʟᴏᴏᴋɪɴɢ ғᴏʀ ʜᴀᴄᴋs, ɪᴅs ᴏʀ ᴀɴʏᴛʜɪɴɢ ᴇʟsᴇ?\nᴍᴇssᴀɢᴇ [@lexxiowner](https://t.me/lexxiowner) — ᴡᴇ ᴍɪɢʜᴛ ʜᴀᴠᴇ ɪᴛ. ⚡"
DEFAULT_ERROR = ">> ғᴀᴄɪɴɢ ᴀɴʏ ɪssᴜᴇ?\nᴄᴏɴᴛᴀᴄᴛ [@lexxiowner](https://t.me/lexxiowner) ғᴏʀ ʜᴇʟᴘ. ⚡"
DEFAULT_FJ_TEXT = ">> ʜᴇʏ {name} ×\nʏᴏᴜʀ ғɪʟᴇ ɪs ʀᴇᴀᴅʏ\nʟᴏᴏᴋs ʟɪᴋᴇ ʏᴏᴜ ʜᴀᴠᴇɴ'ᴛ sᴜʙsᴄʀɪʙᴇᴅ ᴛᴏ\nᴏᴜʀ ᴄʜᴀɴɴᴇʟs ʏᴇᴛ,"

def send_tg_request(method, data):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        r = requests.post(url, json=data, timeout=10)
        return r.json()
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
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
        try:
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

    btn_text = get_setting("verify_button_text", "⚡ ᴠᴇʀɪғʏ")
    raw_caption = get_setting("force_join_text", DEFAULT_FJ_TEXT)
    
    # Replace {name} placeholder or inject user name
    if "{name}" in raw_caption:
        fj_caption = raw_caption.replace("{name}", user_name)
    else:
        fj_caption = raw_caption

    poster_url = get_setting("force_join_poster", "")

    keyboard = []
    row = []
    for c in unjoined:
        title = c.get("button_text") or "Channel"
        row.append({"text": f"• {title} ↗", "url": c.get("join_url")})
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([{"text": btn_text, "callback_data": f"verify:{deep_link}"}])
    reply_markup = {"inline_keyboard": keyboard}

    if message_id:
        if poster_url:
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
        if poster_url:
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

def auto_delete_task(chat_id, mids, delay=300):
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
    storage_channel = get_setting("storage_channel_id", "")
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

    update_url = get_setting("update_channel_url", "https://t.me/lexxiowner")
    warn_kb = [[{"text": "📟 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ", "url": update_url}]]
    warn_text = (
        "⚠️ **ɪᴍᴘᴏʀᴛᴀɴᴛ ɴᴏᴛɪᴄᴇ:**\n\n"
        "_ᴀʟʟ ᴍᴇssᴀɢᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ᴀғᴛᴇʀ 𝟻 ᴍɪɴᴜᴛᴇs.\n"
        "ᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ/sᴀᴠᴇ ᴛʜɪs ғɪʟᴇ ᴛᴏ ʏᴏᴜʀ sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs!_"
    )
    w_res = send_tg_request("sendMessage", {
        "chat_id": chat_id,
        "text": warn_text,
        "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": warn_kb}
    })
    if w_res.get("ok"):
        mids_to_delete.append(w_res.get("result", {}).get("message_id"))

    threading.Thread(target=auto_delete_task, args=(chat_id, mids_to_delete), daemon=True).start()

def show_admin_panel(chat_id):
    keyboard = [
        [{"text": "➕ ᴀᴅᴅ ғɪʟᴇ", "callback_data": "adm_add_app"}, {"text": "🗑️ ᴅᴇʟᴇᴛᴇ ғɪʟᴇ", "callback_data": "adm_del_app"}],
        [{"text": "📁 ʟɪsᴛ ғɪʟᴇs", "callback_data": "adm_list_apps"}],
        [{"text": "📢 ғᴏʀᴄᴇ ᴊᴏɪɴ", "callback_data": "adm_fj_menu"}, {"text": "👥 ᴜsᴇʀ sᴛᴀᴛs", "callback_data": "adm_stats"}],
        [{"text": "📣 ʙʀᴏᴀᴅᴄᴀsᴛ", "callback_data": "adm_broadcast"}],
        [{"text": "⚙️ ᴄᴏɴғɪɢ / sᴇᴛᴛɪɴɢs", "callback_data": "adm_settings"}]
    ]
    send_tg_request("sendMessage", {
        "chat_id": chat_id,
        "text": "🎛 **ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟ ᴘᴀɴᴇʟ**\n\nSelect an option below to manage bot configurations:",
        "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": keyboard}
    })

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
                show_admin_panel(chat_id)
            elif cb_data == "adm_add_app":
                ADMIN_SESSIONS[user_id] = {"step": "APP_TITLE"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📝 File/Content ka **Title** bhejo:"})
            elif cb_data == "adm_del_app":
                ADMIN_SESSIONS[user_id] = {"step": "DEL_APP"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "🗑️ Delete karne ke liye file ka **Unique Code** bhejo:"})
            elif cb_data == "adm_list_apps":
                apps = supabase.table("apps").select("*").limit(20).execute().data or []
                text = "📁 **ʀᴇᴄᴇɴᴛʟʏ ᴀᴅᴅᴇᴅ ғɪʟᴇs:**\n\n"
                for a in apps:
                    text += f"• **{a['title']}** (Code: `{a['deep_link_code']}`)\n🔗 Link: `https://t.me/LexxiAdminBot?start={a['deep_link_code']}`\n\n"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": text or "Koi files nahi mili."})
            elif cb_data == "adm_fj_menu":
                channels = supabase.table("force_join_channels").select("*").order("slot").execute().data or []
                btns = []
                for c in channels:
                    status = "🟢" if c.get("active") else "🔴"
                    btns.append([{"text": f"Slot {c['slot']} {status} {c.get('button_text', '')}", "callback_data": f"adm_slot_{c['slot']}"}])
                btns.append([{"text": "⬅️ Back", "callback_data": "adm_main"}])
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📢 **Force Join Channels Setup:**", "reply_markup": {"inline_keyboard": btns}})
            elif cb_data.startswith("adm_slot_"):
                slot = int(cb_data.split("_")[2])
                ADMIN_SESSIONS[user_id] = {"step": "FJ_CONFIG", "slot": slot}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"Slot {slot} ke liye is format me details bhejo:\n`Chat_ID | Button_Name | Invite_Link`\n\nBand karne ke liye `disable` bhejein."})
            elif cb_data == "adm_stats":
                count = len(supabase.table("users").select("telegram_user_id").execute().data or [])
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"👥 **ᴛᴏᴛᴀʟ ᴜsᴇʀs ɪɴ ᴅᴀᴛᴀʙᴀsᴇ:** `{count}`"})
            elif cb_data == "adm_broadcast":
                ADMIN_SESSIONS[user_id] = {"step": "BROADCAST"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "📣 Sabhi users ko broadcast karne wala message bhejo:"})
            elif cb_data == "adm_settings":
                welcome_txt = get_setting("welcome_text", DEFAULT_WELCOME)
                err_txt = get_setting("error_feedback_text", DEFAULT_ERROR)
                fj_text = get_setting("force_join_text", DEFAULT_FJ_TEXT)
                btn_text = get_setting("verify_button_text", "⚡ ᴠᴇʀɪғʏ")
                poster = get_setting("force_join_poster", "None")
                storage_ch = get_setting("storage_channel_id", "Not Configured")
                up_url = get_setting("update_channel_url", "https://t.me/lexxiowner")

                text = (
                    "⚙️ **ʙᴏᴛ ᴄᴏɴғɪɢᴜʀᴀᴛɪᴏɴs**\n\n"
                    f"**👋 Welcome Msg:**\n{welcome_txt}\n\n"
                    f"**⚠️ Error Msg:**\n{err_txt}\n\n"
                    f"**📦 Storage Channel ID:** `{storage_ch}`\n"
                    f"**🔗 Update Channel:** `{up_url}`\n\n"
                    f"**📢 Force Join Caption:**\n{fj_text}\n\n"
                    f"**🔘 Verify Button:** `{btn_text}`\n"
                    f"**🖼️ Poster Link:** `{poster}`"
                )
                keyboard = [
                    [{"text": "👋 Edit Welcome", "callback_data": "set_msg_welcome"}, {"text": "⚠️ Edit Error", "callback_data": "set_msg_error"}],
                    [{"text": "📦 Set Storage ID", "callback_data": "set_msg_storage_ch"}, {"text": "🔗 Set Update Link", "callback_data": "set_msg_up_url"}],
                    [{"text": "📝 Edit FJ Caption", "callback_data": "set_msg_fj_text"}, {"text": "🔘 Edit Verify Button", "callback_data": "set_msg_verify_btn"}],
                    [{"text": "🖼️ Edit Poster Link", "callback_data": "set_msg_poster"}],
                    [{"text": "⬅️ Back", "callback_data": "adm_main"}]
                ]
                send_tg_request("sendMessage", {
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "reply_markup": {"inline_keyboard": keyboard}
                })
            elif cb_data == "set_msg_welcome":
                ADMIN_SESSIONS[user_id] = {"step": "SET_WELCOME_MSG"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Welcome message bhejo:"})
            elif cb_data == "set_msg_error":
                ADMIN_SESSIONS[user_id] = {"step": "SET_ERROR_MSG"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Error message bhejo:"})
            elif cb_data == "set_msg_storage_ch":
                ADMIN_SESSIONS[user_id] = {"step": "SET_STORAGE_CH"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Storage Channel ID bhejo (jaise `-100xxxxxxxxxx`):"})
            elif cb_data == "set_msg_up_url":
                ADMIN_SESSIONS[user_id] = {"step": "SET_UP_URL"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Update Channel ka link bhejo:"})
            elif cb_data == "set_msg_fj_text":
                ADMIN_SESSIONS[user_id] = {"step": "SET_FJ_TEXT"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Force Join message caption bhejo (user name ke liye `{name}` use kar sakte hain):"})
            elif cb_data == "set_msg_verify_btn":
                ADMIN_SESSIONS[user_id] = {"step": "SET_VERIFY_BTN"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Verify Button text bhejo:"})
            elif cb_data == "set_msg_poster":
                ADMIN_SESSIONS[user_id] = {"step": "SET_POSTER"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Poster image URL bhejo (ya `none` bhejo):"})

        return "OK", 200

    if "message" in data:
        msg = data["message"]
        chat_id = msg["chat"]["id"]
        from_user = msg["from"]
        user_id = from_user["id"]
        user_name = from_user.get("first_name", "User")
        username = from_user.get("username", "")
        text = msg.get("text", "")

        try:
            supabase.table("users").upsert({"telegram_user_id": user_id, "username": username}).execute()
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
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Deep-link ke liye ek **Short Code** bhejo (jaise `1`, `ep01`, `hack01`):"})
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
                    send_tg_request("sendMessage", {"chat_id": chat_id, "text": "⚠️ Kripya Storage channel se file seedha FORWARD karein."})
                    return "OK", 200

                sess["storage_id"] = fwd_mid
                if fwd_chat and fwd_chat.get("id"):
                    set_setting("storage_channel_id", str(fwd_chat.get("id")))

                sess["step"] = "APP_PASS"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Password bhejein (agar password nahi rakhna to `none` likhein):"})
                return "OK", 200

            elif step == "APP_PASS":
                pwd = "" if text.strip().lower() == "none" else text.strip()
                supabase.table("apps").upsert({
                    "title": sess["title"],
                    "deep_link_code": sess["code"],
                    "storage_message_id": sess["storage_id"],
                    "password": pwd,
                    "active": True
                }, on_conflict="deep_link_code").execute()
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {
                    "chat_id": chat_id,
                    "text": f"✅ **File Successfully Saved!**\n\n🔗 **Link:** `https://t.me/LexxiAdminBot?start={sess['code']}`",
                    "parse_mode": "Markdown"
                })
                return "OK", 200

            elif step == "DEL_APP":
                code_to_del = text.strip()
                supabase.table("apps").delete().eq("deep_link_code", code_to_del).execute()
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"✅ File code `{code_to_del}` delete kar di gayi hai!"})
                show_admin_panel(chat_id)
                return "OK", 200

            elif step == "SET_STORAGE_CH":
                set_setting("storage_channel_id", text.strip())
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"✅ Storage Channel ID set: `{text.strip()}`"})
                show_admin_panel(chat_id)
                return "OK", 200

            elif step == "SET_UP_URL":
                set_setting("update_channel_url", text.strip())
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Update Channel link update ho gaya!"})
                show_admin_panel(chat_id)
                return "OK", 200

            elif step == "FJ_CONFIG":
                slot = sess["slot"]
                if text.strip().lower() == "disable":
                    supabase.table("force_join_channels").update({"active": False}).eq("slot", slot).execute()
                    send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"Slot {slot} disable kar diya gaya."})
                else:
                    parts = [p.strip() for p in text.split("|")]
                    if len(parts) == 3:
                        c_id, b_text, j_url = int(parts[0]), parts[1], parts[2]
                        supabase.table("force_join_channels").update({
                            "chat_id": c_id,
                            "button_text": b_text,
                            "join_url": j_url,
                            "active": True
                        }).eq("slot", slot).execute()
                        send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"✅ Slot {slot} update ho gaya!"})
                    else:
                        send_tg_request("sendMessage", {"chat_id": chat_id, "text": "⚠️ Format galat tha. Format: `Chat_ID | Name | Link`"})
                ADMIN_SESSIONS.pop(user_id, None)
                return "OK", 200

            elif step == "BROADCAST":
                ADMIN_SESSIONS.pop(user_id, None)
                users = supabase.table("users").select("telegram_user_id").execute().data or []
                sent = 0
                for u in users:
                    res = send_tg_request("sendMessage", {"chat_id": u["telegram_user_id"], "text": text})
                    if res.get("ok"):
                        sent += 1
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"📢 Broadcast complete: {sent} users ko deliver hua."})
                return "OK", 200

            elif step == "SET_WELCOME_MSG":
                set_setting("welcome_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Welcome message update ho gaya!"})
                show_admin_panel(chat_id)
                return "OK", 200

            elif step == "SET_ERROR_MSG":
                set_setting("error_feedback_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Error message update ho gaya!"})
                show_admin_panel(chat_id)
                return "OK", 200

            elif step == "SET_FJ_TEXT":
                set_setting("force_join_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Force join caption update ho gaya!"})
                show_admin_panel(chat_id)
                return "OK", 200

            elif step == "SET_VERIFY_BTN":
                set_setting("verify_button_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Button text update ho gaya!"})
                show_admin_panel(chat_id)
                return "OK", 200

            elif step == "SET_POSTER":
                poster_val = "" if text.lower().strip() == "none" else text.strip()
                set_setting("force_join_poster", poster_val)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Poster link update ho gaya!"})
                show_admin_panel(chat_id)
                return "OK", 200

        # /start handler
        if text.startswith("/start"):
            args = text.split()
            if len(args) > 1:
                deep_link = args[1].strip()
                all_joined = send_or_update_force_join(chat_id, user_id, deep_link, user_name=user_name)
                if all_joined:
                    deliver_file(chat_id, deep_link)
            else:
                welcome_msg = get_setting("welcome_text", DEFAULT_WELCOME)
                send_tg_request("sendMessage", {
                    "chat_id": chat_id, 
                    "text": welcome_msg, 
                    "parse_mode": "Markdown"
                })
            return "OK", 200

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

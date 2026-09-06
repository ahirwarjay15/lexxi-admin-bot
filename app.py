import os
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

def send_or_update_force_join(chat_id, user_id, deep_link, message_id=None):
    unjoined = get_unjoined_channels(user_id)
    if not unjoined:
        return True

    btn_text = get_setting("verify_button_text", "✅ VERIFY")
    fj_caption = get_setting("force_join_text", ">> HEY ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T SUBSCRIBED TO OUR CHANNELS YET,")
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

    if message_id:
        if poster_url:
            send_tg_request("editMessageCaption", {
                "chat_id": chat_id,
                "message_id": message_id,
                "caption": fj_caption,
                "reply_markup": reply_markup
            })
        else:
            send_tg_request("editMessageText", {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": fj_caption,
                "reply_markup": reply_markup
            })
    else:
        if poster_url:
            send_tg_request("sendPhoto", {
                "chat_id": chat_id,
                "photo": poster_url,
                "caption": fj_caption,
                "reply_markup": reply_markup
            })
        else:
            send_tg_request("sendMessage", {
                "chat_id": chat_id,
                "text": fj_caption,
                "reply_markup": reply_markup
            })
    return False

def deliver_file(chat_id, deep_link):
    res = supabase.table("apps").select("*").eq("deep_link_code", deep_link).eq("active", True).execute()
    if not res.data:
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": "File not found or inactive."})
        return

    app_data = res.data[0]
    storage_channel = get_setting("storage_channel_id", "")
    fwd_res = send_tg_request("copyMessage", {
        "chat_id": chat_id,
        "from_chat_id": storage_channel,
        "message_id": app_data.get("storage_message_id")
    })

    if not fwd_res.get("ok"):
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": "File access karne me samasya aayi."})
        return

    pwd = app_data.get("password")
    if pwd:
        send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"🔑 Password: `{pwd}`", "parse_mode": "Markdown"})

def show_admin_panel(chat_id):
    keyboard = [
        [{"text": "📱 Add App", "callback_data": "adm_add_app"}, {"text": "📋 List Apps", "callback_data": "adm_list_apps"}],
        [{"text": "📢 Force Join Settings", "callback_data": "adm_fj_menu"}],
        [{"text": "👥 Total Users", "callback_data": "adm_stats"}, {"text": "📣 Broadcast", "callback_data": "adm_broadcast"}],
        [{"text": "⚙️ Bot Messages/Settings", "callback_data": "adm_settings"}]
    ]
    send_tg_request("sendMessage", {
        "chat_id": chat_id,
        "text": "🛠 **Admin Panel**",
        "parse_mode": "Markdown",
        "reply_markup": {"inline_keyboard": keyboard}
    })

@app.route("/", methods=["GET"])
def home():
    return "Bot is running fine!"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data:
        return "OK", 200

    # Handle Callback Queries (Buttons)
    if "callback_query" in data:
        cq = data["callback_query"]
        cq_id = cq["id"]
        user_id = cq["from"]["id"]
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
                send_or_update_force_join(chat_id, user_id, deep_link, message_id=message_id)

        # Admin panel actions
        elif user_id in ADMIN_IDS:
            if cb_data == "adm_main":
                show_admin_panel(chat_id)
            elif cb_data == "adm_add_app":
                ADMIN_SESSIONS[user_id] = {"step": "APP_TITLE"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "App ka Title/Naam bhejo:"})
            elif cb_data == "adm_list_apps":
                apps = supabase.table("apps").select("*").limit(20).execute().data or []
                text = "📋 **Recent Apps:**\n\n"
                for a in apps:
                    text += f"• **{a['title']}** (Code: `{a['deep_link_code']}`)\nLink: https://t.me/LexxiAdminBot?start={a['deep_link_code']}\n\n"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": text or "Koi apps nahi mile."})
            elif cb_data == "adm_fj_menu":
                channels = supabase.table("force_join_channels").select("*").order("slot").execute().data or []
                btns = []
                for c in channels:
                    status = "✅" if c.get("active") else "❌"
                    btns.append([{"text": f"Slot {c['slot']} ({status}) {c.get('button_text', '')}", "callback_data": f"adm_slot_{c['slot']}"}])
                btns.append([{"text": "⬅️ Back", "callback_data": "adm_main"}])
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Configure 4 Channels:", "reply_markup": {"inline_keyboard": btns}})
            elif cb_data.startswith("adm_slot_"):
                slot = int(cb_data.split("_")[2])
                ADMIN_SESSIONS[user_id] = {"step": "FJ_CONFIG", "slot": slot}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"Slot {slot} ke liye is format me bhejo:\n`Chat_ID | Button_Name | Invite_Link`\n\nBand karne ke liye `disable` bhejein."})
            elif cb_data == "adm_stats":
                count = len(supabase.table("users").select("telegram_user_id").execute().data or [])
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"👥 **Total Registered Users:** {count}"})
            elif cb_data == "adm_broadcast":
                ADMIN_SESSIONS[user_id] = {"step": "BROADCAST"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Wo message bhejo jo sabhi users ko bhejna hai:"})
            elif cb_data == "adm_settings":
                fj_text = get_setting("force_join_text", "Not Set")
                btn_text = get_setting("verify_button_text", "✅ VERIFY")
                poster = get_setting("force_join_poster", "Not Set")
                text = (
                    "⚙️ **Bot Messages & Settings**\n\n"
                    f"**Force Join Caption:**\n`{fj_text}`\n\n"
                    f"**Verify Button:** `{btn_text}`\n"
                    f"**Poster URL:** `{poster}`\n\n"
                    "Neeche diye gaye buttons se edit karein:"
                )
                keyboard = [
                    [{"text": "📝 Edit Caption", "callback_data": "set_msg_fj_text"}],
                    [{"text": "🔘 Edit Verify Button", "callback_data": "set_msg_verify_btn"}],
                    [{"text": "🖼️ Edit Poster Link", "callback_data": "set_msg_poster"}],
                    [{"text": "⬅️ Back", "callback_data": "adm_main"}]
                ]
                send_tg_request("sendMessage", {
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "reply_markup": {"inline_keyboard": keyboard}
                })
            elif cb_data == "set_msg_fj_text":
                ADMIN_SESSIONS[user_id] = {"step": "SET_FJ_TEXT"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Force Join message/caption bhejo:"})
            elif cb_data == "set_msg_verify_btn":
                ADMIN_SESSIONS[user_id] = {"step": "SET_VERIFY_BTN"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Naya Verify Button text bhejo:"})
            elif cb_data == "set_msg_poster":
                ADMIN_SESSIONS[user_id] = {"step": "SET_POSTER"}
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Poster image ka direct URL bhejo (ya empty karne ke liye `none` bhejo):"})

        return "OK", 200

    # Handle Normal Messages
    if "message" in data:
        msg = data["message"]
        chat_id = msg["chat"]["id"]
        user_id = msg["from"]["id"]
        username = msg["from"].get("username", "")
        text = msg.get("text", "")

        # Save user to DB
        try:
            supabase.table("users").upsert({"telegram_user_id": user_id, "username": username}).execute()
        except Exception:
            pass

        # /admin Command (Reset Session + Open Panel)
        if text and text.lower().strip() == "/admin" and user_id in ADMIN_IDS:
            ADMIN_SESSIONS.pop(user_id, None)
            show_admin_panel(chat_id)
            return "OK", 200

        # Admin step-by-step inputs
        if user_id in ADMIN_IDS and user_id in ADMIN_SESSIONS:
            sess = ADMIN_SESSIONS[user_id]
            step = sess.get("step")

            if step == "APP_TITLE":
                sess["title"] = text
                sess["step"] = "APP_CODE"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Deep-link code bhejo (jaise `99` ya `file12`):"})
                return "OK", 200

            elif step == "APP_CODE":
                sess["code"] = text.strip()
                sess["step"] = "APP_FILE"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Storage Channel se wo file yahan **FORWARD** karein:"})
                return "OK", 200

            elif step == "APP_FILE":
                fwd_mid = msg.get("forward_from_message_id")
                if not fwd_mid:
                    send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Kripya Storage channel se seedha forward karein."})
                    return "OK", 200
                sess["storage_id"] = fwd_mid
                sess["step"] = "APP_PASS"
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Password bhejein (agar koi password nahi hai to `none` likhein):"})
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
                bot_user = "LexxiAdminBot"
                send_tg_request("sendMessage", {
                    "chat_id": chat_id,
                    "text": f"✅ **App Successfully Saved!**\n\nLink: https://t.me/{bot_user}?start={sess['code']}",
                    "parse_mode": "Markdown"
                })
                return "OK", 200

            elif step == "FJ_CONFIG":
                slot = sess["slot"]
                if text.strip().lower() == "disable":
                    supabase.table("force_join_channels").update({"active": False}).eq("slot", slot).execute()
                    send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"Slot {slot} disabled."})
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
                        send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"✅ Slot {slot} updated!"})
                    else:
                        send_tg_request("sendMessage", {"chat_id": chat_id, "text": "Format galat tha. Kripya Chat_ID | Name | Link bhejein."})
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
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": f"📢 Broadcast complete. Sent to {sent} users."})
                return "OK", 200

            elif step == "SET_FJ_TEXT":
                set_setting("force_join_text", text)
                ADMIN_SESSIONS.pop(user_id, None)
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": "✅ Caption update ho gaya!"})
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

        # /start command with deep-linking
        if text.startswith("/start"):
            args = text.split()
            if len(args) > 1:
                deep_link = args[1].strip()
                all_joined = send_or_update_force_join(chat_id, user_id, deep_link)
                if all_joined:
                    deliver_file(chat_id, deep_link)
            else:
                welcome_msg = get_setting("welcome_text", "Welcome! Open a valid file link to download.")
                send_tg_request("sendMessage", {"chat_id": chat_id, "text": welcome_msg})
            return "OK", 200

    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
                

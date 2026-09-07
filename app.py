import os, re, time, threading, requests
from flask import Flask, request
from supabase import create_client

app = Flask(__name__)

# Config
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = [int(i.strip()) for i in os.environ.get("ADMIN_IDS", "").split(",") if i.strip()]
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try: supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except: pass

ADMIN_SESSIONS = {}

# Exact Styled Defaults
DEFAULT_WELCOME = ">> LOOKING FOR HACKS, ID'S OR ANYTHING ELSE?\nMESSAGE [@lexxiowner](https://t.me/lexxiowner) — WE MIGHT HAVE IT. ⚡"
DEFAULT_ERROR = ">> FACING ANY ISSUE?\nCONTACT [@lexxiowner](https://t.me/lexxiowner) FOR HELP. ⚡"
DEFAULT_FJ = ">> HEY {name} ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T JOINED TO\nOUR CHANNELS YET,"
DEFAULT_STORAGE = "-1004430375220"
DEFAULT_POSTER = ""

DEFAULT_CHANNELS = [
    {"slot": 1, "chat_id": -1001875503950, "button_text": "ʟᴇxɪ ᴍᴏᴅs", "join_url": "https://t.me/+JjB0fpWk8dAwZTBl", "active": True},
    {"slot": 2, "chat_id": -1002991207510, "button_text": "ɪ'ᴅ sᴛᴏʀᴇ", "join_url": "https://t.me/+axSZFBZ5ztVkMTZl", "active": True},
    {"slot": 3, "chat_id": -1003992674272, "button_text": "ғʀᴇᴇ ʜᴀᴄᴋ", "join_url": "https://t.me/BGMIHackLexi", "active": True},
    {"slot": 4, "chat_id": -1004335377904, "button_text": "ʙᴀᴄᴋᴜᴘ", "join_url": "https://t.me/+G7-o7RSFmw8zOTI1", "active": True}
]

def clean_name(name):
    return re.sub(r'([_*\[\]()~`>#+=|{}.!-])', r'\\\1', str(name or "User"))

def tg_call(method, data):
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=10).json()
        if not r.get("ok") and "parse_mode" in data:
            data.pop("parse_mode", None)
            return requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=data, timeout=10).json()
        return r
    except:
        return {}

def get_setting(key, default=""):
    if not supabase: return default
    try:
        res = supabase.table("settings").select("value").eq("key", key).execute()
        if res.data: return res.data[0].get("value", default)
    except: pass
    return default

def set_setting(key, value):
    if not supabase: return
    try: supabase.table("settings").upsert({"key": key, "value": str(value)}).execute()
    except: pass

def get_channels():
    channels = DEFAULT_CHANNELS
    if supabase:
        try:
            r = supabase.table("force_join_channels").select("*").order("slot").execute()
            if r.data: channels = r.data
        except: pass
    return channels

def get_unjoined(user_id):
    channels = get_channels()
    unjoined = []
    for c in channels:
        if not c.get("active") or not c.get("chat_id"): continue
        try:
            res = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember", json={"chat_id": c["chat_id"], "user_id": user_id}, timeout=4).json()
            if not res.get("ok") or res.get("result", {}).get("status") in ["left", "kicked"]:
                unjoined.append(c)
        except:
            unjoined.append(c)
    return unjoined

def send_fj(chat_id, user_id, code, name, old_mid=None):
    unjoined = get_unjoined(user_id)
    if not unjoined: return True
    if old_mid:
        tg_call("deleteMessage", {"chat_id": chat_id, "message_id": old_mid})
    raw = get_setting("force_join_text", DEFAULT_FJ)
    caption = raw.replace("{name}", clean_name(name)) if "{name}" in raw else raw
    poster = get_setting("force_join_poster", DEFAULT_POSTER)
    kb, row = [], []
    for c in unjoined:
        btn_txt = c.get('button_text', '').replace("↗", "").replace("↗️", "").strip()
        row.append({"text": btn_txt, "url": c.get("join_url")})
        if len(row) == 2:
            kb.append(row)
            row = []
    if row: kb.append(row)
    kb.append([{"text": "» VERIFY", "callback_data": f"v:{code}"}])
    markup = {"inline_keyboard": kb}

    has_photo = bool(poster and poster.lower() != "none" and not poster.startswith("https://t.me/c/"))
    if has_photo:
        tg_call("sendPhoto", {"chat_id": chat_id, "photo": poster, "caption": caption, "parse_mode": "Markdown", "reply_markup": markup})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": caption, "parse_mode": "Markdown", "reply_markup": markup})
    return False

def del_task(chat_id, mids):
    time.sleep(300)
    for m in mids:
        if m: tg_call("deleteMessage", {"chat_id": chat_id, "message_id": m})

def deliver_file(chat_id, code):
    app_data = None
    if supabase:
        try:
            r = supabase.table("apps").select("*").eq("deep_link_code", code).eq("active", True).execute()
            if r.data: app_data = r.data[0]
        except: pass
    if not app_data:
        tg_call("sendMessage", {"chat_id": chat_id, "text": get_setting("error_feedback_text", DEFAULT_ERROR), "parse_mode": "Markdown"})
        return
    storage = get_setting("storage_channel_id", DEFAULT_STORAGE)
    fwd = tg_call("copyMessage", {"chat_id": chat_id, "from_chat_id": storage, "message_id": app_data["storage_message_id"]})
    if not fwd.get("ok"):
        tg_call("sendMessage", {"chat_id": chat_id, "text": get_setting("error_feedback_text", DEFAULT_ERROR), "parse_mode": "Markdown"})
        return
    mids = [fwd.get("result", {}).get("message_id")]
    if app_data.get("password"):
        p = tg_call("sendMessage", {"chat_id": chat_id, "text": f"🔑 **ᴘᴀssᴡᴏʀᴅ:** `{app_data['password']}`", "parse_mode": "Markdown"})
        if p.get("ok"): mids.append(p["result"]["message_id"])
    warn = "⚠️ **ɪᴍᴘᴏʀᴛᴀɴᴛ ɴᴏᴛɪᴄᴇ:**\n\n_ᴀʟʟ ᴍᴇssᴀɢᴇs ᴡɪʟʟ ʙᴇ ᴅᴇʟᴇᴛᴇᴅ ᴀғᴛᴇʀ 𝟻 ᴍɪɴᴜᴛᴇs.\nᴘʟᴇᴀsᴇ ғᴏʀᴡᴀʀᴅ/sᴀᴠᴇ ᴛʜɪs ғɪʟᴇ ᴛᴏ ʏᴏᴜʀ sᴀᴠᴇᴅ ᴍᴇssᴀɢᴇs!_"
    w = tg_call("sendMessage", {"chat_id": chat_id, "text": warn, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": [[{"text": "📟 ᴜᴘᴅᴀᴛᴇ ᴄʜᴀɴɴᴇʟ", "url": "https://t.me/lexxiowner"}]]}})
    if w.get("ok"): mids.append(w["result"]["message_id"])
    threading.Thread(target=del_task, args=(chat_id, mids), daemon=True).start()

def show_admin(chat_id, message_id=None):
    kb = [
        [{"text": "➕ Add File", "callback_data": "adm_add"}, {"text": "🗑️ Delete File", "callback_data": "adm_del"}],
        [{"text": "📁 List Files", "callback_data": "adm_list"}, {"text": "📢 Slots On/Off", "callback_data": "adm_slots"}],
        [{"text": "👥 Total Users", "callback_data": "adm_users"}, {"text": "📣 Broadcast", "callback_data": "adm_bc"}],
        [{"text": "⚙️ Config / Settings", "callback_data": "adm_sett"}]
    ]
    txt = "🎛 **ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟ ᴘᴀɴᴇʟ**"
    if message_id:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
    else:
        tg_call("sendMessage", {"chat_id": chat_id, "text": txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

@app.route("/", methods=["GET"])
def home():
    return "Bot Server Online 🚀"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data: return "OK", 200

    if "callback_query" in data:
        cq = data["callback_query"]
        uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")
        uname = cq["from"].get("first_name", "User")

        if cdata.startswith("v:"):
            code = cdata.split(":", 1)[1]
            if not get_unjoined(uid):
                tg_call("answerCallbackQuery", {"callback_query_id": cq["id"], "text": "✅ Verified Successfully!"})
                tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
                deliver_file(cid, code)
            else:
                tg_call("answerCallbackQuery", {"callback_query_id": cq["id"], "text": "❌ Pehle sabhi channels join karein!", "show_alert": True})
                send_fj(cid, uid, code, uname, old_mid=mid)
            return "OK", 200

        tg_call("answerCallbackQuery", {"callback_query_id": cq["id"]})

        if uid in ADMIN_IDS:
            if cdata == "adm_back":
                show_admin(cid, mid)
            elif cdata == "adm_add":
                ADMIN_SESSIONS[uid] = {"step": "TITLE"}
                tg_call("sendMessage", {"chat_id": cid, "text": "File ka **Title** bhejo:"})
            elif cdata == "adm_del":
                ADMIN_SESSIONS[uid] = {"step": "DEL"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Delete karne ke liye file ka **Code** bhejo:"})
            elif cdata == "adm_users":
                cnt = len(supabase.table("users").select("telegram_user_id").execute().data or []) if supabase else 0
                tg_call("sendMessage", {"chat_id": cid, "text": f"👥 **Total Users:** `{cnt}`"})
            elif cdata == "adm_bc":
                ADMIN_SESSIONS[uid] = {"step": "BC"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Broadcast message bhejo:"})
            elif cdata == "adm_list":
                apps = []
                if supabase:
                    try: apps = supabase.table("apps").select("*").order("created_at", desc=True).limit(8).execute().data or []
                    except: pass
                if not apps:
                    tg_call("sendMessage", {"chat_id": cid, "text": "📁 Database me koi file nahi mili."})
                else:
                    for a in apps:
                        code = a.get('deep_link_code')
                        title = a.get('title')
                        link = f"https://t.me/LexxiAdminBot?start={code}"
                        kb = [[{"text": f"🗑️ Delete `{code}`", "callback_data": f"qdel_{code}"}]]
                        tg_call("sendMessage", {"chat_id": cid, "text": f"📦 **{title}**\n• Code: `{code}`\n• Link: {link}", "reply_markup": {"inline_keyboard": kb}})
            elif cdata.startswith("qdel_"):
                del_code = cdata.split("_")[1]
                if supabase:
                    try: supabase.table("apps").delete().eq("deep_link_code", del_code).execute()
                    except: pass
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": f"🗑️ File `{del_code}` deleted!"})
            elif cdata == "adm_slots":
                ch_list = get_channels()
                btns = [[{"text": f"Slot {c['slot']} ({'🟢 ON' if c.get('active') else '🔴 OFF'})", "callback_data": f"tog_{c['slot']}"}] for c in ch_list]
                btns.append([{"text": "⬅️ Back", "callback_data": "adm_back"}])
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": "Slots Toggle (Tap to switch):", "reply_markup": {"inline_keyboard": btns}})
            elif cdata.startswith("tog_"):
                s = int(cdata.split("_")[1])
                if supabase:
                    cur = supabase.table("force_join_channels").select("active").eq("slot", s).execute().data
                    val = not cur[0]["active"] if cur else False
                    supabase.table("force_join_channels").update({"active": val}).eq("slot", s).execute()
                cq["data"] = "adm_slots"
                return webhook()
            elif cdata == "adm_sett":
                w_txt = get_setting("welcome_text", DEFAULT_WELCOME)
                e_txt = get_setting("error_feedback_text", DEFAULT_ERROR)
                fj_txt = get_setting("force_join_text", DEFAULT_FJ)
                st_id = get_setting("storage_channel_id", DEFAULT_STORAGE)
                p_url = get_setting("force_join_poster", DEFAULT_POSTER or "None")
                panel_txt = (
                    "⚙️ **ʙᴏᴛ ᴄᴏɴғɪɢᴜʀᴀᴛɪᴏɴs**\n\n"
                    f"👋 **Welcome Msg:**\n{w_txt}\n\n"
                    f"⚠️ **Error Msg:**\n{e_txt}\n\n"
                    f"📦 **Storage Channel ID:** `{st_id}`\n\n"
                    f"🖼️ **Poster Link:** `{p_url}`\n\n"
                    f"📢 **Force Join Caption:**\n{fj_txt}"
                )
                kb = [
                    [{"text": "👋 Edit Welcome", "callback_data": "s_wel"}, {"text": "⚠️ Edit Error", "callback_data": "s_err"}],
                    [{"text": "📝 Edit FJ Caption", "callback_data": "s_fj"}, {"text": "📦 Set Storage ID", "callback_data": "s_st"}],
                    [{"text": "🖼️ Edit Poster Link", "callback_data": "s_pos"}],
                    [{"text": "⬅️ Back", "callback_data": "adm_back"}]
                ]
                tg_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": panel_txt, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})
            elif cdata == "s_wel":
                ADMIN_SESSIONS[uid] = {"step": "SET_W"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Naya Welcome message bhejo:"})
            elif cdata == "s_err":
                ADMIN_SESSIONS[uid] = {"step": "SET_E"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Naya Error message bhejo:"})
            elif cdata == "s_fj":
                ADMIN_SESSIONS[uid] = {"step": "SET_FJ"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Naya FJ Caption bhejo (`{name}` name ke liye use karein):"})
            elif cdata == "s_st":
                ADMIN_SESSIONS[uid] = {"step": "SET_ST"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Storage Channel ID bhejo:"})
            elif cdata == "s_pos":
                ADMIN_SESSIONS[uid] = {"step": "SET_POS"}
                tg_call("sendMessage", {"chat_id": cid, "text": "Poster image direct URL bhejo (ya `none` bhejo):"})
        return "OK", 200

    if "message" in data:
        msg = data["message"]
        cid, uid, txt = msg["chat"]["id"], msg["from"]["id"], msg.get("text", "")
        uname = msg["from"].get("first_name", "User")

        if supabase and uid:
            try: supabase.table("users").upsert({"telegram_user_id": uid, "username": msg["from"].get("username", "")}).execute()
            except: pass

        if txt == "/admin" and uid in ADMIN_IDS:
            ADMIN_SESSIONS.pop(uid, None)
            show_admin(cid)
            return "OK", 200

        if uid in ADMIN_IDS and uid in ADMIN_SESSIONS:
            sess = ADMIN_SESSIONS[uid]
            st = sess.get("step")

            if st == "TITLE":
                sess["t"], sess["step"] = txt, "CODE"
                tg_call("sendMessage", {"chat_id": cid, "text": "Short Code bhejo (jaise `1`, `ep01`):"})
            elif st == "CODE":
                sess["c"], sess["step"] = txt.strip(), "FILE"
                tg_call("sendMessage", {"chat_id": cid, "text": "Storage Channel se file **FORWARD** karo:"})
            elif st == "FILE":
                fwd_id = msg.get("forward_from_message_id")
                fwd_chat = msg.get("forward_from_chat", {})
                if not fwd_id:
                    tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Seedha Storage channel se forward karo."})
                    return "OK", 200
                sess["mid"], sess["step"] = fwd_id, "PASS"
                if fwd_chat and fwd_chat.get("id"):
                    set_setting("storage_channel_id", str(fwd_chat["id"]))
                tg_call("sendMessage", {"chat_id": cid, "text": "Password bhejo (ya `none` likho):"})
            elif st == "PASS":
                pwd = "" if txt.strip().lower() == "none" else txt.strip()
                if supabase:
                    supabase.table("apps").upsert({"title": sess["t"], "deep_link_code": sess["c"], "storage_message_id": sess["mid"], "password": pwd, "active": True}, on_conflict="deep_link_code").execute()
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": f"✅ Saved! Link: `https://t.me/LexxiAdminBot?start={sess['c']}`", "parse_mode": "Markdown"})
            elif st == "DEL":
                if supabase: supabase.table("apps").delete().eq("deep_link_code", txt.strip()).execute()
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": f"✅ Code `{txt.strip()}` deleted!"})
                show_admin(cid)
            elif st == "SET_W":
                set_setting("welcome_text", txt)
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "✅ Welcome message update ho gaya!"})
                show_admin(cid)
            elif st == "SET_E":
                set_setting("error_feedback_text", txt)
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "✅ Error message update ho gaya!"})
                show_admin(cid)
            elif st == "SET_FJ":
                set_setting("force_join_text", txt)
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "✅ Force join caption update ho gaya!"})
                show_admin(cid)
            elif st == "SET_ST":
                set_setting("storage_channel_id", txt.strip())
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": f"✅ Storage ID set: `{txt.strip()}`"})
                show_admin(cid)
            elif st == "SET_POS":
                p_val = "" if txt.strip().lower() == "none" else txt.strip()
                set_setting("force_join_poster", p_val)
                ADMIN_SESSIONS.pop(uid, None)
                tg_call("sendMessage", {"chat_id": cid, "text": "✅ Poster URL update ho gaya!"})
                show_admin(cid)
            elif st == "BC":
                ADMIN_SESSIONS.pop(uid, None)
                users = supabase.table("users").select("telegram_user_id").execute().data or [] if supabase else []
                for u in users: tg_call("sendMessage", {"chat_id": u["telegram_user_id"], "text": txt})
                tg_call("sendMessage", {"chat_id": cid, "text": "📢 Broadcast complete!"})
            return "OK", 200

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
    

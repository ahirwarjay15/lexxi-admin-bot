import os, re, time, threading, requests
from flask import Flask, request
from supabase import create_client

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS = [int(i.strip()) for i in os.environ.get("ADMIN_IDS", "").split(",") if i.strip()]
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

supabase = None
if SUPABASE_URL and SUPABASE_KEY:
    try: supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except: pass

ADMIN_SESSIONS = {}

DEFAULT_WELCOME = ">> LOOKING FOR HACKS, ID'S OR ANYTHING ELSE?\nMESSAGE [@lexxiowner](https://t.me/lexxiowner) — WE MIGHT HAVE IT. ⚡"
DEFAULT_ERROR = ">> FACING ANY ISSUE?\nCONTACT [@lexxiowner](https://t.me/lexxiowner) FOR HELP. ⚡"
DEFAULT_FJ = ">> HEY {name} ×\nYOUR FILE IS READY\nLOOKS LIKE YOU HAVEN'T JOINED TO\nOUR CHANNELS YET,"
DEFAULT_STORAGE = "-1004430375220"

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

def get_unjoined(user_id):
    channels = DEFAULT_CHANNELS
    if supabase:
        try:
            r = supabase.table("force_join_channels").select("*").order("slot").execute()
            if r.data: channels = r.data
        except: pass
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

def send_fj(chat_id, user_id, code, name, mid=None):
    unjoined = get_unjoined(user_id)
    if not unjoined: return True
    raw = get_setting("force_join_text", DEFAULT_FJ)
    caption = raw.replace("{name}", clean_name(name)) if "{name}" in raw else raw
    kb, row = [], []
    for c in unjoined:
        row.append({"text": f"{c.get('button_text')} ↗", "url": c.get("join_url")})
        if len(row) == 2:
            kb.append(row)
            row = []
    if row: kb.append(row)
    kb.append([{"text": "» VERIFY", "callback_data": f"v:{code}"}])
    markup = {"inline_keyboard": kb}
    if mid:
        tg_call("editMessageText", {"chat_id": chat_id, "message_id": mid, "text": caption, "parse_mode": "Markdown", "reply_markup": markup})
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

def show_admin(chat_id):
    kb = [
        [{"text": "➕ Add File", "callback_data": "adm_add"}, {"text": "🗑️ Delete File", "callback_data": "adm_del"}],
        [{"text": "📢 Slots On/Off", "callback_data": "adm_slots"}, {"text": "👥 Total Users", "callback_data": "adm_users"}],
        [{"text": "📣 Broadcast", "callback_data": "adm_bc"}]
    ]
    tg_call("sendMessage", {"chat_id": chat_id, "text": "🎛 **ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ**", "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": kb}})

@app.route("/", methods=["GET"])
def home():
    return "Bot Online 🚀"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if not data: return "OK", 200
    if "callback_query" in data:
        cq = data["callback_query"]
        uid, cid, mid, cdata = cq["from"]["id"], cq["message"]["chat"]["id"], cq["message"]["message_id"], cq.get("data", "")
        uname = cq["from"].get("first_name", "User")
        tg_call("answerCallbackQuery", {"callback_query_id": cq["id"]})
        if cdata.startswith("v:"):
            code = cdata.split(":", 1)[1]
            if not get_unjoined(uid):
                tg_call("deleteMessage", {"chat_id": cid, "message_id": mid})
                deliver_file(cid, code)
            else:
                send_fj(cid, uid, code, uname, mid=mid)
        elif uid in ADMIN_IDS:
            if cdata == "adm_add":
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
            elif cdata == "adm_slots":
                ch_list = DEFAULT_CHANNELS
                if supabase:
                    r = supabase.table("force_join_channels").select("*").order("slot").execute()
                    if r.data: ch_list = r.data
                btns = [[{"text": f"Slot {c['slot']} ({'🟢 ON' if c.get('active') else '🔴 OFF'})", "callback_data": f"tog_{c['slot']}"}] for c in ch_list]
                btns.append([{"text": "⬅️ Back", "callback_data": "adm_back"}])
                tg_call("sendMessage", {"chat_id": cid, "text": "Slots Toggle Karein:", "reply_markup": {"inline_keyboard": btns}})
            elif cdata.startswith("tog_"):
                s = int(cdata.split("_")[1])
                if supabase:
                    cur = supabase.table("force_join_channels").select("active").eq("slot", s).execute().data
                    val = not cur[0]["active"] if cur else False
                    supabase.table("force_join_channels").update({"active": val}).eq("slot", s).execute()
                    tg_call("sendMessage", {"chat_id": cid, "text": f"Slot {s} status badal gaya!"})
            elif cdata == "adm_back":
                show_admin(cid)
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
                tg_call("sendMessage", {"chat_id": cid, "text": "File ka short **Code** bhejo (eg `1`, `ep01`):"})
            elif st == "CODE":
                sess["c"], sess["step"] = txt.strip(), "FILE"
                tg_call("sendMessage", {"chat_id": cid, "text": "Storage Channel se file **FORWARD** karo:"})
            elif st == "FILE":
                fwd_id = msg.get("forward_from_message_id")
                if not fwd_id:
                    tg_call("sendMessage", {"chat_id": cid, "text": "⚠️ Seedha Storage channel se forward karo."})
                    return "OK", 200
                sess["mid"], sess["step"] = fwd_id, "PASS"
                fwd_chat = msg.get("forward_from_chat", {})
                if fwd_chat and fwd_chat.get("id") and supabase:
                    supabase.table("settings").upsert({"key": "storage_channel_id", "value": str(fwd_chat["id"])}).execute()
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
            elif st == "BC":
                ADMIN_SESSIONS.pop(uid, None)
                users = supabase.table("users").select("telegram_user_id").execute().data or [] if supabase else []
                for u in users: tg_call("sendMessage", {"chat_id": u["telegram_user_id"], "text": txt})
                tg_call("sendMessage", {"chat_id": cid, "text": "📢 Broadcast Finished!"})
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
    

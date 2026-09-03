import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
WELCOME_TEXT = os.getenv(
    "WELCOME_TEXT",
    "👋 Welcome!\n\nThanks for joining our channel. ❤️"
)

API = f"https://api.telegram.org/bot{BOT_TOKEN}"


def tg(method, **params):
    r = requests.post(f"{API}/{method}", data=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(data)
    return data["result"]


@app.get("/")
def home():
    return "Bot is running."


@app.post("/webhook")
def webhook():
    update = request.get_json(silent=True) or {}
    req = update.get("chat_join_request")

    if not req:
        return jsonify(ok=True)

    chat = req["chat"]
    user = req["from"]

    if CHANNEL_ID and str(chat["id"]) != CHANNEL_ID:
        return jsonify(ok=True)

    user_chat_id = req["user_chat_id"]

    # Send the private message first. Telegram makes user_chat_id from a
    # join request usable for a limited period.
    try:
        tg(
            "sendMessage",
            chat_id=user_chat_id,
            text=WELCOME_TEXT,
            disable_web_page_preview=True,
        )
    except Exception as e:
        print("DM failed:", e)

    # Then approve the join request.
    try:
        tg(
            "approveChatJoinRequest",
            chat_id=chat["id"],
            user_id=user["id"],
        )
    except Exception as e:
        print("Approve failed:", e)

    return jsonify(ok=True)


def setup_webhook():
    base_url = os.getenv("RENDER_EXTERNAL_URL")
    if not base_url:
        print("RENDER_EXTERNAL_URL not available; webhook was not set.")
        return

    webhook_url = base_url.rstrip("/") + "/webhook"
    try:
        result = tg(
            "setWebhook",
            url=webhook_url,
            allowed_updates='["chat_join_request"]',
        )
        print("Webhook set:", result)
    except Exception as e:
        print("Webhook setup failed:", e)


if __name__ == "__main__":
    setup_webhook()
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

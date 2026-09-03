import os
import time
import requests

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # e.g. -1001234567890
WELCOME_TEXT = os.getenv(
    "WELCOME_TEXT",
    "👋 Welcome!\n\nThanks for joining our channel. ❤️"
)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is missing.")

API = f"https://api.telegram.org/bot{BOT_TOKEN}"


def tg(method, **params):
    r = requests.post(f"{API}/{method}", data=params, timeout=40)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(data)
    return data["result"]


def handle_join_request(update):
    req = update.get("chat_join_request")
    if not req:
        return

    chat = req["chat"]
    user = req["from"]

    # If CHANNEL_ID is set, only process that channel.
    if CHANNEL_ID and str(chat["id"]) != str(CHANNEL_ID):
        return

    # Telegram allows the bot to use user_chat_id from a join request
    # for a limited time. Send the DM first, then approve the request.
    user_chat_id = req["user_chat_id"]

    try:
        tg(
            "sendMessage",
            chat_id=user_chat_id,
            text=WELCOME_TEXT,
            disable_web_page_preview=True,
        )
    except Exception as e:
        print(f"DM failed for @{user.get('username', 'no_username')} "
              f"({user['id']}): {e}")

    try:
        tg(
            "approveChatJoinRequest",
            chat_id=chat["id"],
            user_id=user["id"],
        )
        print(f"Approved: {user.get('username', user['id'])}")
    except Exception as e:
        print(f"Approve failed for {user['id']}: {e}")


def main():
    # Remove any existing webhook so long polling works.
    try:
        tg("deleteWebhook", drop_pending_updates=False)
    except Exception as e:
        print("Webhook cleanup:", e)

    offset = 0
    print("Bot is running...")

    while True:
        try:
            updates = tg(
                "getUpdates",
                offset=offset,
                timeout=30,
                allowed_updates='["chat_join_request"]',
            )

            for update in updates:
                offset = update["update_id"] + 1
                try:
                    handle_join_request(update)
                except Exception as e:
                    print("Update error:", e)

        except Exception as e:
            print("Polling error:", e)
            time.sleep(3)


if __name__ == "__main__":
    main()

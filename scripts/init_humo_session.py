#!/usr/bin/env python3
import os
from pathlib import Path

from telethon.sessions import StringSession
from telethon.sync import TelegramClient


def load_dotenv_file(path):
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and not os.getenv(key):
            os.environ[key] = value


def main():
    project_root = Path(__file__).resolve().parent.parent
    load_dotenv_file(project_root / ".env")

    api_id_raw = os.getenv("TELEGRAM_APP_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_APP_API_HASH", "").strip()
    session_file = os.getenv("TELEGRAM_SESSION_FILE", "data/humo_telegram.session").strip()
    humo_username = os.getenv("HUMO_BOT_USERNAME", "@HUMOcardbot").strip() or "@HUMOcardbot"

    if not api_id_raw or not api_hash:
        raise RuntimeError("Set TELEGRAM_APP_API_ID and TELEGRAM_APP_API_HASH in .env first")

    try:
        api_id = int(api_id_raw)
    except ValueError as error:
        raise RuntimeError("TELEGRAM_APP_API_ID must be an integer") from error

    session_path = project_root / session_file
    session_path.parent.mkdir(parents=True, exist_ok=True)

    print("Authorizing Telegram user session...")
    with TelegramClient(str(session_path), api_id, api_hash) as client:
        client.start()
        me = client.get_me()
        entity = client.get_entity(humo_username)
        username = str(getattr(entity, "username", "") or "").strip()
        print(f"Authorized as: id={getattr(me, 'id', 0)} username=@{getattr(me, 'username', '')}")
        print(f"HUMO entity resolved as: @{username}")
        print("")
        session_string = StringSession.save(client.session)
        print("Save this into .env for server mode:")
        print(f"TELEGRAM_SESSION_STRING={session_string}")


if __name__ == "__main__":
    main()

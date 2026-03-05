#!/usr/bin/env python3
import argparse
import html
import json
import os
import ssl
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    import certifi  # type: ignore
except Exception:  # pragma: no cover
    certifi = None


def _safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return int(default)


def _load_reports_store(path):
    with open(path, "r", encoding="utf-8") as source:
        data = json.load(source)
    if not isinstance(data, dict):
        raise ValueError("purchase_reports.json root must be an object")
    return data


def _collect_private_recipients(store):
    user_activity = store.get("user_activity")
    if not isinstance(user_activity, dict):
        return []

    recipients = []
    seen = set()
    for raw_user_id in user_activity.keys():
        user_id = _safe_int(raw_user_id, 0)
        if user_id <= 0 or user_id in seen:
            continue
        seen.add(user_id)
        recipients.append(user_id)
    recipients.sort()
    return recipients


def _collect_group_recipients(store):
    recipients = set()

    known_groups = store.get("known_group_chats")
    if isinstance(known_groups, dict):
        for raw_chat_id in known_groups.keys():
            chat_id = _safe_int(raw_chat_id, 0)
            if chat_id < 0:
                recipients.add(chat_id)

    reports_chat_id = _safe_int(store.get("reports_chat_id"), 0)
    if reports_chat_id < 0:
        recipients.add(reports_chat_id)

    return sorted(recipients)


def _build_notice_text(minutes, reason):
    safe_minutes = max(int(minutes), 1)
    reason_text = str(reason or "").strip()
    if reason_text:
        safe_reason = html.escape(reason_text)
        reason_line = (
            f"🛠 Причина: <b>{safe_reason}</b>\n"
            f"🛠 Sabab: <b>{safe_reason}</b>\n\n"
        )
    else:
        reason_line = ""

    return (
        "⚠️ <b>Внимание: бот и Mini App перезапускаются.</b>\n"
        "⚠️ <b>Diqqat: bot va Mini App qayta ishga tushirilmoqda.</b>\n\n"
        f"{reason_line}"
        f"⏱ В ближайшие <b>{safe_minutes} мин</b> часть функций может временно не работать.\n"
        f"⏱ Keyingi <b>{safe_minutes} daqiqa</b> ayrim funksiyalar vaqtincha ishlamasligi mumkin.\n\n"
        "✅ После перезапуска всё восстановится автоматически.\n"
        "✅ Qayta ishga tushgandan so'ng hammasi avtomatik tiklanadi."
    )


def _build_reply_markup(web_app_url):
    url = str(web_app_url or "").strip()
    if not url.lower().startswith(("http://", "https://")):
        return None

    return {
        "inline_keyboard": [
            [
                {
                    "text": "📱 Open Mini App",
                    "web_app": {"url": url},
                }
            ]
        ]
    }


def _telegram_api_json(bot_token, method, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        f"https://api.telegram.org/bot{bot_token}/{method}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    ssl_context = None
    if certifi is not None:
        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
        except Exception:
            ssl_context = None

    if ssl_context is not None:
        with urlopen(request, timeout=20, context=ssl_context) as response:
            return json.loads(response.read().decode("utf-8"))
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _send_notice(bot_token, chat_id, text, reply_markup=None):
    payload = {
        "chat_id": int(chat_id),
        "text": text,
        "parse_mode": "HTML",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return _telegram_api_json(bot_token, "sendMessage", payload)


def main():
    parser = argparse.ArgumentParser(
        description="Send RU+UZ restart notice to all known private users and groups."
    )
    parser.add_argument(
        "--data-file",
        default=os.path.join("data", "purchase_reports.json"),
        help="Path to purchase_reports.json",
    )
    parser.add_argument(
        "--bot-token",
        default=os.getenv("BOT_TOKEN", "").strip(),
        help="Telegram bot token (defaults to BOT_TOKEN env var)",
    )
    parser.add_argument(
        "--web-app-url",
        default=os.getenv("WEB_APP_URL", "").strip(),
        help="Optional WEB_APP_URL for the inline WebApp button",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        default=2,
        help="Expected temporary unavailability in minutes",
    )
    parser.add_argument(
        "--reason",
        default="Обновление / Yangilanish",
        help="Restart reason shown in both languages",
    )
    parser.add_argument(
        "--messages-per-second",
        type=float,
        default=12.0,
        help="Sending speed limit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call Telegram API, print only recipient stats",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.data_file):
        print(f"[RESTART NOTICE] data file not found: {args.data_file}", file=sys.stderr)
        return 2

    if not args.dry_run and not str(args.bot_token).strip():
        print("[RESTART NOTICE] BOT_TOKEN is required", file=sys.stderr)
        return 2

    try:
        store = _load_reports_store(args.data_file)
    except Exception as error:
        print(f"[RESTART NOTICE] failed to read data: {error}", file=sys.stderr)
        return 2

    private_recipients = _collect_private_recipients(store)
    group_recipients = _collect_group_recipients(store)
    total_private = len(private_recipients)
    total_groups = len(group_recipients)
    total = total_private + total_groups
    print(
        f"[RESTART NOTICE] recipients private={total_private} groups={total_groups} total={total}"
    )

    if total <= 0:
        print("[RESTART NOTICE] no recipients, skipping")
        return 0

    text = _build_notice_text(args.minutes, args.reason)
    reply_markup = _build_reply_markup(args.web_app_url)
    delay_seconds = max(1.0 / max(float(args.messages_per_second), 1.0), 0.05)

    if args.dry_run:
        print("[RESTART NOTICE] dry-run enabled, no messages sent")
        return 0

    sent = 0
    failed = 0

    def send_to(chat_ids, *, with_markup):
        nonlocal sent, failed
        for chat_id in chat_ids:
            try:
                result = _send_notice(
                    bot_token=args.bot_token,
                    chat_id=chat_id,
                    text=text,
                    reply_markup=reply_markup if with_markup else None,
                )
                if bool(result.get("ok")):
                    sent += 1
                else:
                    failed += 1
                    print(
                        f"[RESTART NOTICE] send failed chat_id={chat_id} result={result}",
                        file=sys.stderr,
                    )
            except (HTTPError, URLError, TimeoutError) as error:
                failed += 1
                print(
                    f"[RESTART NOTICE] send failed chat_id={chat_id} error={error}",
                    file=sys.stderr,
                )
            except Exception as error:  # pragma: no cover
                failed += 1
                print(
                    f"[RESTART NOTICE] send failed chat_id={chat_id} error={error}",
                    file=sys.stderr,
                )
            time.sleep(delay_seconds)

    send_to(private_recipients, with_markup=True)
    send_to(group_recipients, with_markup=False)

    print(f"[RESTART NOTICE] done sent={sent}/{total} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

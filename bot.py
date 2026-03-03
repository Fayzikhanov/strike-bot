import asyncio
import base64
import calendar
import datetime
import html
import io
import sys
import json
import re
import time
import threading
import uuid
import hashlib
import http.cookiejar
import ftplib
import ssl
from decimal import Decimal, ROUND_DOWN
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen
from urllib.error import URLError, HTTPError
from zoneinfo import ZoneInfo
from telegram.ext import MessageHandler, filters
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    MenuButtonWebApp,
    WebAppInfo,
)
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.ext import CallbackQueryHandler

from payment_verification import PaymentVerificationService

import os
try:
    import certifi
except Exception:
    certifi = None


def load_dotenv_file(path, *, override=True):
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            value = value.strip().strip("\"")
            if override or not os.getenv(key, "").strip():
                os.environ[key] = value


load_dotenv_file(os.path.join(os.path.dirname(__file__), ".env"), override=True)


def _env_int(name, default_value):
    raw_value = str(os.getenv(name, str(default_value))).strip()
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return int(default_value)


TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEB_APP_URL = os.getenv("WEB_APP_URL", "").strip()

_TUNNEL_URL_FILE = os.path.join(os.path.dirname(__file__), "data", "web_app_url.txt")

def get_web_app_url() -> str:
    """Read WEB_APP_URL dynamically so tunnel URL changes take effect without bot restart."""
    try:
        with open(_TUNNEL_URL_FILE) as _f:
            _url = _f.read().strip()
            if _url:
                return _url
    except (OSError, IOError):
        pass
    return WEB_APP_URL

BASE_IP = "83.69.139.205"
A2S_TIMEOUT = 3.0
A2S_COOLDOWN_SECONDS = float(os.getenv("A2S_COOLDOWN_SECONDS", "60").strip() or "60")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8090"))
REPORTS_TIMEZONE = ZoneInfo("Asia/Tashkent")
REPORTS_SCHEDULER_CHECK_SECONDS = 20
REPORTS_STORAGE_DIR = os.path.join(os.path.dirname(__file__), "data")
REPORTS_STORAGE_PATH = os.path.join(REPORTS_STORAGE_DIR, "purchase_reports.json")
REPORTS_CHAT_ID_ENV = os.getenv("REPORTS_CHAT_ID", "").strip()
MAX_SCREENSHOT_BYTES = 8 * 1024 * 1024
PAYMENT_MAX_SCREENSHOT_ATTEMPTS = max(
    _env_int("PAYMENT_MAX_SCREENSHOT_ATTEMPTS", 3),
    1,
)
PAYMENT_ATTEMPT_WINDOW_SECONDS = max(
    _env_int("PAYMENT_ATTEMPT_WINDOW_SECONDS", 7200),
    300,
)
PAYMENT_USER_BAN_SECONDS = max(
    _env_int("PAYMENT_USER_BAN_SECONDS", 600),
    60,
)
PAYMENT_USER_FAILURE_WINDOW_SECONDS = max(
    _env_int("PAYMENT_USER_FAILURE_WINDOW_SECONDS", PAYMENT_ATTEMPT_WINDOW_SECONDS),
    300,
)
PAYMENT_UPLOAD_SESSION_SECONDS = max(
    _env_int("PAYMENT_UPLOAD_SESSION_SECONDS", 300),
    60,
)

SERVERS = {
    "public": {
        "title": "🔥 Public (15-18)",
        "servers": [27015, 27016, 27017, 27018],
    },

    "cw1": {
        "title": "🎯 ClanWar (MIX) [1] (01-05)",
        "servers": [27001, 27002, 27003, 27004, 27005],
    },

    "cw2": {
        "title": "🎯 ClanWar (MIX) [2] (06-11)",
        "servers": [27006, 27007, 27008, 27009, 27010, 27011],
    },
}
KNOWN_PORTS = {port for category in SERVERS.values() for port in category["servers"]}

DATA_URL_PATTERN = re.compile(r"^data:(?P<mime>[\w.+-]+/[\w.+-]+);base64,(?P<data>[A-Za-z0-9+/=\s]+)$")
FILENAME_SAFE_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
STEAM_ID_PATTERN = re.compile(r"^STEAM_[01]:[01]:\d{5,15}$", re.IGNORECASE)
USERS_INI_ENTRY_PATTERN = re.compile(
    r'^\s*(?P<comment>;)?\s*"(?P<nick>[^"]*)"\s*"(?P<password>[^"]*)"\s*"(?P<flags>[^"]+)"\s*"(?P<access>[^"]*)"\s*"(?P<days>-?\d*)"(?:\s*;.*|\s+.*)?$'
)
MONITORING_TABLE_ROW_PATTERN = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
MONITORING_CELL_PATTERN = re.compile(r"<td[^>]*>(.*?)</td>", re.IGNORECASE | re.DOTALL)
MONITORING_ADDRESS_PATTERN = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})")
MONITORING_PLAYERS_PATTERN = re.compile(r"(\d+)\s*из\s*(\d+)", re.IGNORECASE)
MONITORING_MAP_BR_PATTERN = re.compile(r"<br\s*/?>\s*([^<\s]+)", re.IGNORECASE)
NICKNAME_ALLOWED_PATTERN = re.compile(r"^[A-Za-z0-9_\-!^~*()]{1,25}$")
PASSWORD_ALLOWED_PATTERN = re.compile(r"^[A-Za-z0-9]{1,20}$")
PRIVILEGE_IDENTIFIER_NICKNAME = "nickname"
PRIVILEGE_IDENTIFIER_STEAM = "steam"
MONITORING_URL = os.getenv("MONITORING_URL", "https://strike.uz/").strip() or "https://strike.uz/"
MONITORING_TIMEOUT_SECONDS = float(os.getenv("MONITORING_TIMEOUT_SECONDS", "8").strip() or "8")
MONITORING_CACHE_TTL_SECONDS = int(os.getenv("MONITORING_CACHE_TTL_SECONDS", "20").strip() or "20")

PHPMYADMIN_BASE_URL = os.getenv("PHPMYADMIN_BASE_URL", "").strip().rstrip("/")
PHPMYADMIN_LOGIN = os.getenv("PHPMYADMIN_LOGIN", "").strip()
PHPMYADMIN_PASSWORD = os.getenv("PHPMYADMIN_PASSWORD", "").strip()
PHPMYADMIN_SERVER = os.getenv("PHPMYADMIN_SERVER", "1").strip() or "1"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TELEGRAM_APP_API_HASH = os.getenv("TELEGRAM_APP_API_HASH", "").strip()
TELEGRAM_SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "").strip()

BONUS_DB_PUBLIC = os.getenv("BONUS_DB_PUBLIC", "c_2strike").strip() or "c_2strike"
BONUS_TABLE_NAME = os.getenv("BONUS_TABLE_NAME", "AesStatsPublic").strip() or "AesStatsPublic"
BONUS_ROW_ID_COLUMN = os.getenv("BONUS_ROW_ID_COLUMN", "id").strip() or "id"
BONUS_STEAM_ID_COLUMN = os.getenv("BONUS_STEAM_ID_COLUMN", "steamid").strip() or "steamid"
BONUS_NAME_COLUMN = os.getenv("BONUS_NAME_COLUMN", "name").strip() or "name"
BONUS_VALUE_COLUMN = os.getenv("BONUS_VALUE_COLUMN", "bonus_count").strip() or "bonus_count"

FTP_HOST = os.getenv("FTP_HOST", "").strip()
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER", "").strip()
FTP_PASSWORD = os.getenv("FTP_PASSWORD", "").strip()
FTP_TIMEOUT_SECONDS = float(os.getenv("FTP_TIMEOUT_SECONDS", "20").strip() or "20")
FTP_USERS_INI_SUFFIX = (
    os.getenv("FTP_USERS_INI_SUFFIX", "cs/cstrike/addons/amxmodx/configs/users.ini").strip().strip("/")
    or "cs/cstrike/addons/amxmodx/configs/users.ini"
)
DEFAULT_FTP_SERVER_FOLDER_BY_PORT = {
    27015: "service790",
    27016: "service802",
    27017: "service800",
    27018: "service803",
    27001: "service804",
    27002: "service805",
    27003: "service806",
    27004: "service814",
    27005: "service831",
    27006: "service832",
    27007: "service833",
    27008: "service834",
    27009: "service835",
    27010: "service836",
    27011: "service837",
}
FTP_SERVER_FOLDERS_BY_PORT_RAW = os.getenv("FTP_SERVER_FOLDERS_BY_PORT", "").strip()


def _sanitize_ftp_folder_name(raw_value):
    folder_name = str(raw_value or "").strip().strip("/")
    if not folder_name:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9._#-]+", folder_name):
        raise ValueError("FTP folder contains unsupported characters")
    return folder_name


def _load_ftp_server_folder_mapping(raw_value):
    mapping = dict(DEFAULT_FTP_SERVER_FOLDER_BY_PORT)
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return mapping

    try:
        parsed = json.loads(raw_text)
    except Exception as error:
        print(
            f"[CONFIG WARNING] Failed to parse FTP_SERVER_FOLDERS_BY_PORT: {error}",
            file=sys.stderr,
        )
        return mapping

    if not isinstance(parsed, dict):
        print("[CONFIG WARNING] FTP_SERVER_FOLDERS_BY_PORT must be a JSON object", file=sys.stderr)
        return mapping

    for raw_port, raw_folder in parsed.items():
        try:
            port = int(str(raw_port).strip())
        except (TypeError, ValueError):
            continue
        if port <= 0:
            continue
        try:
            folder_name = _sanitize_ftp_folder_name(raw_folder)
        except ValueError:
            continue
        if folder_name:
            mapping[port] = folder_name

    return mapping


FTP_SERVER_FOLDER_BY_PORT = _load_ftp_server_folder_mapping(FTP_SERVER_FOLDERS_BY_PORT_RAW)
FTP_USERS_INI_PATH_CACHE = {}


def _redact_sensitive_text(raw_text):
    text = str(raw_text or "")
    for secret in (
        FTP_PASSWORD,
        FTP_USER,
        PHPMYADMIN_PASSWORD,
        PHPMYADMIN_LOGIN,
        TOKEN,
        OPENAI_API_KEY,
        TELEGRAM_APP_API_HASH,
        TELEGRAM_SESSION_STRING,
    ):
        value = str(secret or "").strip()
        if value:
            text = text.replace(value, "[REDACTED]")
    return text


def _normalize_chat_id(raw_value):
    if raw_value is None:
        return None

    value = str(raw_value).strip()
    if not value:
        return None
    if value.startswith("@"):
        return value

    try:
        return int(value)
    except ValueError:
        return value


def _build_default_reports_store():
    return {
        "reports_chat_id": _normalize_chat_id(REPORTS_CHAT_ID_ENV),
        "reports_chat_title": "",
        "purchases": [],
        "balances": {},
        "balance_transactions": [],
        "payment_attempts": {},
        "payment_user_violations": {},
        "last_reports": {
            "daily": "",
            "weekly": "",
            "monthly": "",
        },
    }


def _load_reports_store():
    os.makedirs(REPORTS_STORAGE_DIR, exist_ok=True)
    default_store = _build_default_reports_store()

    if not os.path.exists(REPORTS_STORAGE_PATH):
        return default_store

    try:
        with open(REPORTS_STORAGE_PATH, "r", encoding="utf-8") as source:
            loaded = json.load(source)
    except Exception as error:
        print(
            f"[REPORTS WARNING] Failed to read reports store: {_redact_sensitive_text(error)}",
            file=sys.stderr,
        )
        return default_store

    if not isinstance(loaded, dict):
        return default_store

    store = default_store
    store["reports_chat_id"] = _normalize_chat_id(loaded.get("reports_chat_id")) or store["reports_chat_id"]
    store["reports_chat_title"] = str(loaded.get("reports_chat_title", "")).strip()

    purchases = loaded.get("purchases", [])
    if isinstance(purchases, list):
        store["purchases"] = purchases

    balances = loaded.get("balances", {})
    if isinstance(balances, dict):
        store["balances"] = balances

    balance_transactions = loaded.get("balance_transactions", [])
    if isinstance(balance_transactions, list):
        store["balance_transactions"] = balance_transactions

    payment_attempts = loaded.get("payment_attempts", {})
    if isinstance(payment_attempts, dict):
        store["payment_attempts"] = payment_attempts

    payment_user_violations = loaded.get("payment_user_violations", {})
    if isinstance(payment_user_violations, dict):
        store["payment_user_violations"] = payment_user_violations

    last_reports = loaded.get("last_reports", {})
    if isinstance(last_reports, dict):
        store["last_reports"]["daily"] = str(last_reports.get("daily", "")).strip()
        store["last_reports"]["weekly"] = str(last_reports.get("weekly", "")).strip()
        store["last_reports"]["monthly"] = str(last_reports.get("monthly", "")).strip()

    return store


REPORTS_LOCK = threading.Lock()
REPORTS_STORE = _load_reports_store()
MONITORING_CACHE_LOCK = threading.Lock()
MONITORING_CACHE = {
    "timestamp": 0.0,
    "servers": {},
}
MONITORING_LAST_ERROR_LOG_TS = 0.0
MONITORING_ERROR_LOG_COOLDOWN_SECONDS = 30.0
A2S_STATE_LOCK = threading.Lock()
A2S_DISABLED_UNTIL = 0.0
FTP_PATH_CACHE_LOCK = threading.Lock()
PAYMENT_VERIFIER = PaymentVerificationService()


def _save_reports_store_locked():
    with open(REPORTS_STORAGE_PATH, "w", encoding="utf-8") as target:
        json.dump(REPORTS_STORE, target, ensure_ascii=False, indent=2)


def get_reports_chat_id():
    env_chat_id = _normalize_chat_id(REPORTS_CHAT_ID_ENV)
    if env_chat_id is not None:
        return env_chat_id

    with REPORTS_LOCK:
        return _normalize_chat_id(REPORTS_STORE.get("reports_chat_id"))


def bind_reports_chat(chat_id, chat_title=""):
    normalized_chat_id = _normalize_chat_id(chat_id)
    if normalized_chat_id is None:
        return False

    with REPORTS_LOCK:
        REPORTS_STORE["reports_chat_id"] = normalized_chat_id
        REPORTS_STORE["reports_chat_title"] = str(chat_title or "").strip()
        _save_reports_store_locked()
    return True


def format_money_uzs(amount):
    try:
        value = int(amount)
    except (TypeError, ValueError):
        value = 0
    return f"{value:,}".replace(",", " ")


def _normalize_balance_record(raw_value):
    if isinstance(raw_value, dict):
        try:
            balance_value = int(raw_value.get("balance", 0) or 0)
        except (TypeError, ValueError):
            balance_value = 0
        if balance_value < 0:
            balance_value = 0
        try:
            updated_at_value = int(raw_value.get("updated_at", 0) or 0)
        except (TypeError, ValueError):
            updated_at_value = 0
        return {
            "balance": int(balance_value),
            "updated_at": int(updated_at_value),
        }

    try:
        balance_value = int(raw_value or 0)
    except (TypeError, ValueError):
        balance_value = 0
    if balance_value < 0:
        balance_value = 0
    return {
        "balance": int(balance_value),
        "updated_at": 0,
    }


def get_user_balance_snapshot(user_id):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        safe_user_id = 0
    if safe_user_id <= 0:
        return {"balance": 0, "updated_at": 0}

    key = str(safe_user_id)
    with REPORTS_LOCK:
        balances = REPORTS_STORE.setdefault("balances", {})
        if not isinstance(balances, dict):
            balances = {}
            REPORTS_STORE["balances"] = balances
        normalized = _normalize_balance_record(balances.get(key))
        balances[key] = dict(normalized)
    return normalized


def get_user_balance(user_id):
    snapshot = get_user_balance_snapshot(user_id)
    return int(snapshot.get("balance", 0) or 0)


def adjust_user_balance(user_id, delta_amount, *, transaction_type="", metadata=None):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        raise ValueError("Invalid user id")
    if safe_user_id <= 0:
        raise ValueError("Invalid user id")

    try:
        delta = int(delta_amount)
    except (TypeError, ValueError):
        raise ValueError("Invalid balance delta")
    if delta == 0:
        snapshot = get_user_balance_snapshot(safe_user_id)
        return int(snapshot.get("balance", 0) or 0), int(snapshot.get("balance", 0) or 0)

    now_ts = int(time.time())
    tx_type = str(transaction_type or "").strip().lower() or "adjustment"
    tx_metadata = metadata if isinstance(metadata, dict) else {}
    key = str(safe_user_id)

    with REPORTS_LOCK:
        balances = REPORTS_STORE.setdefault("balances", {})
        if not isinstance(balances, dict):
            balances = {}
            REPORTS_STORE["balances"] = balances

        current_record = _normalize_balance_record(balances.get(key))
        balance_before = int(current_record.get("balance", 0) or 0)
        balance_after = balance_before + delta
        if balance_after < 0:
            raise ValueError("Insufficient balance")

        next_record = {
            "balance": int(balance_after),
            "updated_at": now_ts,
        }
        balances[key] = next_record

        transactions = REPORTS_STORE.setdefault("balance_transactions", [])
        if not isinstance(transactions, list):
            transactions = []
            REPORTS_STORE["balance_transactions"] = transactions
        transactions.append(
            {
                "id": uuid.uuid4().hex[:14],
                "created_at": now_ts,
                "user_id": safe_user_id,
                "type": tx_type,
                "delta": int(delta),
                "before": int(balance_before),
                "after": int(balance_after),
                "meta": tx_metadata,
            }
        )
        if len(transactions) > 5000:
            del transactions[: len(transactions) - 5000]

        _save_reports_store_locked()

    return int(balance_before), int(balance_after)


def get_user_balance_transactions(user_id, *, limit=120):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        safe_user_id = 0
    if safe_user_id <= 0:
        return []

    try:
        max_items = max(int(limit), 1)
    except (TypeError, ValueError):
        max_items = 120
    max_items = min(max_items, 300)

    with REPORTS_LOCK:
        transactions = REPORTS_STORE.setdefault("balance_transactions", [])
        if not isinstance(transactions, list):
            return []
        matched = [
            item
            for item in transactions
            if isinstance(item, dict) and int(item.get("user_id", 0) or 0) == safe_user_id
        ]

    matched.sort(key=lambda item: int(item.get("created_at", 0) or 0), reverse=True)
    normalized = []
    for item in matched[:max_items]:
        meta = item.get("meta")
        normalized.append(
            {
                "id": str(item.get("id", "")).strip() or uuid.uuid4().hex[:14],
                "created_at": int(item.get("created_at", 0) or 0),
                "user_id": safe_user_id,
                "type": str(item.get("type", "")).strip().lower() or "adjustment",
                "delta": int(item.get("delta", 0) or 0),
                "before": int(item.get("before", 0) or 0),
                "after": int(item.get("after", 0) or 0),
                "meta": dict(meta) if isinstance(meta, dict) else {},
            }
        )
    return normalized


def get_all_users_balance_total():
    total_balance = 0
    users_with_balance = 0
    with REPORTS_LOCK:
        balances = REPORTS_STORE.setdefault("balances", {})
        if not isinstance(balances, dict):
            return 0, 0
        for value in balances.values():
            normalized = _normalize_balance_record(value)
            balance_value = int(normalized.get("balance", 0) or 0)
            total_balance += balance_value
            if balance_value > 0:
                users_with_balance += 1
    return int(total_balance), int(users_with_balance)


def get_cashback_totals(*, start_ts=0, end_ts=0):
    total_cashback = 0
    with REPORTS_LOCK:
        transactions = REPORTS_STORE.setdefault("balance_transactions", [])
        if not isinstance(transactions, list):
            return 0
        for item in transactions:
            if not isinstance(item, dict):
                continue
            if str(item.get("type", "")).strip().lower() != "cashback":
                continue
            created_at = int(item.get("created_at", 0) or 0)
            if start_ts > 0 and created_at < int(start_ts):
                continue
            if end_ts > 0 and created_at >= int(end_ts):
                continue
            delta = int(item.get("delta", 0) or 0)
            if delta > 0:
                total_cashback += delta
    return int(total_cashback)


def _folder_name_variants(raw_folder_name):
    value = str(raw_folder_name or "").strip().strip("/").lower()
    if not value:
        return set()
    variants = {value, value.replace("#", "")}
    match = re.search(r"service#?(\d+)$", value, re.IGNORECASE)
    if match:
        service_id = match.group(1)
        variants.add(f"service{service_id}")
        variants.add(f"service#{service_id}")
    return {item for item in variants if item}


def _resolve_server_id_for_purchase_record(record):
    direct_server_id = str(record.get("server_id", "")).strip()
    if _is_known_server(direct_server_id):
        return str(_normalize_server_port(direct_server_id))

    users_ini_path = str(record.get("issued_users_ini_path", "")).strip().strip("/")
    folder_name = users_ini_path.split("/", 1)[0].strip() if users_ini_path else ""
    folder_variants = _folder_name_variants(folder_name)
    if not folder_variants:
        return ""

    merged_mapping = dict(DEFAULT_FTP_SERVER_FOLDER_BY_PORT)
    merged_mapping.update(FTP_SERVER_FOLDER_BY_PORT)
    for port, mapped_folder in merged_mapping.items():
        if _folder_name_variants(mapped_folder) & folder_variants:
            return str(int(port))
    return ""


def get_user_privilege_snapshots(user_id, *, limit=30):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        safe_user_id = 0
    if safe_user_id <= 0:
        return []

    try:
        max_items = max(int(limit), 1)
    except (TypeError, ValueError):
        max_items = 30
    max_items = min(max_items, 100)

    with REPORTS_LOCK:
        purchases = REPORTS_STORE.setdefault("purchases", [])
        if not isinstance(purchases, list):
            return []
        user_privilege_records = [
            item
            for item in purchases
            if isinstance(item, dict)
            and int(item.get("user_id", 0) or 0) == safe_user_id
            and str(item.get("status", "")).strip().lower() == "active"
            and str(item.get("product_type", "privilege")).strip().lower() == "privilege"
        ]

    user_privilege_records.sort(
        key=lambda item: int(item.get("created_at", 0) or 0),
        reverse=True,
    )
    now_local = datetime.datetime.now(REPORTS_TIMEZONE)
    snapshots = []
    seen_account_keys = set()

    for record in user_privilege_records:
        created_at = int(record.get("created_at", 0) or 0)
        if created_at <= 0:
            continue

        identifier_type = normalize_privilege_identifier_type(
            record.get("issued_identifier_type", record.get("identifier_type", PRIVILEGE_IDENTIFIER_NICKNAME))
        )
        nickname = str(record.get("nickname", "")).strip()
        steam_id = normalize_steam_id(record.get("steam_id", ""))
        identifier_value = steam_id if identifier_type == PRIVILEGE_IDENTIFIER_STEAM else nickname
        if not identifier_value:
            continue

        server_id = _resolve_server_id_for_purchase_record(record)
        account_key = "|".join([server_id or str(record.get("server", "")).strip(), identifier_type, identifier_value.casefold()])
        if account_key in seen_account_keys:
            continue

        duration_months = max(int(record.get("duration_months", 0) or 0), 0)
        calculated_total_days = duration_months * 30
        issued_after_days = max(int(record.get("issued_after_days", 0) or 0), 0)
        total_days = max(issued_after_days, calculated_total_days, 30)

        created_local_dt = datetime.datetime.fromtimestamp(created_at, tz=REPORTS_TIMEZONE)
        days_passed = max((now_local.date() - created_local_dt.date()).days, 0)
        remaining_days = max(total_days - days_passed, 0)
        if remaining_days <= 0:
            continue

        issued_privilege = str(record.get("issued_privilege", "")).strip()
        privilege_label = issued_privilege or str(record.get("privilege", "")).strip()
        privilege_key = _normalize_sale_privilege_key(privilege_label)
        if not privilege_key:
            continue

        seen_account_keys.add(account_key)
        snapshots.append(
            {
                "id": str(record.get("id", "")).strip(),
                "created_at": created_at,
                "server_id": server_id,
                "server_name": str(record.get("server", "")).strip(),
                "privilege_key": privilege_key,
                "privilege_label": privilege_label,
                "identifier_type": identifier_type,
                "nickname": nickname,
                "steam_id": steam_id,
                "remaining_days": int(remaining_days),
                "total_days": int(total_days),
                "days_passed": int(days_passed),
                "can_renew": bool(server_id and privilege_key),
            }
        )
        if len(snapshots) >= max_items:
            break

    return snapshots


def decode_image_data_url(data_url):
    text = str(data_url or "").strip()
    match = DATA_URL_PATTERN.match(text)
    if not match:
        raise ValueError("Invalid screenshot format")

    mime_type = match.group("mime")
    encoded = match.group("data")

    try:
        content = base64.b64decode(encoded, validate=True)
    except Exception as error:
        raise ValueError("Invalid screenshot encoding") from error

    if not content:
        raise ValueError("Screenshot is empty")

    if len(content) > MAX_SCREENSHOT_BYTES:
        raise ValueError("Screenshot is too large")

    return content, mime_type


def safe_filename(filename, mime_type):
    base_name = FILENAME_SAFE_PATTERN.sub("_", str(filename or "").strip()).strip("._")
    if not base_name:
        base_name = "payment-proof"

    if "." not in base_name:
        extension = {
            "image/jpeg": ".jpg",
            "image/jpg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
        }.get(mime_type.lower(), ".jpg")
        base_name = f"{base_name}{extension}"

    return base_name[:80]


def normalize_steam_id(raw_value):
    value = str(raw_value or "").strip().upper()
    return value


def is_valid_steam_id(steam_id):
    normalized = normalize_steam_id(steam_id)
    return bool(STEAM_ID_PATTERN.match(normalized))


def normalize_privilege_identifier_type(raw_value):
    normalized = str(raw_value or "").strip().lower()
    if normalized in {"steam", "steam_id", "steamid"}:
        return PRIVILEGE_IDENTIFIER_STEAM
    return PRIVILEGE_IDENTIFIER_NICKNAME


PRIVILEGE_FLAGS_BY_KEY = {
    "vip": "t",
    "prime": "pt",
    "legend": "pst",
    "moder": "cdefijmtu",
    "admin": "abcdefijmtu",
    "admin-cw": "abcdefijmtu",
    "gl-admin": "abcdefghijklmnopqrstu",
}
PRIVILEGE_KEY_BY_FLAGS = {
    "t": "vip",
    "pt": "prime",
    "pst": "legend",
    "cdefijmtu": "moder",
    "abcdefijmtu": "admin",
    "abcdefghijklmnopqrstu": "gl-admin",
}
PRIVILEGE_LABELS_BY_KEY = {
    "vip": "VIP",
    "prime": "PRIME",
    "legend": "LEGEND",
    "moder": "MODER",
    "admin": "ADMIN",
    "admin-cw": "ADMIN CW/MIX",
    "gl-admin": "GL ADMIN",
}
NICKNAME_ONLY_PRIVILEGE_KEYS = {
    "moder",
    "admin",
    "gl-admin",
    "admin-cw",
}
PRIVILEGE_TIER_BY_KEY = {
    "vip": 1,
    "prime": 2,
    "legend": 3,
    "moder": 4,
    "admin": 5,
    "admin-cw": 5,
    "gl-admin": 6,
}
PUBLIC_STYLE_1_ONE_MONTH_PRICE_BY_KEY = {
    "vip": 29000,
    "prime": 49000,
    "legend": 79000,
}
PUBLIC_STYLE_1_TARIFF_PRICE_BY_KEY = {
    "vip": {1: 29000, 2: 50000, 3: 70000},
    "prime": {1: 49000, 2: 90000, 3: 120000},
    "legend": {1: 79000, 2: 140000, 3: 180000},
}
ONLY_DUST_VIP_TARIFF_PRICE_BY_KEY = {
    "vip": {1: 25000, 2: 45000, 3: 60000},
}
CSDM_HNS_TARIFF_PRICE_BY_KEY = {
    "vip": {1: 15000, 2: 25000, 3: 38000},
    "moder": {1: 60000, 2: 105000, 3: 150000},
    "admin": {1: 100000, 2: 175000, 3: 240000},
}
MIX_TARIFF_PRICE_BY_KEY = {
    "moder": {1: 80000, 2: 140000, 3: 190000},
    "admin": {1: 100000, 2: 175000, 3: 240000},
}
SERVER_PRIVILEGE_TARIFFS_BY_PORT = {
    27015: PUBLIC_STYLE_1_TARIFF_PRICE_BY_KEY,
    27016: ONLY_DUST_VIP_TARIFF_PRICE_BY_KEY,
    27017: {
        "moder": CSDM_HNS_TARIFF_PRICE_BY_KEY["moder"],
        "admin": CSDM_HNS_TARIFF_PRICE_BY_KEY["admin"],
    },
    27018: CSDM_HNS_TARIFF_PRICE_BY_KEY,
}
PUBLIC_STYLE_1_KEYS = {"vip", "prime", "legend"}
PUBLIC_STYLE_1_PORT = 27015
PUBLIC_SERVER_PORTS = set(SERVERS.get("public", {}).get("servers", []))
BONUS_ENABLED_SERVER_PORTS = {27015, 27016}
BONUS_TARIFF_PRICE_BY_BONUS_AMOUNT = {
    2250: 10000,
    7500: 30000,
    14000: 50000,
}
DEFAULT_PURCHASE_CASHBACK_PERCENT = 5
LEGEND_PURCHASE_CASHBACK_PERCENT = 10


def _normalize_server_port(server_id):
    try:
        return int(str(server_id).strip())
    except (TypeError, ValueError):
        return None


def _is_known_server(server_id):
    port = _normalize_server_port(server_id)
    return port in KNOWN_PORTS


def _is_public_style_1_server(server_id, server_name):
    port = _normalize_server_port(server_id)
    if port == PUBLIC_STYLE_1_PORT:
        return True

    normalized_name = str(server_name or "").strip().lower()
    return "public style #1" in normalized_name


def _is_bonus_supported_server(server_id):
    port = _normalize_server_port(server_id)
    return port in BONUS_ENABLED_SERVER_PORTS


def _get_server_offer_config(server_id):
    port = _normalize_server_port(server_id)
    if port is None or port not in KNOWN_PORTS:
        return {}

    if port in SERVER_PRIVILEGE_TARIFFS_BY_PORT:
        return SERVER_PRIVILEGE_TARIFFS_BY_PORT[port]

    if port not in PUBLIC_SERVER_PORTS:
        return MIX_TARIFF_PRICE_BY_KEY

    return {}


def _get_server_tariff_price(server_id, privilege_key, months_value):
    offers = _get_server_offer_config(server_id)
    if not offers:
        return None

    key = str(privilege_key or "").strip().lower()
    tariffs = offers.get(key)
    if not tariffs and key in {"admin-cw", "gl-admin"}:
        tariffs = offers.get("admin")
    if not tariffs:
        return None

    try:
        months_int = int(months_value)
    except (TypeError, ValueError):
        return None

    return tariffs.get(months_int)


def _build_ftp_users_ini_path_candidates(server_id):
    port = _normalize_server_port(server_id)
    if port is None:
        return []

    raw_folders = []
    for candidate in (
        FTP_SERVER_FOLDER_BY_PORT.get(port, ""),
        DEFAULT_FTP_SERVER_FOLDER_BY_PORT.get(port, ""),
    ):
        folder_name = str(candidate or "").strip()
        if folder_name:
            raw_folders.append(folder_name)

    # Support both "service790" and "service#790" naming styles.
    expanded_folders = list(raw_folders)
    for folder_name in raw_folders:
        match = re.search(r"service#?(\d+)$", folder_name, re.IGNORECASE)
        if not match:
            continue
        service_id = match.group(1)
        expanded_folders.append(f"service{service_id}")
        expanded_folders.append(f"service#{service_id}")

    sanitized_folders = []
    seen_folders = set()
    for raw_folder in expanded_folders:
        try:
            safe_folder = _sanitize_ftp_folder_name(raw_folder)
        except ValueError:
            continue
        if not safe_folder or safe_folder in seen_folders:
            continue
        seen_folders.add(safe_folder)
        sanitized_folders.append(safe_folder)

    candidates = []
    for folder_name in sanitized_folders:
        path = f"{folder_name}/{FTP_USERS_INI_SUFFIX}".strip("/")
        if not path or ".." in path:
            continue
        candidates.append(path)
    return candidates


def _probe_ftp_users_ini_path(candidates):
    if not FTP_HOST or not FTP_USER or not FTP_PASSWORD:
        raise RuntimeError("FTP credentials are not configured")

    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT, timeout=FTP_TIMEOUT_SECONDS)
        ftp.login(FTP_USER, FTP_PASSWORD)
        ftp.set_pasv(True)
        for path in candidates:
            try:
                response = str(ftp.sendcmd(f"MLST {path}")).strip()
            except ftplib.error_perm:
                continue
            except Exception:
                continue

            if response.startswith("250"):
                return path

    return ""


def _resolve_ftp_users_ini_path(server_id, server_name="", *, raise_if_missing=True):
    port = _normalize_server_port(server_id)
    if port is None:
        if raise_if_missing:
            raise ValueError("Invalid server id")
        return ""

    with FTP_PATH_CACHE_LOCK:
        cached_path = str(FTP_USERS_INI_PATH_CACHE.get(port, "")).strip()
    if cached_path:
        return cached_path

    candidates = _build_ftp_users_ini_path_candidates(server_id)
    if not candidates:
        if raise_if_missing:
            readable_server = str(server_name or server_id or "").strip()
            raise ValueError(f"FTP folder is not configured for server: {readable_server}")
        return ""

    resolved_path = _probe_ftp_users_ini_path(candidates)
    if resolved_path:
        with FTP_PATH_CACHE_LOCK:
            FTP_USERS_INI_PATH_CACHE[port] = resolved_path
        return resolved_path

    if raise_if_missing:
        readable_server = str(server_name or server_id or "").strip()
        raise ValueError(f"users.ini not found on FTP for server: {readable_server}")
    return ""


def _normalize_privilege_key(privilege_name):
    sale_key = _normalize_sale_privilege_key(privilege_name)
    if sale_key:
        return sale_key

    return _normalize_privilege_key_from_flags(privilege_name)


def _normalize_sale_privilege_key(privilege_name):
    normalized = str(privilege_name or "").strip().lower()
    normalized = re.sub(r"[\s_]+", "", normalized)
    normalized = normalized.replace("/", "")
    if not normalized:
        return ""

    if "gladmin" in normalized or "globaladmin" in normalized:
        return "gl-admin"
    if "admincwmix" in normalized or "admincw" in normalized:
        return "admin-cw"
    if "moderator" in normalized or "moder" in normalized:
        return "moder"
    if "admin" in normalized:
        return "admin"
    if "legend" in normalized:
        return "legend"
    if "prime" in normalized:
        return "prime"
    if "vip" in normalized:
        return "vip"

    return ""


def _get_privilege_cashback_percent(privilege_name):
    normalized_key = _normalize_sale_privilege_key(privilege_name)
    if normalized_key == "legend":
        return LEGEND_PURCHASE_CASHBACK_PERCENT
    return DEFAULT_PURCHASE_CASHBACK_PERCENT


def _calculate_privilege_cashback_amount(privilege_name, paid_amount):
    try:
        safe_paid_amount = max(int(paid_amount or 0), 0)
    except (TypeError, ValueError):
        safe_paid_amount = 0
    if safe_paid_amount <= 0:
        return 0

    cashback_percent = _get_privilege_cashback_percent(privilege_name)
    return int((safe_paid_amount * cashback_percent) // 100)


def _is_steam_allowed_for_privilege(privilege_name):
    sale_key = _normalize_sale_privilege_key(privilege_name)
    if not sale_key:
        return True
    return sale_key not in NICKNAME_ONLY_PRIVILEGE_KEYS


def _normalize_privilege_key_from_flags(flags):
    normalized = str(flags or "").strip().lower()
    if normalized in PRIVILEGE_KEY_BY_FLAGS:
        return PRIVILEGE_KEY_BY_FLAGS[normalized]
    return ""


def _label_for_privilege_flags(flags):
    privilege_key = _normalize_privilege_key_from_flags(flags)
    if privilege_key:
        return PRIVILEGE_LABELS_BY_KEY.get(privilege_key, privilege_key.upper())
    return str(flags or "").strip().upper()


def _round_down_to_nearest_500(amount_value):
    try:
        normalized = Decimal(str(amount_value))
    except Exception:
        return 0

    if normalized <= 0:
        return 0

    return int((normalized // Decimal("500")) * Decimal("500"))


def _get_public_style_1_tariff_price(privilege_key, months_value):
    tariffs = PUBLIC_STYLE_1_TARIFF_PRICE_BY_KEY.get(str(privilege_key or "").strip().lower())
    if not tariffs:
        return None

    try:
        months_int = int(months_value)
    except (TypeError, ValueError):
        return None

    return tariffs.get(months_int)


def _calculate_public_style_1_upgrade_due_amount(target_price, existing_privilege_key, existing_days):
    try:
        target_price_value = Decimal(str(int(target_price)))
    except Exception:
        target_price_value = Decimal("0")

    existing_key = str(existing_privilege_key or "").strip().lower()
    monthly_price = PUBLIC_STYLE_1_ONE_MONTH_PRICE_BY_KEY.get(existing_key, 0)
    try:
        days_value = max(int(existing_days), 0)
    except (TypeError, ValueError):
        days_value = 0

    if monthly_price <= 0 or days_value <= 0:
        return int(max(target_price_value, Decimal("0"))), 0

    credit_raw = (Decimal(str(monthly_price)) / Decimal("30")) * Decimal(str(days_value))
    due_raw = target_price_value - credit_raw
    if due_raw < 0:
        due_raw = Decimal("0")

    due_amount = _round_down_to_nearest_500(due_raw)
    credit_amount = int(credit_raw.to_integral_value(rounding=ROUND_DOWN))
    return due_amount, max(credit_amount, 0)


def _sanitize_nickname(raw_value):
    value = str(raw_value or "").strip()
    if not value:
        raise ValueError("Nickname is required")
    if not NICKNAME_ALLOWED_PATTERN.fullmatch(value):
        raise ValueError(
            "Nickname must be 1-25 chars and use only English letters, digits, _, -, !, ^, ~, *, (, )"
        )
    return value


def _sanitize_password(raw_value, *, field_name="Password", allow_empty=False):
    value = str(raw_value or "").strip()
    if not value:
        if allow_empty:
            return ""
        raise ValueError(f"{field_name} is required")
    if not PASSWORD_ALLOWED_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be 1-20 chars and contain only English letters and digits")
    return value


def _sanitize_privilege_identifier(identifier_type, nickname="", steam_id=""):
    normalized_type = normalize_privilege_identifier_type(identifier_type)
    if normalized_type == PRIVILEGE_IDENTIFIER_STEAM:
        normalized_steam_id = normalize_steam_id(steam_id)
        if not is_valid_steam_id(normalized_steam_id):
            raise ValueError("Invalid STEAM_ID format")
        return normalized_type, normalized_steam_id
    return normalized_type, _sanitize_nickname(nickname)


def _build_users_ini_entry(*, identifier_value, password, privilege_flags, access_mode, duration_days):
    return (
        f"\"{identifier_value}\" \"{password}\" \"{privilege_flags}\" "
        f"\"{access_mode}\" \"{int(duration_days)}\""
    )


def _download_users_ini_bytes(users_ini_path):
    if not FTP_HOST or not FTP_USER or not FTP_PASSWORD:
        raise RuntimeError("FTP credentials are not configured")
    safe_path = str(users_ini_path or "").strip().lstrip("/")
    if not safe_path:
        raise RuntimeError("FTP users.ini path is not configured")

    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT, timeout=FTP_TIMEOUT_SECONDS)
        ftp.login(FTP_USER, FTP_PASSWORD)
        ftp.set_pasv(True)
        try:
            ftp.voidcmd("TYPE I")
        except Exception:
            pass

        original_bytes = bytearray()
        ftp.retrbinary(f"RETR {safe_path}", original_bytes.extend)
        return bytes(original_bytes)


def _upload_users_ini_bytes(updated_bytes, rollback_bytes, users_ini_path):
    if not FTP_HOST or not FTP_USER or not FTP_PASSWORD:
        raise RuntimeError("FTP credentials are not configured")
    safe_path = str(users_ini_path or "").strip().lstrip("/")
    if not safe_path:
        raise RuntimeError("FTP users.ini path is not configured")

    with ftplib.FTP() as ftp:
        ftp.connect(FTP_HOST, FTP_PORT, timeout=FTP_TIMEOUT_SECONDS)
        ftp.login(FTP_USER, FTP_PASSWORD)
        ftp.set_pasv(True)
        try:
            ftp.voidcmd("TYPE I")
        except Exception:
            pass

        ftp.storbinary(f"STOR {safe_path}", io.BytesIO(updated_bytes))

        verified_bytes = bytearray()
        ftp.retrbinary(f"RETR {safe_path}", verified_bytes.extend)
        if bytes(verified_bytes) != bytes(updated_bytes):
            try:
                ftp.storbinary(f"STOR {safe_path}", io.BytesIO(rollback_bytes))
            except Exception as rollback_error:
                raise RuntimeError(
                    f"users.ini verification failed and rollback failed: {rollback_error}"
                ) from rollback_error
            raise RuntimeError("users.ini verification failed after upload")


def _choose_users_ini_newline(content_bytes):
    if b"\r\n" in content_bytes:
        return "\r\n"
    return "\n"


def _parse_users_ini_entry_line(raw_line):
    line_body = raw_line.rstrip("\r\n")
    newline = raw_line[len(line_body):]
    match = USERS_INI_ENTRY_PATTERN.match(line_body)
    if not match:
        return None

    raw_days = str(match.group("days") or "").strip()
    is_permanent = raw_days == ""
    if is_permanent:
        days = None
    else:
        try:
            days = int(raw_days)
        except (TypeError, ValueError):
            days = 0

    return {
        "line_body": line_body,
        "newline": newline,
        "is_disabled": bool(match.group("comment")),
        "nickname": match.group("nick"),
        "password": match.group("password"),
        "flags": match.group("flags"),
        "access": match.group("access"),
        "days": days,
        "is_permanent": bool(is_permanent),
    }


def _users_ini_entry_days_value(entry):
    if not entry:
        return 0
    if bool(entry.get("is_permanent")):
        return 0
    try:
        return max(int(entry.get("days", 0) or 0), 0)
    except (TypeError, ValueError):
        return 0


def _users_ini_entry_is_expired(entry):
    if not entry:
        return True
    if bool(entry.get("is_permanent")):
        return False
    return bool(entry.get("is_disabled")) or _users_ini_entry_days_value(entry) <= 0


def _users_ini_entry_priority(entry):
    if not entry:
        return (-1, -1, -1, -1)

    is_enabled = 0 if bool(entry.get("is_disabled")) else 1
    is_permanent = 1 if bool(entry.get("is_permanent")) else 0
    privilege_key = _normalize_privilege_key_from_flags(entry.get("flags", ""))
    privilege_tier = int(PRIVILEGE_TIER_BY_KEY.get(privilege_key, 0) or 0)
    days_value = _users_ini_entry_days_value(entry)
    return (is_enabled, is_permanent, privilege_tier, days_value)


def _find_users_ini_entry(lines, identifier_type, identifier_value):
    normalized_type = normalize_privilege_identifier_type(identifier_type)
    raw_target = str(identifier_value or "").strip()
    if not raw_target:
        return None

    if normalized_type == PRIVILEGE_IDENTIFIER_STEAM:
        target = normalize_steam_id(raw_target)
    else:
        target = raw_target.casefold()

    found = None
    found_priority = (-1, -1, -1, -1)
    for index, raw_line in enumerate(lines):
        parsed = _parse_users_ini_entry_line(raw_line)
        if not parsed:
            continue
        parsed_value = str(parsed.get("nickname", "")).strip()
        if normalized_type == PRIVILEGE_IDENTIFIER_STEAM:
            if normalize_steam_id(parsed_value) != target:
                continue
        else:
            if parsed_value.casefold() != target:
                continue
        parsed["index"] = index
        priority = _users_ini_entry_priority(parsed)
        if found is None or priority > found_priority:
            found = parsed
            found_priority = priority
    return found


def _extract_privilege_account_from_users_ini(
    *,
    server_id,
    server_name,
    identifier_type=PRIVILEGE_IDENTIFIER_NICKNAME,
    nickname="",
    steam_id="",
):
    users_ini_path = _resolve_ftp_users_ini_path(server_id, server_name, raise_if_missing=False)
    if not users_ini_path:
        return {
            "supported": False,
            "exists": False,
            "identifier_type": normalize_privilege_identifier_type(identifier_type),
            "password": "",
        }

    resolved_identifier_type, resolved_identifier_value = _sanitize_privilege_identifier(
        identifier_type=identifier_type,
        nickname=nickname,
        steam_id=steam_id,
    )
    users_ini_bytes = _download_users_ini_bytes(users_ini_path)
    users_ini_text = users_ini_bytes.decode("latin-1")
    lines = users_ini_text.splitlines(keepends=True)
    found = _find_users_ini_entry(lines, resolved_identifier_type, resolved_identifier_value)
    if not found:
        return {
            "supported": True,
            "exists": False,
            "identifier_type": resolved_identifier_type,
            "nickname": resolved_identifier_value if resolved_identifier_type == PRIVILEGE_IDENTIFIER_NICKNAME else "",
            "steam_id": resolved_identifier_value if resolved_identifier_type == PRIVILEGE_IDENTIFIER_STEAM else "",
            "password": "",
        }

    days_value = _users_ini_entry_days_value(found)
    return {
        "supported": True,
        "exists": True,
        "identifier_type": resolved_identifier_type,
        "nickname": found["nickname"] if resolved_identifier_type == PRIVILEGE_IDENTIFIER_NICKNAME else "",
        "steam_id": (
            normalize_steam_id(found["nickname"])
            if resolved_identifier_type == PRIVILEGE_IDENTIFIER_STEAM
            else ""
        ),
        "flags": found["flags"],
        "privilege": _label_for_privilege_flags(found["flags"]),
        "days": days_value,
        "is_permanent": bool(found.get("is_permanent")),
        "is_disabled": bool(found["is_disabled"]),
        "is_expired": _users_ini_entry_is_expired(found),
        "password": found["password"] if resolved_identifier_type == PRIVILEGE_IDENTIFIER_NICKNAME else "",
    }


def _verify_privilege_password_from_users_ini(
    *,
    server_id,
    server_name,
    nickname,
    password,
):
    users_ini_path = _resolve_ftp_users_ini_path(server_id, server_name, raise_if_missing=False)
    if not users_ini_path:
        return {
            "supported": False,
            "exists": False,
            "valid": False,
        }

    safe_nickname = _sanitize_nickname(nickname)
    safe_password = _sanitize_password(password)
    users_ini_bytes = _download_users_ini_bytes(users_ini_path)
    users_ini_text = users_ini_bytes.decode("latin-1")
    lines = users_ini_text.splitlines(keepends=True)
    found = _find_users_ini_entry(lines, PRIVILEGE_IDENTIFIER_NICKNAME, safe_nickname)
    if not found:
        return {
            "supported": True,
            "exists": False,
            "valid": False,
            "identifier_type": PRIVILEGE_IDENTIFIER_NICKNAME,
        }

    is_valid = safe_password == found["password"]
    days_value = _users_ini_entry_days_value(found)
    return {
        "supported": True,
        "exists": True,
        "valid": is_valid,
        "identifier_type": PRIVILEGE_IDENTIFIER_NICKNAME,
        "nickname": found["nickname"],
        "flags": found["flags"],
        "privilege": _label_for_privilege_flags(found["flags"]),
        "days": days_value,
        "is_permanent": bool(found.get("is_permanent")),
        "is_disabled": bool(found["is_disabled"]),
        "is_expired": _users_ini_entry_is_expired(found),
    }


def issue_privilege_via_ftp_if_required(
    *,
    server_id,
    server_name,
    privilege,
    duration_months,
    identifier_type=PRIVILEGE_IDENTIFIER_NICKNAME,
    nickname="",
    steam_id="",
    password="",
    renewal_requested=False,
    current_password="",
    change_password=False,
    paid_amount=0,
):
    users_ini_path = _resolve_ftp_users_ini_path(server_id, server_name, raise_if_missing=True)

    try:
        months_value = int(duration_months)
    except (TypeError, ValueError):
        raise ValueError("Invalid duration months")
    if months_value <= 0:
        raise ValueError("Invalid duration months")

    target_privilege_key = _normalize_privilege_key(privilege)
    if not target_privilege_key:
        raise ValueError("Unsupported privilege for FTP issuing")
    target_privilege_flags = PRIVILEGE_FLAGS_BY_KEY.get(target_privilege_key)
    if not target_privilege_flags:
        raise ValueError("Failed to resolve privilege flags")

    target_tariff_price = _get_server_tariff_price(server_id, target_privilege_key, months_value)
    if target_tariff_price is None:
        raise ValueError("Unsupported privilege tariff for this duration")

    try:
        paid_amount_value = int(paid_amount)
    except (TypeError, ValueError):
        raise ValueError("Invalid purchase amount")
    if paid_amount_value < 0:
        raise ValueError("Invalid purchase amount")

    resolved_identifier_type, resolved_identifier_value = _sanitize_privilege_identifier(
        identifier_type=identifier_type,
        nickname=nickname,
        steam_id=steam_id,
    )
    is_steam_identifier = resolved_identifier_type == PRIVILEGE_IDENTIFIER_STEAM
    final_password = "" if is_steam_identifier else _sanitize_password(password)
    current_password_safe = (
        ""
        if is_steam_identifier
        else (
            _sanitize_password(current_password, field_name="Current password")
            if str(current_password or "").strip()
            else ""
        )
    )
    access_mode = "ce" if is_steam_identifier else "a"
    duration_days_to_add = months_value * 30

    users_ini_bytes = _download_users_ini_bytes(users_ini_path)
    users_ini_text = users_ini_bytes.decode("latin-1")
    lines = users_ini_text.splitlines(keepends=True)
    found = _find_users_ini_entry(lines, resolved_identifier_type, resolved_identifier_value)
    newline = _choose_users_ini_newline(users_ini_bytes)

    if not found:
        if renewal_requested:
            raise ValueError(
                "Renewal requested but privilege was not found"
                if resolved_identifier_type == PRIVILEGE_IDENTIFIER_NICKNAME
                else "Renewal requested but privilege was not found for this STEAM_ID"
            )
        if paid_amount_value != int(target_tariff_price):
            raise ValueError("Invalid amount for this privilege purchase")

        users_ini_line = _build_users_ini_entry(
            identifier_value=resolved_identifier_value,
            password=final_password,
            privilege_flags=target_privilege_flags,
            access_mode=access_mode,
            duration_days=duration_days_to_add,
        )

        if lines and not lines[-1].endswith(("\n", "\r")):
            lines[-1] = f"{lines[-1]}{newline}"
        lines.append(f"{users_ini_line}{newline}")
        updated_text = "".join(lines)
        updated_bytes = updated_text.encode("latin-1")
        _upload_users_ini_bytes(updated_bytes, users_ini_bytes, users_ini_path)
        return {
            "mode": "created",
            "line": users_ini_line,
            "users_ini_path": users_ini_path,
            "before_days": 0,
            "after_days": duration_days_to_add,
            "privilege": _label_for_privilege_flags(target_privilege_flags),
            "flags": target_privilege_flags,
            "access": access_mode,
            "was_disabled": False,
            "password_changed": False,
            "calculated_amount": int(target_tariff_price),
            "credit_amount": 0,
            "previous_privilege": "",
            "target_privilege": _label_for_privilege_flags(target_privilege_flags),
            "identifier_type": resolved_identifier_type,
            "identifier_value": resolved_identifier_value,
            "nickname": resolved_identifier_value if not is_steam_identifier else "",
            "steam_id": resolved_identifier_value if is_steam_identifier else "",
        }

    if bool(found.get("is_permanent")):
        raise ValueError(
            "This identifier already has a permanent privilege. Use another nickname or STEAM_ID."
        )

    if not renewal_requested:
        raise ValueError(
            "Privilege already exists for this nickname"
            if resolved_identifier_type == PRIVILEGE_IDENTIFIER_NICKNAME
            else "Privilege already exists for this STEAM_ID"
        )

    if resolved_identifier_type == PRIVILEGE_IDENTIFIER_NICKNAME:
        if not current_password_safe:
            raise ValueError("Current password is required for renewal")

        if current_password_safe != found["password"]:
            raise ValueError("Current password is incorrect")

    existing_days = _users_ini_entry_days_value(found)
    existing_privilege_key = _normalize_privilege_key_from_flags(found["flags"])
    existing_privilege_label = _label_for_privilege_flags(found["flags"])
    target_privilege_rank = int(PRIVILEGE_TIER_BY_KEY.get(target_privilege_key, 0) or 0)
    existing_privilege_rank = int(PRIVILEGE_TIER_BY_KEY.get(existing_privilege_key, 0) or 0)
    existing_is_active = bool(existing_days > 0 and not found["is_disabled"] and existing_privilege_rank > 0)
    public_style_1_flow = bool(
        _is_public_style_1_server(server_id, server_name)
        and target_privilege_key in PUBLIC_STYLE_1_KEYS
        and existing_privilege_key in PUBLIC_STYLE_1_KEYS
    )

    mode = "renewed"
    resulting_flags = found["flags"]
    expected_amount = int(target_tariff_price)
    credit_amount = 0
    updated_days = existing_days + duration_days_to_add

    if existing_is_active and target_privilege_rank < existing_privilege_rank:
        raise ValueError(
            "Cannot downgrade active privilege. Buy for a new nickname or wait until it expires."
        )

    if existing_is_active and target_privilege_rank > existing_privilege_rank:
        mode = "upgraded"
        resulting_flags = target_privilege_flags
        updated_days = duration_days_to_add
        if public_style_1_flow:
            expected_amount, credit_amount = _calculate_public_style_1_upgrade_due_amount(
                target_tariff_price,
                existing_privilege_key,
                existing_days,
            )
        else:
            expected_amount = int(target_tariff_price)
            credit_amount = 0
    elif existing_is_active:
        mode = "renewed"
        resulting_flags = found["flags"]
        updated_days = existing_days + duration_days_to_add
        expected_amount = int(target_tariff_price)
    else:
        mode = "reactivated"
        resulting_flags = target_privilege_flags
        updated_days = duration_days_to_add
        expected_amount = int(target_tariff_price)

    if paid_amount_value != int(expected_amount):
        raise ValueError("Invalid amount for this privilege purchase")

    resulting_password = "" if is_steam_identifier else (final_password if bool(change_password) else found["password"])
    resulting_access = "ce" if is_steam_identifier else "a"
    resulting_line = _build_users_ini_entry(
        identifier_value=found["nickname"],
        password=resulting_password,
        privilege_flags=resulting_flags,
        access_mode=resulting_access,
        duration_days=updated_days,
    )
    line_newline = found["newline"] or newline
    lines[int(found["index"])] = f"{resulting_line}{line_newline}"

    updated_text = "".join(lines)
    updated_bytes = updated_text.encode("latin-1")
    _upload_users_ini_bytes(updated_bytes, users_ini_bytes, users_ini_path)
    return {
        "mode": mode,
        "line": resulting_line,
        "users_ini_path": users_ini_path,
        "before_days": existing_days,
        "after_days": updated_days,
        "privilege": _label_for_privilege_flags(resulting_flags),
        "flags": resulting_flags,
        "access": resulting_access,
        "was_disabled": bool(found["is_disabled"]),
        "password_changed": bool(change_password) if not is_steam_identifier else False,
        "calculated_amount": int(expected_amount),
        "credit_amount": int(credit_amount),
        "previous_privilege": existing_privilege_label,
        "target_privilege": _label_for_privilege_flags(resulting_flags),
        "identifier_type": resolved_identifier_type,
        "identifier_value": resolved_identifier_value,
        "nickname": found["nickname"] if not is_steam_identifier else "",
        "steam_id": normalize_steam_id(found["nickname"]) if is_steam_identifier else "",
    }


def _sanitize_sql_identifier(name):
    value = str(name or "").strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def _extract_pma_token(page_html):
    hidden_match = re.search(r'name="token"\s+value="([a-f0-9]{32})"', page_html, re.IGNORECASE)
    if hidden_match:
        return hidden_match.group(1)

    query_match = re.search(r"token=([a-f0-9]{32})", page_html, re.IGNORECASE)
    if query_match:
        return query_match.group(1)

    return ""


def _strip_html_text(raw_html):
    text = re.sub(r"<[^>]+>", " ", str(raw_html or ""))
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _open_phpmyadmin_session():
    if not PHPMYADMIN_BASE_URL:
        raise RuntimeError("PHPMYADMIN_BASE_URL is not set")
    if not PHPMYADMIN_LOGIN or not PHPMYADMIN_PASSWORD:
        raise RuntimeError("PHPMYADMIN credentials are not set")

    cookie_jar = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(cookie_jar))
    opener.addheaders = [
        ("User-Agent", "StrikeBot/1.0 (+bonus-purchase)"),
        ("Accept", "text/html,application/xhtml+xml"),
    ]

    login_url = f"{PHPMYADMIN_BASE_URL}/index.php"
    with opener.open(login_url, timeout=20) as response:
        login_page = response.read().decode("utf-8", errors="replace")

    token = _extract_pma_token(login_page)
    if not token:
        raise RuntimeError("phpMyAdmin login token not found")

    payload = urlencode(
        {
            "pma_username": PHPMYADMIN_LOGIN,
            "pma_password": PHPMYADMIN_PASSWORD,
            "server": PHPMYADMIN_SERVER,
            "target": "index.php",
            "lang": "en",
            "collation_connection": "utf8_general_ci",
            "token": token,
        }
    ).encode("utf-8")

    login_request = Request(
        login_url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with opener.open(login_request, timeout=20) as response:
        logged_in_page = response.read().decode("utf-8", errors="replace")

    if "Log out" not in logged_in_page and "old_usr=" not in logged_in_page:
        raise RuntimeError("phpMyAdmin login failed")

    session_token = _extract_pma_token(logged_in_page)
    if not session_token:
        raise RuntimeError("phpMyAdmin session token not found after login")

    return opener, session_token


def _run_phpmyadmin_sql_query(db_name, table_name, sql_query):
    opener, token = _open_phpmyadmin_session()
    sql_url = f"{PHPMYADMIN_BASE_URL}/sql.php"
    payload = urlencode(
        {
            "server": PHPMYADMIN_SERVER,
            "db": db_name,
            "table": table_name,
            "token": token,
            "sql_query": sql_query,
        }
    ).encode("utf-8")

    request = Request(
        sql_url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with opener.open(request, timeout=25) as response:
        page = response.read().decode("utf-8", errors="replace")

    error_messages = re.findall(r'<div class="error"[^>]*>(.*?)</div>', page, re.IGNORECASE | re.DOTALL)
    cleaned_errors = []
    for raw_error in error_messages:
        text = _strip_html_text(raw_error)
        if not text:
            continue
        if "Javascript must be enabled past this point!" in text:
            continue
        cleaned_errors.append(text)

    if cleaned_errors:
        raise RuntimeError(cleaned_errors[0])

    if "MySQL said" in page and 'class="error"' in page:
        raise RuntimeError("phpMyAdmin reported a MySQL error")

    return page


def _extract_first_table_cell(query_page_html):
    row_cells = _extract_first_table_row_cells(query_page_html)
    if not row_cells:
        return ""
    return row_cells[0]


def _extract_first_table_row_cells(query_page_html):
    table_match = re.search(
        r'<table id="table_results"[^>]*>.*?<tbody>(.*?)</tbody>',
        query_page_html,
        re.IGNORECASE | re.DOTALL,
    )
    if not table_match:
        return ""

    row_match = re.search(r"<tr[^>]*>(.*?)</tr>", table_match.group(1), re.IGNORECASE | re.DOTALL)
    if not row_match:
        return []

    raw_cells = re.findall(r"<td[^>]*>(.*?)</td>", row_match.group(1), re.IGNORECASE | re.DOTALL)
    if not raw_cells:
        return []

    cleaned_cells = []
    for raw_cell in raw_cells:
        text = _strip_html_text(raw_cell)
        if text:
            cleaned_cells.append(text)
    return cleaned_cells


def _bonus_database_by_server_id(server_id):
    try:
        server_port = int(server_id)
    except (TypeError, ValueError):
        return None

    if server_port in SERVERS.get("public", {}).get("servers", []):
        return BONUS_DB_PUBLIC

    return None


def fetch_bonus_account(server_id, steam_id):
    normalized_steam_id = normalize_steam_id(steam_id)
    if not is_valid_steam_id(normalized_steam_id):
        raise ValueError("Invalid STEAM_ID format")

    db_name = _bonus_database_by_server_id(server_id)
    if not db_name:
        raise ValueError("Bonus purchase is available only for Public servers")

    table_name = _sanitize_sql_identifier(BONUS_TABLE_NAME)
    row_id_column = _sanitize_sql_identifier(BONUS_ROW_ID_COLUMN)
    steam_column = _sanitize_sql_identifier(BONUS_STEAM_ID_COLUMN)
    name_column = _sanitize_sql_identifier(BONUS_NAME_COLUMN)
    bonus_column = _sanitize_sql_identifier(BONUS_VALUE_COLUMN)
    escaped_steam_id = normalized_steam_id.replace("'", "''")

    query = (
        f"SELECT CAST(`{row_id_column}` AS CHAR) AS row_id, "
        f"COALESCE(`{name_column}`, '') AS nickname, "
        f"COALESCE(`{bonus_column}`, 0) AS bonus_value "
        f"FROM `{table_name}` WHERE `{steam_column}` = '{escaped_steam_id}' "
        f"ORDER BY `{row_id_column}` DESC LIMIT 1"
    )
    page = _run_phpmyadmin_sql_query(db_name, table_name, query)
    row_cells = _extract_first_table_row_cells(page)
    if not row_cells:
        return None

    if len(row_cells) < 3:
        raise RuntimeError("Unexpected SQL response format")

    row_id_raw, nickname_raw, bonus_raw = row_cells[0], row_cells[1], row_cells[2]
    try:
        row_id = int(str(row_id_raw).strip())
    except (TypeError, ValueError):
        row_id = 0
    nickname = nickname_raw.strip()
    try:
        bonus_count = int(str(bonus_raw).strip())
    except (TypeError, ValueError):
        bonus_count = 0

    return {
        "steam_id": normalized_steam_id,
        "row_id": row_id,
        "nickname": nickname or "-",
        "bonus_count": bonus_count,
        "database": db_name,
    }


def apply_bonus_purchase(server_id, steam_id, bonus_amount):
    try:
        added_bonus = int(bonus_amount)
    except (TypeError, ValueError):
        raise ValueError("Invalid bonus amount")

    if added_bonus <= 0:
        raise ValueError("Bonus amount must be greater than zero")

    account_before = fetch_bonus_account(server_id, steam_id)
    if not account_before:
        return None

    db_name = account_before["database"]
    table_name = _sanitize_sql_identifier(BONUS_TABLE_NAME)
    row_id_column = _sanitize_sql_identifier(BONUS_ROW_ID_COLUMN)
    bonus_column = _sanitize_sql_identifier(BONUS_VALUE_COLUMN)
    try:
        row_id_value = int(account_before.get("row_id", 0) or 0)
    except (TypeError, ValueError):
        row_id_value = 0
    if row_id_value <= 0:
        raise RuntimeError("Invalid player row id")

    update_query = (
        f"UPDATE `{table_name}` "
        f"SET `{bonus_column}` = COALESCE(`{bonus_column}`, 0) + {added_bonus} "
        f"WHERE `{row_id_column}` = {row_id_value} LIMIT 1"
    )
    _run_phpmyadmin_sql_query(db_name, table_name, update_query)

    account_after = fetch_bonus_account(server_id, steam_id)
    if not account_after:
        raise RuntimeError("Failed to load account after bonus update")

    return {
        "steam_id": account_before["steam_id"],
        "nickname": account_after["nickname"],
        "before": int(account_before["bonus_count"]),
        "after": int(account_after["bonus_count"]),
        "added": added_bonus,
        "database": db_name,
    }


def telegram_api_json(method, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        f"https://api.telegram.org/bot{TOKEN}/{method}",
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
        response_context = urlopen(request, timeout=20, context=ssl_context)
    else:
        response_context = urlopen(request, timeout=20)

    with response_context as response:
        return json.loads(response.read().decode("utf-8"))


def telegram_send_message(chat_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return telegram_api_json("sendMessage", payload)


def telegram_send_photo(chat_id, caption, photo_bytes, filename, content_type, reply_markup=None):
    boundary = f"----StrikeBotBoundary{uuid.uuid4().hex}"
    body = bytearray()

    def add_text_field(name, value):
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        body.extend(str(value).encode("utf-8"))
        body.extend(b"\r\n")

    add_text_field("chat_id", chat_id)
    add_text_field("caption", caption)
    add_text_field("parse_mode", "HTML")
    if reply_markup is not None:
        add_text_field("reply_markup", json.dumps(reply_markup, ensure_ascii=False))

    body.extend(f"--{boundary}\r\n".encode("utf-8"))
    body.extend(
        f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'.encode("utf-8")
    )
    body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
    body.extend(photo_bytes)
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))

    request = Request(
        f"https://api.telegram.org/bot{TOKEN}/sendPhoto",
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )

    ssl_context = None
    if certifi is not None:
        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
        except Exception:
            ssl_context = None

    if ssl_context is not None:
        response_context = urlopen(request, timeout=25, context=ssl_context)
    else:
        response_context = urlopen(request, timeout=25)

    with response_context as response:
        return json.loads(response.read().decode("utf-8"))


def build_user_mention(user_id, username="", first_name="", last_name=""):
    normalized_username = str(username or "").strip().lstrip("@")
    if normalized_username:
        return f"@{html.escape(normalized_username)}"

    display_name = f"{str(first_name or '').strip()} {str(last_name or '').strip()}".strip()
    if not display_name:
        display_name = f"ID {user_id}"

    return f'<a href="tg://user?id={int(user_id)}">{html.escape(display_name)}</a>'


def create_purchase_record(
    *,
    user_id,
    username,
    first_name,
    last_name,
    server_id,
    privilege,
    server_name,
    duration,
    duration_months,
    nickname,
    password,
    language,
    amount,
    product_type="privilege",
    identifier_type=PRIVILEGE_IDENTIFIER_NICKNAME,
    steam_id="",
    bonus_added=0,
    bonus_before=0,
    bonus_after=0,
):
    safe_password = str(password or "").strip()
    return {
        "id": uuid.uuid4().hex[:14],
        "created_at": int(time.time()),
        "status": "active",
        "user_id": int(user_id),
        "username": str(username or "").strip().lstrip("@"),
        "first_name": str(first_name or "").strip(),
        "last_name": str(last_name or "").strip(),
        "server_id": str(server_id or "").strip(),
        "privilege": str(privilege),
        "server": str(server_name),
        "duration": str(duration),
        "duration_months": int(duration_months),
        "nickname": str(nickname),
        "password": "[REDACTED]" if safe_password else "",
        "password_set": bool(safe_password),
        "language": str(language),
        "amount": int(amount),
        "product_type": str(product_type or "privilege"),
        "identifier_type": normalize_privilege_identifier_type(identifier_type),
        "steam_id": str(steam_id or ""),
        "bonus_added": int(bonus_added or 0),
        "bonus_before": int(bonus_before or 0),
        "bonus_after": int(bonus_after or 0),
        "report_message_id": None,
    }


def save_purchase_record(record):
    with REPORTS_LOCK:
        REPORTS_STORE["purchases"].append(record)
        _save_reports_store_locked()


def set_purchase_report_message_id(purchase_id, message_id):
    with REPORTS_LOCK:
        for record in REPORTS_STORE["purchases"]:
            if str(record.get("id")) == str(purchase_id):
                record["report_message_id"] = int(message_id)
                _save_reports_store_locked()
                return True
    return False


def cancel_purchase_record(purchase_id, canceled_by_user_id):
    timestamp = int(time.time())
    with REPORTS_LOCK:
        for record in REPORTS_STORE["purchases"]:
            if str(record.get("id")) != str(purchase_id):
                continue
            if record.get("status") != "active":
                return None
            record["status"] = "canceled"
            record["canceled_at"] = timestamp
            record["canceled_by"] = int(canceled_by_user_id)
            _save_reports_store_locked()
            return dict(record)
    return None


def get_active_purchases_between(start_dt_local, end_dt_local):
    start_ts = int(start_dt_local.astimezone(datetime.timezone.utc).timestamp())
    end_ts = int(end_dt_local.astimezone(datetime.timezone.utc).timestamp())

    with REPORTS_LOCK:
        purchases = [
            dict(purchase)
            for purchase in REPORTS_STORE["purchases"]
            if purchase.get("status") == "active"
            and start_ts <= int(purchase.get("created_at", 0)) < end_ts
        ]

    return purchases


def should_send_periodic_report(report_type, key):
    with REPORTS_LOCK:
        last_value = str(REPORTS_STORE.get("last_reports", {}).get(report_type, "")).strip()
    return last_value != key


def mark_periodic_report_sent(report_type, key):
    with REPORTS_LOCK:
        REPORTS_STORE["last_reports"][report_type] = key
        _save_reports_store_locked()


def _cleanup_payment_attempts_locked(now_ts):
    attempts = REPORTS_STORE.setdefault("payment_attempts", {})
    if not isinstance(attempts, dict):
        REPORTS_STORE["payment_attempts"] = {}
        return

    stale_keys = []
    for key, value in attempts.items():
        if not isinstance(value, dict):
            stale_keys.append(key)
            continue
        expires_at = int(value.get("expires_at", 0) or 0)
        if expires_at > 0 and now_ts >= expires_at:
            stale_keys.append(key)
    for key in stale_keys:
        attempts.pop(key, None)


def _build_payment_attempt_key(
    *,
    user_id,
    product_type,
    server_id,
    amount,
    privilege="",
    duration_months=0,
    identifier_type=PRIVILEGE_IDENTIFIER_NICKNAME,
    nickname="",
    steam_id="",
    bonus_amount=0,
    payment_session_id="",
):
    identifier_mode = normalize_privilege_identifier_type(identifier_type)
    if identifier_mode == PRIVILEGE_IDENTIFIER_STEAM:
        identifier_value = normalize_steam_id(steam_id)
    else:
        identifier_value = str(nickname or "").strip().casefold()

    payload = {
        "user_id": int(user_id),
        "product_type": str(product_type or "").strip().lower(),
        "server_id": str(server_id or "").strip(),
        "amount": int(amount or 0),
        "privilege": str(privilege or "").strip().lower(),
        "duration_months": int(duration_months or 0),
        "identifier_type": identifier_mode,
        "identifier_value": identifier_value,
        "bonus_amount": int(bonus_amount or 0),
        "payment_session_id": str(payment_session_id or "").strip(),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"pay:{digest}"


def get_payment_attempt_status(attempt_key):
    now_ts = int(time.time())
    with REPORTS_LOCK:
        _cleanup_payment_attempts_locked(now_ts)
        attempts = REPORTS_STORE.setdefault("payment_attempts", {})
        state = attempts.get(str(attempt_key), {})

    failures = int(state.get("failures", 0) or 0)
    remaining = max(PAYMENT_MAX_SCREENSHOT_ATTEMPTS - failures, 0)
    blocked = failures >= PAYMENT_MAX_SCREENSHOT_ATTEMPTS
    return {
        "failures": failures,
        "remaining": remaining,
        "blocked": blocked,
    }


def register_payment_attempt_failure(attempt_key, reason=""):
    now_ts = int(time.time())
    key = str(attempt_key or "").strip()
    if not key:
        return {
            "failures": 0,
            "remaining": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
            "blocked": False,
        }

    with REPORTS_LOCK:
        _cleanup_payment_attempts_locked(now_ts)
        attempts = REPORTS_STORE.setdefault("payment_attempts", {})
        current = attempts.get(key)
        if not isinstance(current, dict):
            current = {}

        first_failure_at = int(current.get("first_failure_at", 0) or 0)
        failures = int(current.get("failures", 0) or 0)
        if first_failure_at <= 0 or (now_ts - first_failure_at) >= PAYMENT_ATTEMPT_WINDOW_SECONDS:
            first_failure_at = now_ts
            failures = 0

        failures += 1
        record = {
            "first_failure_at": first_failure_at,
            "updated_at": now_ts,
            "expires_at": now_ts + PAYMENT_ATTEMPT_WINDOW_SECONDS,
            "failures": int(failures),
            "last_reason": str(reason or "").strip()[:300],
        }
        attempts[key] = record
        _save_reports_store_locked()

    remaining = max(PAYMENT_MAX_SCREENSHOT_ATTEMPTS - failures, 0)
    blocked = failures >= PAYMENT_MAX_SCREENSHOT_ATTEMPTS
    return {
        "failures": failures,
        "remaining": remaining,
        "blocked": blocked,
    }


def reset_payment_attempts(attempt_key):
    key = str(attempt_key or "").strip()
    if not key:
        return

    with REPORTS_LOCK:
        _cleanup_payment_attempts_locked(int(time.time()))
        attempts = REPORTS_STORE.setdefault("payment_attempts", {})
        if key in attempts:
            attempts.pop(key, None)
            _save_reports_store_locked()


def _build_payment_violation_key(*, user_id, payment_session_id):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        safe_user_id = 0

    session_id = str(payment_session_id or "").strip()
    if safe_user_id <= 0 or not session_id:
        return ""
    return f"{safe_user_id}:{session_id}"


def _cleanup_payment_user_violations_locked(now_ts):
    violations = REPORTS_STORE.setdefault("payment_user_violations", {})
    if not isinstance(violations, dict):
        REPORTS_STORE["payment_user_violations"] = {}
        return

    stale_keys = []
    for key, value in violations.items():
        if not isinstance(value, dict):
            stale_keys.append(key)
            continue

        payment_session_id = str(value.get("payment_session_id", "")).strip()
        blocked_until = int(value.get("blocked_until", 0) or 0)
        session_expires_at = int(value.get("session_expires_at", 0) or 0)
        failures = int(value.get("failures", 0) or 0)

        if not payment_session_id:
            stale_keys.append(key)
            continue

        # Keep active ban until timer ends, then reset attempts for this session.
        if blocked_until > 0:
            if now_ts < blocked_until:
                continue
            stale_keys.append(key)
            continue

        if failures <= 0:
            stale_keys.append(key)
            continue

        # Non-banned counters are valid only inside the current payment session.
        if session_expires_at > 0:
            if now_ts >= session_expires_at:
                stale_keys.append(key)
                continue
        else:
            updated_at = int(value.get("updated_at", 0) or 0)
            if updated_at <= 0 or (now_ts - updated_at) >= PAYMENT_UPLOAD_SESSION_SECONDS:
                stale_keys.append(key)
                continue

    for key in stale_keys:
        violations.pop(key, None)


def get_user_payment_violation_status(user_id, *, payment_session_id=""):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        safe_user_id = 0

    session_id = str(payment_session_id or "").strip()
    if safe_user_id <= 0 or not session_id:
        return {
            "banned": False,
            "blocked_until": 0,
            "seconds_remaining": 0,
            "reason": "",
            "failures": 0,
            "max_attempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
            "ban_seconds": PAYMENT_USER_BAN_SECONDS,
        }

    now_ts = int(time.time())
    key = _build_payment_violation_key(user_id=safe_user_id, payment_session_id=session_id)
    with REPORTS_LOCK:
        _cleanup_payment_user_violations_locked(now_ts)
        violations = REPORTS_STORE.setdefault("payment_user_violations", {})
        record = violations.get(key, {})

    if not isinstance(record, dict):
        record = {}

    blocked_until = int(record.get("blocked_until", 0) or 0)
    is_banned = blocked_until > now_ts
    seconds_remaining = max(blocked_until - now_ts, 0) if is_banned else 0
    failures = int(record.get("failures", 0) or 0) if not is_banned else PAYMENT_MAX_SCREENSHOT_ATTEMPTS

    return {
        "banned": is_banned,
        "blocked_until": blocked_until if is_banned else 0,
        "seconds_remaining": seconds_remaining,
        "reason": str(record.get("reason", "")).strip() if is_banned else "",
        "failures": failures,
        "max_attempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
        "ban_seconds": PAYMENT_USER_BAN_SECONDS,
    }


def register_user_payment_violation(
    user_id,
    reason="",
    *,
    payment_session_id="",
    session_started_at=0,
    session_expires_at=0,
):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        safe_user_id = 0

    session_id = str(payment_session_id or "").strip()
    if safe_user_id <= 0 or not session_id:
        return {
            "banned": False,
            "blocked_until": 0,
            "seconds_remaining": 0,
            "reason": "",
            "failures": 0,
            "max_attempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
            "ban_seconds": PAYMENT_USER_BAN_SECONDS,
        }

    now_ts = int(time.time())
    key = _build_payment_violation_key(user_id=safe_user_id, payment_session_id=session_id)
    with REPORTS_LOCK:
        _cleanup_payment_user_violations_locked(now_ts)
        violations = REPORTS_STORE.setdefault("payment_user_violations", {})
        record = violations.get(key)
        if not isinstance(record, dict):
            record = {}

        blocked_until = int(record.get("blocked_until", 0) or 0)
        if blocked_until > now_ts:
            seconds_remaining = blocked_until - now_ts
            return {
                "banned": True,
                "blocked_until": blocked_until,
                "seconds_remaining": seconds_remaining,
                "reason": str(record.get("reason", "")).strip(),
                "failures": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                "max_attempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                "ban_seconds": PAYMENT_USER_BAN_SECONDS,
            }

        stored_session_expires_at = int(record.get("session_expires_at", 0) or 0)
        safe_session_expires_at = int(session_expires_at or 0)
        safe_session_started_at = int(session_started_at or 0)
        if safe_session_expires_at <= 0:
            safe_session_expires_at = stored_session_expires_at

        # If session already expired, start counters from zero for a new session.
        if safe_session_expires_at > 0 and now_ts >= safe_session_expires_at:
            record = {}

        first_failure_at = int(record.get("first_failure_at", 0) or 0)
        failures = int(record.get("failures", 0) or 0)
        if first_failure_at <= 0:
            first_failure_at = now_ts
            failures = 0

        failures += 1
        is_banned = failures >= PAYMENT_MAX_SCREENSHOT_ATTEMPTS
        blocked_until = now_ts + PAYMENT_USER_BAN_SECONDS if is_banned else 0
        violation_record = {
            "first_failure_at": first_failure_at,
            "updated_at": now_ts,
            "failures": failures,
            "blocked_until": blocked_until,
            "payment_session_id": session_id,
            "session_started_at": safe_session_started_at,
            "session_expires_at": safe_session_expires_at,
            "reason": str(reason or "").strip()[:300],
        }
        violations[key] = violation_record
        _save_reports_store_locked()

    return {
        "banned": is_banned,
        "blocked_until": blocked_until,
        "seconds_remaining": max(blocked_until - now_ts, 0) if is_banned else 0,
        "reason": str(reason or "").strip(),
        "failures": failures,
        "max_attempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
        "ban_seconds": PAYMENT_USER_BAN_SECONDS,
    }


def reset_user_payment_violations(user_id, *, payment_session_id=""):
    try:
        safe_user_id = int(user_id)
    except (TypeError, ValueError):
        safe_user_id = 0
    session_id = str(payment_session_id or "").strip()
    if safe_user_id <= 0 or not session_id:
        return

    key = _build_payment_violation_key(user_id=safe_user_id, payment_session_id=session_id)
    with REPORTS_LOCK:
        _cleanup_payment_user_violations_locked(int(time.time()))
        violations = REPORTS_STORE.setdefault("payment_user_violations", {})
        if key in violations:
            violations.pop(key, None)
            _save_reports_store_locked()


def localize_payment_reason(reason, language="ru"):
    text = str(reason or "").strip()
    normalized_language = str(language or "ru").strip().lower()
    if not text or normalized_language != "uz":
        return text

    translated = text
    replacements = [
        (
            "Проверка оплаты недоступна: не настроен OPENAI_API_KEY",
            "To'lov tekshiruvi mavjud emas: OPENAI_API_KEY sozlanmagan",
        ),
        (
            "Проверка оплаты недоступна: укажите PAYMENT_RECIPIENT_CARD_LAST4 "
            "или PAYMENT_RECIPIENT_NAMES/PAYMENT_RECIPIENT_CARD_HINTS",
            "To'lov tekshiruvi mavjud emas: PAYMENT_RECIPIENT_CARD_LAST4 yoki "
            "PAYMENT_RECIPIENT_NAMES/PAYMENT_RECIPIENT_CARD_HINTS sozlang",
        ),
        (
            "Проверка оплаты недоступна: установите зависимость telethon",
            "To'lov tekshiruvi mavjud emas: telethon kutubxonasini o'rnating",
        ),
        (
            "Проверка оплаты недоступна: не настроены TELEGRAM_APP_API_ID/TELEGRAM_APP_API_HASH",
            "To'lov tekshiruvi mavjud emas: TELEGRAM_APP_API_ID/TELEGRAM_APP_API_HASH sozlanmagan",
        ),
        (
            "Проверка оплаты завершилась ошибкой:",
            "To'lov tekshiruvi xatolik bilan yakunlandi:",
        ),
        (
            "Оплата уже привязана к другой покупке и не может быть использована повторно",
            "To'lov allaqachon boshqa xaridga biriktirilgan va qayta ishlatilmaydi",
        ),
        (
            "нет данных для сравнения баланса @HUMOcardbot",
            "@HUMOcardbot balansini solishtirish uchun ma'lumot topilmadi",
        ),
        ("баланс карты не изменился", "karta balansi o'zgarmadi"),
        ("разница баланса (", "balans farqi ("),
        (") не равна сумме платежа (", ") to'lov summasiga teng emas ("),
        ("на скриншоте не найден перевод", "skrinshotda o'tkazma topilmadi"),
        (
            "не удалось определить сумму перевода",
            "o'tkazma summasini aniqlab bo'lmadi",
        ),
        (
            "получатель на скриншоте не совпадает с вашей картой",
            "skrinshotdagi qabul qiluvchi sizning kartangizga mos kelmadi",
        ),
        (
            "сумма на скриншоте (",
            "skrinshotdagi summa (",
        ),
        (
            ") не совпадает с ожидаемой (",
            ") kutilgan summa (",
        ),
        ("Пополнение не подтверждено", "Balans to'ldirish tasdiqlanmadi"),
        ("Оплата не подтверждена", "To'lov tasdiqlanmadi"),
    ]
    for source, target in replacements:
        translated = translated.replace(source, target)

    return translated


def is_technical_payment_verification_error(reason):
    text = str(reason or "").strip().lower()
    return (
        text.startswith("проверка оплаты недоступна")
        or text.startswith("проверка оплаты завершилась ошибкой")
        or text.startswith("to'lov tekshiruvi mavjud emas")
        or text.startswith("to'lov tekshiruvi xatolik bilan yakunlandi")
    )


def format_payment_ban_reason(*, reason, seconds_remaining, language="ru"):
    normalized_language = str(language or "ru").strip().lower()
    localized_reason = localize_payment_reason(reason, normalized_language)
    base_reason = str(localized_reason or "").strip() or (
        "Noto'g'ri skrinshotlar juda ko'p yuborildi"
        if normalized_language == "uz"
        else "Слишком много неподходящих скриншотов"
    )
    safe_seconds = max(int(seconds_remaining or 0), 0)
    minutes = max((safe_seconds + 59) // 60, 1)
    if normalized_language == "uz":
        return (
            f"To'lov tekshiruvi vaqtincha bloklangan: {base_reason}. "
            f"Qayta urinish vaqti taxminan {minutes} daqiqa."
        )
    return (
        f"Проверка оплаты временно заблокирована: {base_reason}. "
        f"Повторить можно примерно через {minutes} минут."
    )


START_TEXT = """<b>👋 Assalomu alaykum! Strike.Uz ga xush kelibsiz!</b>

<b>Mavjud buyruqlar:</b>
<b>/info</b> — Strike.Uz loyihasi haqida ma’lumot
<b>/server</b> — Serverlar ro‘yxati
<b>/players</b> — Serverlardagi o‘yinchilar ro‘yxati
<b>/vip</b> — VIP haqida ma’lumot
<b>/miniapp</b> — Strike.Uz Mini App

<b>ℹ️ Qo‘shimcha ma’lumot uchun: @MccallStrike</b>

<b>📣 Telegram kanalimizga obuna bo‘ling:</b>
@STRIKEUZCHANNEL

<b>💬 Telegram guruhlarimizda faol bo‘ling:</b>
@STRIKEUZCOMMUNITY
@STRIKECW
@STRIKEUZREPORTS

────────────────────

<b>👋 Добро пожаловать в Strike.Uz!</b>

<b>Доступные команды:</b>
<b>/info</b> — Информация о проекте Strike.Uz
<b>/server</b> — Список серверов
<b>/players</b> — Список игроков на серверах
<b>/vip</b> — Информация о VIP
<b>/miniapp</b> — Strike.Uz Mini App

<b>ℹ️ Для дополнительной информации: @MccallStrike</b>

<b>📣 Подпишитесь на наш Telegram-канал:</b>
@STRIKEUZCHANNEL

<b>💬 Будьте активны в наших Telegram-группах:</b>
@STRIKEUZCOMMUNITY
@STRIKECW
@STRIKEUZREPORTS
"""

INFO_TEXT = """<b>🇺🇿 Strike.Uz ga hush kelibsiz! 👋</b>

Strike.Uz — bu O‘zbekistondagi eng sifatli va qiziqarli Counter-Strike 1.6 serverlari.
Agar siz kuchli o‘yinchilar bilan va qiziqarli serverlarda o‘ynashni xohlasangiz,
hoziroq Strike.Uz saytimizdan o‘yinni yuklab oling!

<b>🌐 Server IP manzillari:</b>
`/server` buyrug‘i orqali yoki Strike.Uz saytimizda mavjud.

<b>🔥 Biz sizni serverlarimizda kutib qolamiz!</b>

────────────────────

<b>🇷🇺 Добро пожаловать в Strike.Uz! 👋</b>

Strike.Uz — это качественные и интересные сервера Counter-Strike 1.6 в Узбекистане.
Хотите играть с сильнейшими игроками страны и на интересных серверах?
Тогда прямо сейчас скачивайте игру с нашего сайта!

<b>🌐 IP адреса серверов:</b>
Доступны по команде `/server` или на сайте Strike.Uz.

<b>🔥 Мы ждём тебя на наших серверах!</b>
"""

VIP_TEXT = """<b>🇺🇿 VIP haqida ma’lumot</b>

VIP xizmatlari haqida batafsil ma’lumot olish uchun:
👉 @MccallStrike

────────────────────

<b>🇷🇺 Информация о VIP</b>

Полную информацию о VIP-услугах вы можете получить у:
👉 @MccallStrike
"""

def build_main_keyboard():
    _url = get_web_app_url()
    app_button = (
        KeyboardButton(
            text="📱 App",
            web_app=WebAppInfo(url=_url),
        )
        if _url
        else KeyboardButton("📱 App")
    )

    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🌐 Servers"), KeyboardButton("👥 Players")],
            [app_button],
        ],
        resize_keyboard=True,
    )


async def configure_chat_menu_button(application):
    _url = get_web_app_url()
    if not _url:
        return

    try:
        await application.bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="Mini App",
                web_app=WebAppInfo(url=_url),
            )
        )
    except Exception as e:
        print(
            f"[BOT WARNING] Failed to set chat menu button: {_redact_sensitive_text(e)}",
            file=sys.stderr,
        )


async def ensure_chat_menu_button_for_chat(bot, chat_id):
    _url = get_web_app_url()
    if not _url:
        return

    try:
        await bot.set_chat_menu_button(
            chat_id=chat_id,
            menu_button=MenuButtonWebApp(
                text="Mini App",
                web_app=WebAppInfo(url=_url),
            ),
        )
    except Exception as error:
        print(
            (
                f"[BOT WARNING] Failed to set chat menu button for chat {chat_id}: "
                f"{_redact_sensitive_text(error)}"
            ),
            file=sys.stderr,
        )


def get_unique_ports():
    ports = []
    seen = set()
    for category in SERVERS.values():
        for port in category["servers"]:
            if port not in seen:
                seen.add(port)
                ports.append(port)
    return ports


def _strip_html(text):
    value = re.sub(r"<[^>]+>", " ", str(text or ""), flags=re.DOTALL)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _parse_monitoring_servers_html(page_html):
    parsed = {}
    for row_match in MONITORING_TABLE_ROW_PATTERN.finditer(str(page_html or "")):
        row_html = row_match.group(1)
        address_match = MONITORING_ADDRESS_PATTERN.search(row_html)
        if not address_match:
            continue

        try:
            port = int(address_match.group(2))
        except (TypeError, ValueError):
            continue

        cells = MONITORING_CELL_PATTERN.findall(row_html)
        if len(cells) < 4:
            continue

        name = _strip_html(cells[0]) or f"Server {port}"
        status_text = _strip_html(cells[1]).lower()

        players = 0
        max_players = 0
        players_match = MONITORING_PLAYERS_PATTERN.search(status_text)
        if players_match:
            try:
                players = int(players_match.group(1))
                max_players = int(players_match.group(2))
            except (TypeError, ValueError):
                players = 0
                max_players = 0

        map_cell = cells[2]
        map_match = MONITORING_MAP_BR_PATTERN.search(map_cell)
        if map_match:
            map_name = _strip_html(map_match.group(1))
        else:
            map_text = _strip_html(map_cell)
            map_name = map_text.split()[-1] if map_text else "unknown"
        if not map_name:
            map_name = "unknown"

        parsed[port] = {
            "name": name,
            "map": map_name,
            "players": players,
            "max": max_players,
        }

    return parsed


def _get_monitoring_servers_snapshot(force=False):
    global MONITORING_LAST_ERROR_LOG_TS
    now = time.time()
    with MONITORING_CACHE_LOCK:
        cached_servers = dict(MONITORING_CACHE.get("servers", {}))
        cached_timestamp = float(MONITORING_CACHE.get("timestamp", 0.0) or 0.0)
        if (
            not force
            and cached_servers
            and (now - cached_timestamp) < MONITORING_CACHE_TTL_SECONDS
        ):
            return cached_servers

        try:
            request = Request(
                MONITORING_URL,
                headers={
                    "User-Agent": "StrikeBot/1.0 (+monitoring-fallback)",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            ssl_context = None
            if certifi is not None:
                try:
                    ssl_context = ssl.create_default_context(cafile=certifi.where())
                except Exception:
                    ssl_context = None

            if ssl_context is not None:
                response_context = urlopen(request, timeout=MONITORING_TIMEOUT_SECONDS, context=ssl_context)
            else:
                response_context = urlopen(request, timeout=MONITORING_TIMEOUT_SECONDS)
            with response_context as response:
                page_html = response.read().decode("utf-8", errors="ignore")
            parsed = _parse_monitoring_servers_html(page_html)
            if parsed:
                MONITORING_CACHE["servers"] = parsed
                MONITORING_CACHE["timestamp"] = time.time()
                return dict(parsed)
        except Exception as error:
            # Throttle repeated monitoring errors and avoid retry storms in parallel requests.
            MONITORING_CACHE["timestamp"] = time.time()
            now_ts = time.time()
            if (now_ts - float(MONITORING_LAST_ERROR_LOG_TS or 0.0)) >= MONITORING_ERROR_LOG_COOLDOWN_SECONDS:
                MONITORING_LAST_ERROR_LOG_TS = now_ts
                print(f"[MONITORING ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)

        return cached_servers


def get_servers_sync():
    ports = get_unique_ports()

    with ThreadPoolExecutor(max_workers=min(16, len(ports) or 1)) as executor:
        infos = list(executor.map(get_server_info, ports))

    servers = []
    for port, info in zip(ports, infos):
        is_online = bool(info["max"] or info["players"])
        servers.append({
            "id": str(port),
            "port": port,
            "name": info["name"],
            "players": info["players"],
            "max": info["max"],
            "maxPlayers": info["max"],
            "map": info["map"],
            "ip": f"{BASE_IP}:{port}",
            "status": "online" if is_online else "offline",
        })

    return servers


async def get_servers():
    return await asyncio.to_thread(get_servers_sync)

def percent(a, b):
    return int((a / b) * 100) if b else 0

import a2s

# Prevent silent fallback when a wrong "a2s" package is installed.
if not hasattr(a2s, "info") or not hasattr(a2s, "players"):
    raise RuntimeError("Invalid a2s package installed. Install `python-a2s`.")


def _a2s_is_temporarily_disabled():
    if A2S_COOLDOWN_SECONDS <= 0:
        return False
    with A2S_STATE_LOCK:
        return time.time() < A2S_DISABLED_UNTIL


def _mark_a2s_failure():
    if A2S_COOLDOWN_SECONDS <= 0:
        return True

    with A2S_STATE_LOCK:
        global A2S_DISABLED_UNTIL
        now = time.time()
        was_disabled = now < A2S_DISABLED_UNTIL
        A2S_DISABLED_UNTIL = max(A2S_DISABLED_UNTIL, now + A2S_COOLDOWN_SECONDS)
        return not was_disabled


def _mark_a2s_success():
    with A2S_STATE_LOCK:
        global A2S_DISABLED_UNTIL
        A2S_DISABLED_UNTIL = 0.0


def get_server_info(port):
    if not _a2s_is_temporarily_disabled():
        try:
            info = a2s.info((BASE_IP, port), timeout=A2S_TIMEOUT)
            _mark_a2s_success()

            return {
                "name": info.server_name,
                "map": info.map_name,
                "players": info.player_count,
                "max": info.max_players,
            }

        except Exception as error:
            should_log = _mark_a2s_failure()
            if should_log:
                print(
                    (
                        f"[INFO ERROR] {BASE_IP}:{port} -> {error}; "
                        f"using monitoring fallback for {int(A2S_COOLDOWN_SECONDS)}s"
                    ),
                    file=sys.stderr,
                )

    snapshot = _get_monitoring_servers_snapshot()
    fallback = snapshot.get(port)
    if fallback:
        return fallback

    return {
        "name": f"Server {port}",
        "map": "unknown",
        "players": 0,
        "max": 0,
    }

def main_inline_keyboard():
    _url = get_web_app_url()
    app_button = (
        InlineKeyboardButton("📱 App", web_app=WebAppInfo(url=_url))
        if _url
        else InlineKeyboardButton("📱 App", callback_data="menu_app_unavailable")
    )

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 Players", callback_data="menu_players"),
            InlineKeyboardButton("🌐 Servers", callback_data="menu_servers"),
        ],
        [app_button],
    ])


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎮 <b>Strike.Uz меню</b>",
        reply_markup=main_inline_keyboard(),
        parse_mode="HTML"
    )


async def miniapp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _url = get_web_app_url()
    if not _url:
        await update.message.reply_text(
            "❌ Mini App hali sozlanmagan. WEB_APP_URL o'zgaruvchisini kiriting.\n❌ Mini App пока не настроен. Укажите WEB_APP_URL.",
            parse_mode="HTML",
        )
        return

    await update.message.reply_text(
        "🌐 <b>Strike.Uz Mini App</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Open Mini App", web_app=WebAppInfo(url=_url))]]
        ),
    )


def get_players(port):
    try:
        players = a2s.players((BASE_IP, port), timeout=A2S_TIMEOUT)
        result = []

        for idx, p in enumerate(players, start=1):
            result.append({
                "id": f"{port}-{idx}",
                "name": p.name.strip() if p.name else "unnamed",
                "nickname": p.name.strip() if p.name else "unnamed",
                "kills": int(p.score),
                "deaths": None,
                "time": int(p.duration // 60),
            })

        result.sort(key=lambda x: x["kills"], reverse=True)
        return result

    except Exception as e:
        print(
            f"[PLAYERS ERROR] {BASE_IP}:{port} -> {_redact_sensitive_text(e)}",
            file=sys.stderr,
        )
        return []


async def get_players_async(port):
    return await asyncio.to_thread(get_players, port)


def number_to_emoji(n: int) -> str:
    digits = {
        "0": "0️⃣",
        "1": "1️⃣",
        "2": "2️⃣",
        "3": "3️⃣",
        "4": "4️⃣",
        "5": "5️⃣",
        "6": "6️⃣",
        "7": "7️⃣",
        "8": "8️⃣",
        "9": "9️⃣",
    }
    return "".join(digits[d] for d in str(n))



async def players_server_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    port = int(query.data.split(":")[1])

    server_info = get_server_info(port)
    server_name = html.escape(server_info["name"])

    players = await get_players_async(port)
    players.sort(key=lambda x: x["kills"], reverse=True)

    if not players:
        await query.edit_message_text(
            "❌ Игроки не найдены или сервер не отвечает"
        )
        return

    text = f"👥 <b>Игроки на сервере {server_name}</b>\n\n"

    for i, p in enumerate(players, start=1):
        num = number_to_emoji(i)
        player_name = html.escape(p["name"])

        text += (
            f"{num} <b>{player_name}</b>: "
            f"<i>🎯Kills: {p['kills']} | ⏱Time: {p['time']} min</i>\n"
        )

    await query.edit_message_text(
        text,
        parse_mode="HTML"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.chat.type == "private":
        await ensure_chat_menu_button_for_chat(context.bot, update.message.chat.id)
        await update.message.reply_text(
            START_TEXT,
            parse_mode="HTML",
            reply_markup=build_main_keyboard()
        )
    else:
        await update.message.reply_text(
            "👋 Strike.Uz bot\n\n"
            "Нажмите кнопки ниже или используйте команды:\n"
            "/players — Игроки\n"
            "/server — Серверы\n"
            "/info — Информация\n"
            "/vip — VIP",
            parse_mode="HTML",
            reply_markup=main_inline_keyboard() 
        )

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        INFO_TEXT,
        parse_mode="HTML"
    )

async def vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        VIP_TEXT,
        parse_mode="HTML"
    )

async def server(update: Update, context: ContextTypes.DEFAULT_TYPE):
    response_message = update.effective_message
    servers = await get_servers()

    if not servers:
        await response_message.reply_text("Не удалось получить сервера.")
        return

    total_players = sum(s["players"] for s in servers)
    total_max = sum(s["max"] for s in servers)

    text = f"<b>📊 Statistics:</b> {total_players}/{total_max} [{percent(total_players, total_max)}%]\n\n"

    for s in servers:
        text += (
            f"⚡<b>️Server:</b> {s['name']}\n"
            f"🌐<b>IP:</b> {s['ip']}\n"
            f"📍<b>Map:</b> {s['map']}\n"
            f"👥<b>Players:</b> {s['players']} из {s['max']} [{percent(s['players'], s['max'])}%]\n\n\n"
        )

    await response_message.reply_text(text, parse_mode="HTML")

async def players_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await players(update, context)


async def players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    response_message = update.effective_message
    keyboard = []

    for key, category in SERVERS.items():
        keyboard.append([
            InlineKeyboardButton(
                category["title"],
                callback_data=f"players_category:{key}"
            )
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await response_message.reply_text(
        "👥 <b>Выберите тип сервера:</b>",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )


async def players_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    category_key = query.data.split(":")[1]
    category = SERVERS.get(category_key)

    if not category:
        await query.edit_message_text("❌ Категория не найдена")
        return

    keyboard = []

    for port in category["servers"]:
        info = get_server_info(port)

        title = f"🎮 {info['name']} ({info['players']}/{info['max']})"

        keyboard.append([
            InlineKeyboardButton(
                title,
                callback_data=f"players_server:{port}"
            )
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        f"👥 <b>{category['title']}</b>\nВыберите сервер:",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if query.data == "menu_players":
        await query.answer()
        await players(update, context)

    elif query.data == "menu_servers":
        await query.answer()
        await server(update, context)

    elif query.data == "menu_app_unavailable":
        await query.answer(
            "Mini App пока не настроен. Укажите WEB_APP_URL.",
            show_alert=True,
        )

    else:
        await query.answer()


def send_purchase_confirmation_message(
    user_id: int,
    privilege: str,
    server_name: str,
    duration: str,
    nickname: str,
    password: str,
    language: str = "ru",
    issue_mode: str = "",
    before_days: int = 0,
    after_days: int = 0,
    password_changed: bool = False,
    previous_privilege: str = "",
    target_privilege: str = "",
    credit_amount: int = 0,
    paid_amount: int = 0,
    cashback_amount: int = 0,
    cashback_percent: int = 0,
    balance_after: int = 0,
    identifier_type: str = PRIVILEGE_IDENTIFIER_NICKNAME,
    steam_id: str = "",
) -> bool:
    if not TOKEN:
        print("[PURCHASE NOTIFY ERROR] BOT_TOKEN is not set", file=sys.stderr)
        return False

    safe_privilege = html.escape(privilege)
    safe_server_name = html.escape(server_name)
    safe_duration = html.escape(duration)
    safe_nickname = html.escape(nickname)
    safe_steam_id = html.escape(normalize_steam_id(steam_id))
    safe_password = html.escape(password)
    safe_before_days = html.escape(str(max(int(before_days or 0), 0)))
    safe_after_days = html.escape(str(max(int(after_days or 0), 0)))
    safe_previous_privilege = html.escape(str(previous_privilege or "").strip() or "-")
    safe_target_privilege = html.escape(str(target_privilege or "").strip() or str(privilege))
    safe_credit_amount = format_money_uzs(max(int(credit_amount or 0), 0))
    safe_paid_amount = format_money_uzs(max(int(paid_amount or 0), 0))
    safe_cashback_amount_int = max(int(cashback_amount or 0), 0)
    safe_cashback_percent_int = max(int(cashback_percent or 0), 0)
    safe_cashback_amount = format_money_uzs(safe_cashback_amount_int)
    safe_balance_after = format_money_uzs(max(int(balance_after or 0), 0))
    normalized_mode = str(issue_mode or "").strip().lower()
    normalized_identifier_type = normalize_privilege_identifier_type(identifier_type)
    is_steam_identifier = normalized_identifier_type == PRIVILEGE_IDENTIFIER_STEAM
    cashback_line_uz = (
        f"🎁 <b>Keshbek:</b> +{safe_cashback_amount} UZS ({safe_cashback_percent_int}%)"
        if safe_cashback_amount_int > 0
        else ""
    )
    cashback_line_ru = (
        f"🎁 <b>Кэшбек:</b> +{safe_cashback_amount} UZS ({safe_cashback_percent_int}%)"
        if safe_cashback_amount_int > 0
        else ""
    )
    balance_line_uz = f"💳 <b>Balans:</b> {safe_balance_after} UZS"
    balance_line_ru = f"💳 <b>Баланс:</b> {safe_balance_after} UZS"

    normalized_language = str(language).strip().lower()
    if normalized_language == "uz":
        identifier_line = (
            f"🆔 <b>STEAM_ID:</b> <code>{safe_steam_id}</code>"
            if is_steam_identifier
            else f"👤 <b>Nick:</b> {safe_nickname}"
        )
        if normalized_mode == "upgraded":
            password_block = (
                f"🔑 <b>Yangi parol:</b> <tg-spoiler>{safe_password}</tg-spoiler>"
                if (password_changed and not is_steam_identifier)
                else "🔑 <b>Parol:</b> O'zgartirilmadi, eski parol qoldi"
            )
            text = (
                "⬆️ <b>Imtiyoz yangilandi (upgrade) ✅</b>\n"
                f"🔁 <b>O'tish:</b> {safe_previous_privilege} → {safe_target_privilege}\n"
                f"🎮 <b>Server:</b> {safe_server_name}\n"
                f"⏳ <b>Yangi muddat:</b> {safe_duration}\n"
                f"{identifier_line}\n"
                f"📊 <b>Oldin (kun):</b> {safe_before_days}\n"
                f"📈 <b>Hozir (kun):</b> {safe_after_days}\n"
                f"🏦 <b>Qolgan balans:</b> {safe_credit_amount} UZS\n"
                f"💰 <b>To'langan summa:</b> {safe_paid_amount} UZS"
            )
            if not is_steam_identifier:
                text += f"\n{password_block}"
            if cashback_line_uz:
                text += f"\n{cashback_line_uz}"
            text += f"\n{balance_line_uz}"
            text += "\n\n🔒 <b>Parolingizni hech kimga bermang</b>"
        elif normalized_mode in {"renewed", "reactivated"}:
            password_block = (
                f"🔑 <b>Yangi parol:</b> <tg-spoiler>{safe_password}</tg-spoiler>"
                if (password_changed and not is_steam_identifier)
                else "🔑 <b>Parol:</b> O'zgartirilmadi, eski parol qoldi"
            )
            text = (
                f"🔄 <b>{safe_privilege} uzaytirildi ✅</b>\n"
                f"🎮 <b>Server:</b> {safe_server_name}\n"
                f"⏳ <b>Qo'shilgan muddat:</b> {safe_duration}\n"
                f"{identifier_line}\n"
                f"📊 <b>Oldin (kun):</b> {safe_before_days}\n"
                f"📈 <b>Hozir (kun):</b> {safe_after_days}"
            )
            if not is_steam_identifier:
                text += f"\n{password_block}"
            if cashback_line_uz:
                text += f"\n{cashback_line_uz}"
            text += f"\n{balance_line_uz}"
            text += "\n\n🔒 <b>Parolingizni hech kimga bermang</b>"
        else:
            text = (
                f"🛒 <b>{safe_privilege} xaridi ✅</b>\n"
                f"🎮 <b>Server:</b> {safe_server_name}\n"
                f"⏳ <b>Muddat:</b> {safe_duration}\n"
                f"{identifier_line}"
            )
            if not is_steam_identifier:
                text += f"\n🔑 <b>Parol:</b> <tg-spoiler>{safe_password}</tg-spoiler>"
            if cashback_line_uz:
                text += f"\n{cashback_line_uz}"
            text += f"\n{balance_line_uz}"
            text += "\n\n🔒 <b>Parolingizni hech kimga bermang</b>"
    else:
        identifier_line = (
            f"🆔 <b>STEAM_ID:</b> <code>{safe_steam_id}</code>"
            if is_steam_identifier
            else f"👤 <b>Nick:</b> {safe_nickname}"
        )
        if normalized_mode == "upgraded":
            password_block = (
                f"🔑 <b>Ваш новый пароль:</b> <tg-spoiler>{safe_password}</tg-spoiler>"
                if (password_changed and not is_steam_identifier)
                else "🔑 <b>Пароль:</b> Вы не меняли пароль, у вас остался старый"
            )
            text = (
                "⬆️ <b>Апгрейд привилегии выполнен ✅</b>\n"
                f"🔁 <b>Переход:</b> {safe_previous_privilege} → {safe_target_privilege}\n"
                f"🎮 <b>Сервер:</b> {safe_server_name}\n"
                f"⏳ <b>Новый срок:</b> {safe_duration}\n"
                f"{identifier_line}\n"
                f"📊 <b>Было дней:</b> {safe_before_days}\n"
                f"📈 <b>Стало дней:</b> {safe_after_days}\n"
                f"🏦 <b>Было на балансе:</b> {safe_credit_amount} UZS\n"
                f"💸 <b>Доплата:</b> {safe_paid_amount} UZS"
            )
            if not is_steam_identifier:
                text += f"\n{password_block}"
            if cashback_line_ru:
                text += f"\n{cashback_line_ru}"
            text += f"\n{balance_line_ru}"
            text += "\n\n🔒 <b>Не передавайте свой пароль никому</b>"
        elif normalized_mode in {"renewed", "reactivated"}:
            password_block = (
                f"🔑 <b>Ваш новый пароль:</b> <tg-spoiler>{safe_password}</tg-spoiler>"
                if (password_changed and not is_steam_identifier)
                else "🔑 <b>Пароль:</b> Вы не меняли пароль, у вас остался старый"
            )
            text = (
                f"🔄 <b>Продление {safe_privilege} ✅</b>\n"
                f"🎮 <b>Сервер:</b> {safe_server_name}\n"
                f"⏳ <b>Продлено на:</b> {safe_duration}\n"
                f"{identifier_line}\n"
                f"📊 <b>Было дней:</b> {safe_before_days}\n"
                f"📈 <b>Стало дней:</b> {safe_after_days}"
            )
            if not is_steam_identifier:
                text += f"\n{password_block}"
            if cashback_line_ru:
                text += f"\n{cashback_line_ru}"
            text += f"\n{balance_line_ru}"
            text += "\n\n🔒 <b>Не передавайте свой пароль никому</b>"
        else:
            text = (
                f"🛒 <b>Покупка {safe_privilege} ✅</b>\n"
                f"🎮 <b>Сервер:</b> {safe_server_name}\n"
                f"⏳ <b>Срок:</b> {safe_duration}\n"
                f"{identifier_line}"
            )
            if not is_steam_identifier:
                text += f"\n🔑 <b>Пароль:</b> <tg-spoiler>{safe_password}</tg-spoiler>"
            if cashback_line_ru:
                text += f"\n{cashback_line_ru}"
            text += f"\n{balance_line_ru}"
            text += "\n\n🔒 <b>Не передавайте свой пароль никому</b>"

    try:
        response_payload = telegram_send_message(user_id, text)
        is_ok = bool(response_payload.get("ok"))
        if is_ok:
            print(f"[PURCHASE NOTIFY] Message sent to user_id={user_id}")
        else:
            print(
                f"[PURCHASE NOTIFY ERROR] Telegram API response: {response_payload}",
                file=sys.stderr,
            )
        return is_ok
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
        print(f"[PURCHASE NOTIFY ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
        return False


def send_bonus_confirmation_message(
    user_id: int,
    server_name: str,
    steam_id: str,
    nickname: str,
    bonus_added: int,
    bonus_before: int,
    bonus_after: int,
    amount: int,
    language: str = "ru",
) -> bool:
    if not TOKEN:
        print("[BONUS NOTIFY ERROR] BOT_TOKEN is not set", file=sys.stderr)
        return False

    safe_server_name = html.escape(server_name)
    safe_steam_id = html.escape(steam_id)
    safe_nickname = html.escape(nickname)
    safe_bonus_added = format_money_uzs(bonus_added)
    safe_bonus_before = format_money_uzs(bonus_before)
    safe_bonus_after = format_money_uzs(bonus_after)
    safe_amount = format_money_uzs(amount)

    normalized_language = str(language).strip().lower()
    if normalized_language == "uz":
        text = (
            "🪙 <b>Bonuslar muvaffaqiyatli qo'shildi ✅</b>\n"
            f"🎮 <b>Server:</b> {safe_server_name}\n"
            f"🆔 <b>STEAM_ID:</b> <code>{safe_steam_id}</code>\n"
            f"👤 <b>Nick:</b> {safe_nickname}\n"
            f"➕ <b>Qo'shildi:</b> {safe_bonus_added}\n"
            f"📊 <b>Oldin:</b> {safe_bonus_before}\n"
            f"📈 <b>Hozir:</b> {safe_bonus_after}\n"
            f"💰 <b>To'lov:</b> {safe_amount} UZS"
        )
    else:
        text = (
            "🪙 <b>Бонусы успешно зачислены ✅</b>\n"
            f"🎮 <b>Сервер:</b> {safe_server_name}\n"
            f"🆔 <b>STEAM_ID:</b> <code>{safe_steam_id}</code>\n"
            f"👤 <b>Ник:</b> {safe_nickname}\n"
            f"➕ <b>Начислено:</b> {safe_bonus_added}\n"
            f"📊 <b>Было:</b> {safe_bonus_before}\n"
            f"📈 <b>Стало:</b> {safe_bonus_after}\n"
            f"💰 <b>Сумма:</b> {safe_amount} UZS"
        )

    try:
        response_payload = telegram_send_message(user_id, text)
        return bool(response_payload.get("ok"))
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
        print(f"[BONUS NOTIFY ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
        return False


def send_balance_topup_confirmation_message(
    *,
    user_id: int,
    amount_added: int,
    balance_before: int,
    balance_after: int,
    language: str = "ru",
) -> bool:
    if not TOKEN:
        return False

    safe_amount_added = format_money_uzs(amount_added)
    safe_before = format_money_uzs(balance_before)
    safe_after = format_money_uzs(balance_after)
    normalized_language = str(language or "ru").strip().lower()

    if normalized_language == "uz":
        text = (
            "💳 <b>Balans muvaffaqiyatli to'ldirildi ✅</b>\n"
            f"➕ <b>Qo'shildi:</b> {safe_amount_added} UZS\n"
            f"📊 <b>Oldin:</b> {safe_before} UZS\n"
            f"💰 <b>Hozir:</b> {safe_after} UZS"
        )
    else:
        text = (
            "💳 <b>Баланс успешно пополнен ✅</b>\n"
            f"➕ <b>Зачислено:</b> {safe_amount_added} UZS\n"
            f"📊 <b>Было:</b> {safe_before} UZS\n"
            f"💰 <b>Стало:</b> {safe_after} UZS"
        )

    try:
        response_payload = telegram_send_message(user_id, text)
        return bool(response_payload.get("ok"))
    except Exception as error:
        print(f"[BALANCE TOPUP NOTIFY ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
        return False


def send_balance_topup_report_to_group(
    *,
    user_id: int,
    username: str = "",
    first_name: str = "",
    last_name: str = "",
    amount_added: int,
    balance_before: int,
    balance_after: int,
    screenshot_bytes: bytes = b"",
    screenshot_mime_type: str = "image/jpeg",
    screenshot_name: str = "balance-topup.jpg",
):
    reports_chat_id = get_reports_chat_id()
    if reports_chat_id is None:
        return False, None

    mention = build_user_mention(
        user_id=int(user_id),
        username=str(username or "").strip(),
        first_name=str(first_name or "").strip(),
        last_name=str(last_name or "").strip(),
    )
    safe_amount_added = format_money_uzs(amount_added)
    safe_before = format_money_uzs(balance_before)
    safe_after = format_money_uzs(balance_after)
    caption = (
        "💳 <b>Пополнение баланса</b>\n"
        f"👤 <b>Username:</b> {mention}\n"
        f"➕ <b>Зачислено:</b> {safe_amount_added} UZS\n"
        f"📊 <b>Было:</b> {safe_before} UZS\n"
        f"💰 <b>Стало:</b> {safe_after} UZS\n"
        "#balance_topup_report"
    )

    try:
        if screenshot_bytes:
            result = telegram_send_photo(
                chat_id=reports_chat_id,
                caption=caption,
                photo_bytes=screenshot_bytes,
                filename=safe_filename(screenshot_name, screenshot_mime_type),
                content_type=screenshot_mime_type or "image/jpeg",
                reply_markup=None,
            )
        else:
            result = telegram_send_message(reports_chat_id, caption)

        if not result.get("ok"):
            print(
                f"[REPORTS ERROR] Top-up report API response: {result}",
                file=sys.stderr,
            )
            return False, None

        message_id = result.get("result", {}).get("message_id")
        if isinstance(message_id, int):
            return True, int(message_id)
        return True, None
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
        print(
            f"[REPORTS ERROR] Failed to send balance top-up report: {_redact_sensitive_text(error)}",
            file=sys.stderr,
        )
        return False, None


def send_payment_verification_failed_message(
    user_id: int,
    *,
    reason: str,
    remaining_attempts: int,
    max_attempts: int,
    language: str = "ru",
    technical_error: bool = False,
    ban_seconds_remaining: int = 0,
) -> bool:
    if not TOKEN:
        return False

    normalized_language = str(language or "ru").strip().lower()
    localized_reason = localize_payment_reason(reason, normalized_language)
    safe_reason = html.escape(str(localized_reason or "").strip())
    remaining = max(int(remaining_attempts or 0), 0)
    maximum = max(int(max_attempts or 1), 1)
    ban_seconds = max(int(ban_seconds_remaining or 0), 0)
    ban_minutes = max((ban_seconds + 59) // 60, 1) if ban_seconds > 0 else 0

    if technical_error:
        if normalized_language == "uz":
            text = (
                "⏳ <b>To'lovni tekshirish vaqtinchalik ishlamadi.</b>\n"
                f"Sabab: {safe_reason}\n"
                "Iltimos, 1-2 daqiqadan keyin yana urinib ko'ring. "
                "Ushbu holatda urinish limiti hisoblanmaydi."
            )
        else:
            text = (
                "⏳ <b>Проверка оплаты временно недоступна.</b>\n"
                f"Причина: {safe_reason}\n"
                "Попробуйте снова через 1-2 минуты. "
                "В этом случае лимит попыток не уменьшается."
            )
    elif ban_seconds > 0:
        if normalized_language == "uz":
            text = (
                "⛔ <b>To'lov tekshiruvi vaqtincha bloklandi.</b>\n"
                f"Sabab: {safe_reason}\n"
                f"Juda ko'p noto'g'ri skrinshot yuborildi. "
                f"Qayta urinish vaqti: taxminan <b>{ban_minutes} daqiqa</b>."
            )
        else:
            text = (
                "⛔ <b>Проверка оплаты временно заблокирована.</b>\n"
                f"Причина: {safe_reason}\n"
                f"Отправлено слишком много неверных скриншотов. "
                f"Повторить можно примерно через <b>{ban_minutes} минут</b>."
            )
    elif remaining > 0:
        if normalized_language == "uz":
            text = (
                "❌ <b>Skrinshot to'lov sifatida tasdiqlanmadi.</b>\n"
                f"Sabab: {safe_reason}\n"
                f"Qolgan urinishlar: <b>{remaining} / {maximum}</b>."
            )
        else:
            text = (
                "❌ <b>Скриншот не прошёл проверку оплаты.</b>\n"
                f"Причина: {safe_reason}\n"
                f"Осталось попыток: <b>{remaining} из {maximum}</b>."
            )
    else:
        if normalized_language == "uz":
            text = (
                "🚫 <b>Skrinshot urinishlari limiti tugadi.</b>\n"
                f"Sabab: {safe_reason}\n"
                "To'lov sessiyasi yakunlandi. Xaridni boshidan boshlang."
            )
        else:
            text = (
                "🚫 <b>Лимит попыток скриншота исчерпан.</b>\n"
                f"Причина: {safe_reason}\n"
                "Сессия оплаты завершена. Начните покупку заново."
            )

    try:
        response_payload = telegram_send_message(user_id, text)
        return bool(response_payload.get("ok"))
    except Exception as error:
        print(f"[PAYMENT VERIFY NOTIFY ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
        return False


def build_cancel_purchase_prompt_markup(purchase_id):
    purchase_token = str(purchase_id or "").strip()
    if not purchase_token:
        return None

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Отменить",
                    callback_data=f"cancel_purchase_prompt:{purchase_token}",
                )
            ]
        ]
    )


def build_cancel_purchase_confirm_markup(purchase_id):
    purchase_token = str(purchase_id or "").strip()
    if not purchase_token:
        return None

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Да, отменить",
                    callback_data=f"cancel_purchase_confirm:{purchase_token}",
                )
            ],
            [
                InlineKeyboardButton(
                    "↩️ Назад",
                    callback_data=f"cancel_purchase_abort:{purchase_token}",
                )
            ],
        ]
    )


def send_purchase_report_to_group(record, screenshot_bytes, screenshot_mime_type, screenshot_name):
    reports_chat_id = get_reports_chat_id()
    if reports_chat_id is None:
        return False, None

    product_type = str(record.get("product_type", "privilege")).strip().lower()
    safe_privilege = html.escape(str(record.get("privilege", "-")))
    safe_server = html.escape(str(record.get("server", "-")))
    safe_duration = html.escape(str(record.get("duration", "-")))
    safe_nickname = html.escape(str(record.get("nickname", "-")))
    safe_steam_identifier = html.escape(normalize_steam_id(record.get("steam_id", "")))
    identifier_type = normalize_privilege_identifier_type(record.get("identifier_type", PRIVILEGE_IDENTIFIER_NICKNAME))
    is_steam_identifier = identifier_type == PRIVILEGE_IDENTIFIER_STEAM
    identifier_line = (
        f"🆔 <b>STEAM_ID:</b> <code>{safe_steam_identifier}</code>"
        if is_steam_identifier
        else f"🕹 <b>Nick:</b> {safe_nickname}"
    )
    safe_amount = format_money_uzs(record.get("amount", 0))
    cashback_amount = max(int(record.get("cashback_amount", 0) or 0), 0)
    cashback_percent = max(int(record.get("cashback_percent", 0) or 0), 0)
    safe_cashback_amount = format_money_uzs(cashback_amount)
    cashback_line = (
        f"🎁 <b>Кэшбек:</b> +{safe_cashback_amount} UZS ({cashback_percent}%)\n"
        if cashback_amount > 0
        else ""
    )
    balance_after_value = max(
        int(record.get("user_balance_after", record.get("balance_after", 0)) or 0),
        0,
    )
    safe_balance_after = format_money_uzs(balance_after_value)
    balance_after_line = f"💳 <b>Баланс после операции:</b> {safe_balance_after} UZS\n"
    mention = build_user_mention(
        user_id=int(record.get("user_id", 0)),
        username=str(record.get("username", "")),
        first_name=str(record.get("first_name", "")),
        last_name=str(record.get("last_name", "")),
    )

    if product_type == "bonus":
        safe_steam_id = html.escape(str(record.get("steam_id", "-")))
        bonus_added = format_money_uzs(record.get("bonus_added", 0))
        bonus_before = format_money_uzs(record.get("bonus_before", 0))
        bonus_after = format_money_uzs(record.get("bonus_after", 0))
        caption = (
            "🧾 <b>Новая покупка бонусов</b>\n"
            f"👤 <b>Username:</b> {mention}\n"
            f"🎮 <b>Сервер:</b> {safe_server}\n"
            f"🆔 <b>STEAM_ID:</b> <code>{safe_steam_id}</code>\n"
            f"🕹 <b>Ник:</b> {safe_nickname}\n"
            f"➕ <b>Начислено:</b> {bonus_added}\n"
            f"📊 <b>Было:</b> {bonus_before}\n"
            f"📈 <b>Стало:</b> {bonus_after}\n"
            f"💰 <b>Сумма:</b> {safe_amount} UZS\n"
            f"{cashback_line}"
            f"{balance_after_line}"
            "#bonus_purchase_report"
        )
        prompt_markup = build_cancel_purchase_prompt_markup(record.get("id"))
        reply_markup = prompt_markup.to_dict() if prompt_markup is not None else None
    else:
        issued_mode = str(record.get("issued_mode", "")).strip().lower()
        if issued_mode == "upgraded":
            safe_previous_privilege = html.escape(str(record.get("issued_previous_privilege", "")).strip() or "-")
            safe_target_privilege = html.escape(str(record.get("issued_target_privilege", "")).strip() or str(record.get("issued_privilege", "")).strip() or str(record.get("privilege", "-")))
            before_days = html.escape(str(max(int(record.get("issued_before_days", 0) or 0), 0)))
            after_days = html.escape(str(max(int(record.get("issued_after_days", 0) or 0), 0)))
            credit_amount = format_money_uzs(record.get("issued_credit_amount", 0))
            password_changed = bool(record.get("issued_password_changed", False))
            password_status = (
                "Не применяется (режим STEAM_ID)"
                if is_steam_identifier
                else ("Да" if password_changed else "Нет (оставлен старый)")
            )
            caption = (
                "🧾 <b>Апгрейд привилегии</b>\n"
                f"👤 <b>Username:</b> {mention}\n"
                f"🔁 <b>Переход:</b> {safe_previous_privilege} → {safe_target_privilege}\n"
                f"🎮 <b>Сервер:</b> {safe_server}\n"
                f"⏳ <b>Новый срок:</b> {safe_duration}\n"
                f"{identifier_line}\n"
                f"📊 <b>Было дней:</b> {before_days}\n"
                f"📈 <b>Стало дней:</b> {after_days}\n"
                f"🏦 <b>Было на балансе:</b> {credit_amount} UZS\n"
                f"💸 <b>Доплата:</b> {safe_amount} UZS\n"
                f"🔑 <b>Пароль изменён:</b> {password_status}\n"
                f"{cashback_line}"
                f"{balance_after_line}"
                "#privilege_upgrade_report"
            )
        elif issued_mode in {"renewed", "reactivated"}:
            safe_issued_privilege = html.escape(str(record.get("issued_privilege", "")).strip() or str(record.get("privilege", "-")))
            before_days = html.escape(str(max(int(record.get("issued_before_days", 0) or 0), 0)))
            after_days = html.escape(str(max(int(record.get("issued_after_days", 0) or 0), 0)))
            password_changed = bool(record.get("issued_password_changed", False))
            password_status = (
                "Не применяется (режим STEAM_ID)"
                if is_steam_identifier
                else ("Да" if password_changed else "Нет (оставлен старый)")
            )
            caption = (
                "🧾 <b>Продление привилегии</b>\n"
                f"👤 <b>Username:</b> {mention}\n"
                f"🛡 <b>Привилегия:</b> {safe_issued_privilege}\n"
                f"🎮 <b>Сервер:</b> {safe_server}\n"
                f"⏳ <b>Продлено на:</b> {safe_duration}\n"
                f"{identifier_line}\n"
                f"📊 <b>Было дней:</b> {before_days}\n"
                f"📈 <b>Стало дней:</b> {after_days}\n"
                f"🔑 <b>Пароль изменён:</b> {password_status}\n"
                f"💰 <b>Сумма:</b> {safe_amount} UZS\n"
                f"{cashback_line}"
                f"{balance_after_line}"
                "#privilege_renewal_report"
            )
        else:
            caption = (
                "🧾 <b>Новая покупка</b>\n"
                f"👤 <b>Username:</b> {mention}\n"
                f"🛡 <b>Привилегия:</b> {safe_privilege}\n"
                f"🎮 <b>Сервер:</b> {safe_server}\n"
                f"⏳ <b>Срок:</b> {safe_duration}\n"
                f"{identifier_line}\n"
                f"💰 <b>Сумма:</b> {safe_amount} UZS\n"
                f"{cashback_line}"
                f"{balance_after_line}"
                "#purchase_report"
            )
        prompt_markup = build_cancel_purchase_prompt_markup(record.get("id"))
        reply_markup = prompt_markup.to_dict() if prompt_markup is not None else None

    try:
        if screenshot_bytes:
            result = telegram_send_photo(
                chat_id=reports_chat_id,
                caption=caption,
                photo_bytes=screenshot_bytes,
                filename=safe_filename(screenshot_name, screenshot_mime_type),
                content_type=screenshot_mime_type,
                reply_markup=reply_markup,
            )
            if not result.get("ok"):
                print(
                    f"[REPORTS ERROR] sendPhoto API response: {result}",
                    file=sys.stderr,
                )
                return False, None
        else:
            result = telegram_send_message(
                reports_chat_id,
                caption,
                reply_markup=reply_markup,
            )
            if not result.get("ok"):
                print(
                    f"[REPORTS ERROR] sendMessage API response: {result}",
                    file=sys.stderr,
                )
                return False, None

        message_id = result.get("result", {}).get("message_id")
        if isinstance(message_id, int):
            return True, message_id
        return True, None
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
        print(
            f"[REPORTS ERROR] Failed to send group purchase report: {_redact_sensitive_text(error)}",
            file=sys.stderr,
        )
        return False, None


def build_periodic_report_text(report_type, start_dt, end_dt, records, *, period_cashback_total=0):
    if report_type == "daily":
        title = "📊 <b>Ежедневный отчёт по продажам</b>"
        hashtag = "#daily_report"
        period = start_dt.strftime("%d.%m.%Y")
    elif report_type == "weekly":
        title = "📈 <b>Еженедельный отчёт по продажам</b>"
        hashtag = "#weekly_report"
        period = f"{start_dt.strftime('%d.%m.%Y')} - {end_dt.strftime('%d.%m.%Y')}"
    else:
        title = "🗓 <b>Ежемесячный отчёт по продажам</b>"
        hashtag = "#monthly_report"
        period = start_dt.strftime("%m.%Y")

    total_count = len(records)
    total_amount = sum(int(record.get("amount", 0) or 0) for record in records)
    total_users_balance, users_with_balance = get_all_users_balance_total()
    total_cashback_all_time = get_cashback_totals()
    safe_period_cashback_total = max(int(period_cashback_total or 0), 0)
    by_server = defaultdict(lambda: {"count": 0, "amount": 0})
    by_privilege = defaultdict(lambda: {"count": 0, "amount": 0})

    for record in records:
        server_name = str(record.get("server", "Unknown"))
        privilege_name = str(record.get("privilege", "Unknown"))
        amount = int(record.get("amount", 0) or 0)

        by_server[server_name]["count"] += 1
        by_server[server_name]["amount"] += amount
        by_privilege[privilege_name]["count"] += 1
        by_privilege[privilege_name]["amount"] += amount

    text = (
        f"{title}\n"
        f"🕒 <b>Период:</b> {period}\n"
        f"🧾 <b>Продаж:</b> {total_count}\n"
        f"💰 <b>Общая сумма:</b> {format_money_uzs(total_amount)} UZS\n\n"
        f"💳 <b>Общий баланс пользователей:</b> {format_money_uzs(total_users_balance)} UZS "
        f"({users_with_balance} с положительным балансом)\n"
        f"🎁 <b>Общий кэшбек (всего):</b> {format_money_uzs(total_cashback_all_time)} UZS\n"
        f"⚡ <b>Кэшбек за период:</b> {format_money_uzs(safe_period_cashback_total)} UZS\n\n"
    )

    if total_count == 0:
        text += "ℹ️ За выбранный период продаж не было.\n\n"
    else:
        text += "<b>По серверам:</b>\n"
        for name, payload in sorted(
            by_server.items(),
            key=lambda item: (item[1]["amount"], item[1]["count"]),
            reverse=True,
        ):
            text += (
                f"• {html.escape(name)} — {payload['count']} "
                f"({format_money_uzs(payload['amount'])} UZS)\n"
            )

        text += "\n<b>По привилегиям:</b>\n"
        for name, payload in sorted(
            by_privilege.items(),
            key=lambda item: (item[1]["amount"], item[1]["count"]),
            reverse=True,
        ):
            text += (
                f"• {html.escape(name)} — {payload['count']} "
                f"({format_money_uzs(payload['amount'])} UZS)\n"
            )

        text += "\n"

    text += hashtag
    return text


def send_periodic_sales_report(report_type, start_dt, end_dt, key):
    reports_chat_id = get_reports_chat_id()
    if reports_chat_id is None:
        return False

    records = get_active_purchases_between(start_dt, end_dt)
    start_ts = int(start_dt.astimezone(datetime.timezone.utc).timestamp())
    end_ts = int(end_dt.astimezone(datetime.timezone.utc).timestamp())
    period_cashback_total = get_cashback_totals(start_ts=start_ts, end_ts=end_ts)
    text = build_periodic_report_text(
        report_type,
        start_dt,
        end_dt,
        records,
        period_cashback_total=period_cashback_total,
    )

    try:
        response_payload = telegram_send_message(reports_chat_id, text)
    except (HTTPError, URLError, TimeoutError, ValueError) as error:
        print(
            f"[REPORTS ERROR] Failed to send {report_type} report: {_redact_sensitive_text(error)}",
            file=sys.stderr,
        )
        return False

    if not response_payload.get("ok"):
        print(
            f"[REPORTS ERROR] {report_type} report API response: {response_payload}",
            file=sys.stderr,
        )
        return False

    mark_periodic_report_sent(report_type, key)
    return True


def run_reports_scheduler_forever():
    while True:
        try:
            now = datetime.datetime.now(REPORTS_TIMEZONE)

            if (
                now.hour == 21
                and now.minute == 0
                and should_send_periodic_report("daily", now.strftime("%Y-%m-%d"))
            ):
                day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                send_periodic_sales_report("daily", day_start, now, now.strftime("%Y-%m-%d"))

            if (
                now.weekday() == 6
                and now.hour == 21
                and now.minute == 1
                and should_send_periodic_report("weekly", f"{now.isocalendar().year}-W{now.isocalendar().week:02d}")
            ):
                week_start = (now - datetime.timedelta(days=now.weekday())).replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
                weekly_key = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
                send_periodic_sales_report("weekly", week_start, now, weekly_key)

            month_last_day = calendar.monthrange(now.year, now.month)[1]
            monthly_key = f"{now.year:04d}-{now.month:02d}"
            if (
                now.day == month_last_day
                and now.hour == 21
                and now.minute == 2
                and should_send_periodic_report("monthly", monthly_key)
            ):
                month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                send_periodic_sales_report("monthly", month_start, now, monthly_key)
        except Exception as error:
            print(
                f"[REPORTS ERROR] Scheduler loop failed: {_redact_sensitive_text(error)}",
                file=sys.stderr,
            )

        time.sleep(REPORTS_SCHEDULER_CHECK_SECONDS)


async def group_reports_autobind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in {"group", "supergroup"}:
        return

    if get_reports_chat_id() is None:
        bind_reports_chat(chat.id, chat.title or "")
        print(f"[REPORTS] Auto-bound reports chat: {chat.id} ({chat.title})")


async def bind_reports_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message
    if not chat or not message:
        return

    if chat.type not in {"group", "supergroup"}:
        await message.reply_text("Команду /bind_reports нужно запускать внутри группы отчётов.")
        return

    if not bind_reports_chat(chat.id, chat.title or ""):
        await message.reply_text("Не удалось привязать группу отчётов.")
        return

    await message.reply_text(
        f"✅ Группа отчётов привязана.\nID: <code>{chat.id}</code>",
        parse_mode="HTML",
    )


def _extract_purchase_id(callback_data, prefixes):
    payload = str(callback_data or "").strip()
    for prefix in prefixes:
        if payload.startswith(prefix):
            return payload[len(prefix):].strip()
    return ""


async def _ensure_reports_admin_access(query, context):
    message = query.message
    if not message:
        await query.answer("Сообщение не найдено", show_alert=True)
        return None

    reports_chat_id = get_reports_chat_id()
    if reports_chat_id is None or str(message.chat_id) != str(reports_chat_id):
        await query.answer("Эта кнопка доступна только в группе отчётов.", show_alert=True)
        return None

    try:
        member = await context.bot.get_chat_member(message.chat_id, query.from_user.id)
    except Exception:
        await query.answer("Не удалось проверить права в группе.", show_alert=True)
        return None

    if member.status not in {"administrator", "creator"}:
        await query.answer("Только админ может отменять оплату.", show_alert=True)
        return None

    return message


async def cancel_purchase_prompt_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return

    purchase_id = _extract_purchase_id(
        query.data,
        prefixes=("cancel_purchase_prompt:", "cancel_purchase:"),
    )
    if not purchase_id:
        await query.answer("Некорректный ID оплаты", show_alert=True)
        return

    message = await _ensure_reports_admin_access(query, context)
    if message is None:
        return

    confirm_markup = build_cancel_purchase_confirm_markup(purchase_id)
    if confirm_markup is None:
        await query.answer("Некорректный ID оплаты", show_alert=True)
        return

    try:
        await query.edit_message_reply_markup(reply_markup=confirm_markup)
    except Exception:
        pass

    await query.answer(
        "Если отменить покупку, сумма будет исключена из отчётов. Действительно хотите отменить?",
        show_alert=True,
    )


async def cancel_purchase_abort_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return

    purchase_id = _extract_purchase_id(
        query.data,
        prefixes=("cancel_purchase_abort:",),
    )
    if not purchase_id:
        await query.answer("Некорректный ID оплаты", show_alert=True)
        return

    message = await _ensure_reports_admin_access(query, context)
    if message is None:
        return

    prompt_markup = build_cancel_purchase_prompt_markup(purchase_id)
    if prompt_markup is None:
        await query.answer("Некорректный ID оплаты", show_alert=True)
        return

    try:
        await query.edit_message_reply_markup(reply_markup=prompt_markup)
    except Exception:
        pass

    await query.answer("Отмена операции отменена.")


async def cancel_purchase_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return

    purchase_id = _extract_purchase_id(
        query.data,
        prefixes=("cancel_purchase_confirm:",),
    )
    if not purchase_id:
        await query.answer("Некорректный ID оплаты", show_alert=True)
        return

    message = await _ensure_reports_admin_access(query, context)
    if message is None:
        return

    canceled_record = cancel_purchase_record(purchase_id, query.from_user.id)
    if not canceled_record:
        await query.answer("Оплата уже отменена или не найдена.", show_alert=True)
        return

    try:
        await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
    except Exception:
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

    await query.answer("Оплата отменена. Сумма исключена из отчётов.")


async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.web_app_data:
        return

    raw_data = message.web_app_data.data

    try:
        payload = json.loads(raw_data)
    except Exception as error:
        print(
            f"[WEBAPP DATA ERROR] Invalid JSON payload -> {_redact_sensitive_text(error)}",
            file=sys.stderr,
        )
        return

    if payload.get("type") != "purchase_confirmed":
        return

    user_id = int(update.effective_user.id) if update.effective_user else 0
    language = str(payload.get("language", "ru")).strip().lower()
    if language not in {"ru", "uz"}:
        language = "ru"

    if user_id:
        if language == "uz":
            warning_text = (
                "⚠️ <b>Eski miniapp javobi qabul qilinmadi.</b>\n"
                "To'lov tasdiqlash faqat server API orqali ishlaydi.\n"
                "Agar to'lov rad etilgan bo'lsa, skrinshotni qayta yuboring."
            )
        else:
            warning_text = (
                "⚠️ <b>Устаревшее подтверждение миниаппа проигнорировано.</b>\n"
                "Выдача работает только через серверный API.\n"
                "Если оплата отклонена, загрузите скриншот заново."
            )
        try:
            telegram_send_message(user_id, warning_text)
        except Exception:
            pass

    print(
        (
            "[WEBAPP DATA] Ignored legacy purchase_confirmed payload "
            f"user_id={user_id} because API purchase flow is required"
        ),
        file=sys.stderr,
    )


class MiniAppAPIHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept, bypass-tunnel-reminder")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept, bypass-tunnel-reminder")
        self.end_headers()

    def do_POST(self):
        path = urlparse(self.path).path

        content_length = int(self.headers.get("Content-Length", "0") or 0)
        if content_length <= 0:
            self._send_json(400, {"error": "Request body is empty"})
            return

        raw_body = self.rfile.read(content_length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            self._send_json(400, {"error": "Invalid JSON payload"})
            return

        if path == "/api/privilege-password-verify":
            server_id = str(payload.get("serverId", "")).strip()
            server_name = str(payload.get("serverName", "")).strip()
            identifier_type = normalize_privilege_identifier_type(payload.get("identifierType", "nickname"))
            nickname = str(payload.get("nickname", "")).strip()
            password = str(payload.get("password", "")).strip()
            if identifier_type == PRIVILEGE_IDENTIFIER_STEAM:
                self._send_json(400, {"error": "Password verification is not used for STEAM_ID mode"})
                return
            if not server_id or not nickname or not password:
                self._send_json(400, {"error": "serverId, nickname and password are required"})
                return
            if not _is_known_server(server_id):
                self._send_json(400, {"error": "Unknown server"})
                return

            try:
                result = _verify_privilege_password_from_users_ini(
                    server_id=server_id,
                    server_name=server_name,
                    nickname=nickname,
                    password=password,
                )
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                print(f"[PRIVILEGE VERIFY ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
                self._send_json(502, {"error": "Failed to verify privilege password"})
                return

            self._send_json(
                200,
                {
                    "ok": True,
                    "valid": bool(result.get("valid")),
                    "account": {
                        "supported": bool(result.get("supported")),
                        "exists": bool(result.get("exists")),
                        "identifierType": str(result.get("identifier_type", PRIVILEGE_IDENTIFIER_NICKNAME)),
                        "nickname": str(result.get("nickname", "")),
                        "steamId": str(result.get("steam_id", "")),
                        "privilege": str(result.get("privilege", "")),
                        "flags": str(result.get("flags", "")),
                        "days": int(result.get("days", 0) or 0),
                        "isPermanent": bool(result.get("is_permanent")),
                        "isDisabled": bool(result.get("is_disabled")),
                        "isExpired": bool(result.get("is_expired")),
                    },
                    "timestamp": int(time.time()),
                },
            )
            return

        if path == "/api/balance-topup":
            try:
                user_id = int(payload.get("userId", 0))
            except (TypeError, ValueError):
                user_id = 0
            username = str(payload.get("username", "")).strip().lstrip("@")
            first_name = str(payload.get("firstName", "")).strip()
            last_name = str(payload.get("lastName", "")).strip()
            screenshot_data_url = str(payload.get("screenshotDataUrl", "")).strip()
            screenshot_name = str(payload.get("screenshotName", "")).strip()
            screenshot_mime_type = str(payload.get("screenshotMimeType", "")).strip()
            language = str(payload.get("language", "ru")).strip().lower()
            if language not in {"ru", "uz"}:
                language = "ru"
            topup_session_id = str(payload.get("topupSessionId", "")).strip()
            try:
                topup_session_started_at = int(payload.get("topupSessionStartedAt", 0))
            except (TypeError, ValueError):
                topup_session_started_at = 0
            try:
                topup_session_expires_at = int(payload.get("topupSessionExpiresAt", 0))
            except (TypeError, ValueError):
                topup_session_expires_at = 0

            if user_id <= 0 or not screenshot_data_url:
                self._send_json(400, {"error": "Missing required fields"})
                return

            now_ts = int(time.time())
            if (
                not topup_session_id
                or topup_session_started_at <= 0
                or topup_session_expires_at <= 0
                or topup_session_expires_at <= topup_session_started_at
                or (topup_session_expires_at - topup_session_started_at) > (PAYMENT_UPLOAD_SESSION_SECONDS + 15)
            ):
                self._send_json(
                    400,
                    {
                        "error": (
                            "To'ldirish sessiyasi noto'g'ri. Qaytadan boshlang."
                            if language == "uz"
                            else "Сессия пополнения недействительна. Начните заново."
                        )
                    },
                )
                return
            if now_ts > topup_session_expires_at:
                self._send_json(
                    400,
                    {
                        "error": (
                            "To'ldirish sessiyasi tugadi. Qaytadan boshlang."
                            if language == "uz"
                            else "Сессия пополнения истекла. Начните заново."
                        )
                    },
                )
                return

            violation_status = get_user_payment_violation_status(
                user_id,
                payment_session_id=topup_session_id,
            )
            if violation_status.get("banned"):
                reason = format_payment_ban_reason(
                    reason=violation_status.get("reason", ""),
                    seconds_remaining=violation_status.get("seconds_remaining", 0),
                    language=language,
                )
                send_payment_verification_failed_message(
                    user_id=user_id,
                    reason=reason,
                    remaining_attempts=0,
                    max_attempts=PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    language=language,
                    technical_error=False,
                    ban_seconds_remaining=int(violation_status.get("seconds_remaining", 0) or 0),
                )
                self._send_json(
                    429,
                    {
                        "error": reason,
                        "paymentVerified": False,
                        "paymentBanned": True,
                        "blockedUntil": int(violation_status.get("blocked_until", 0) or 0),
                        "secondsRemaining": int(violation_status.get("seconds_remaining", 0) or 0),
                        "remainingAttempts": 0,
                        "maxAttempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    },
                )
                return

            attempt_key = _build_payment_attempt_key(
                user_id=user_id,
                product_type="topup",
                server_id="balance",
                amount=0,
                payment_session_id=topup_session_id,
            )
            attempt_status = get_payment_attempt_status(attempt_key)
            if attempt_status.get("blocked"):
                violation_status = get_user_payment_violation_status(
                    user_id,
                    payment_session_id=topup_session_id,
                )
                ban_seconds_remaining = int(violation_status.get("seconds_remaining", 0) or 0)
                is_banned = bool(violation_status.get("banned")) or ban_seconds_remaining > 0
                reason = (
                    format_payment_ban_reason(
                        reason=violation_status.get("reason", ""),
                        seconds_remaining=ban_seconds_remaining,
                        language=language,
                    )
                    if is_banned
                    else (
                        "Ushbu to'ldirish sessiyasi uchun urinishlar limiti tugagan"
                        if language == "uz"
                        else "Лимит попыток уже исчерпан для этой сессии пополнения"
                    )
                )
                self._send_json(
                    429,
                    {
                        "error": reason,
                        "paymentVerified": False,
                        "paymentBanned": bool(is_banned),
                        "blockedUntil": int(violation_status.get("blocked_until", 0) or 0) if is_banned else 0,
                        "secondsRemaining": ban_seconds_remaining if is_banned else 0,
                        "remainingAttempts": 0,
                        "maxAttempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    },
                )
                return

            try:
                screenshot_bytes, detected_mime_type = decode_image_data_url(screenshot_data_url)
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return

            if not screenshot_mime_type:
                screenshot_mime_type = detected_mime_type

            payment_verification = PAYMENT_VERIFIER.verify_topup(
                screenshot_bytes=screenshot_bytes,
                screenshot_mime_type=screenshot_mime_type,
                payment_key=attempt_key,
                payment_context={
                    "session_id": topup_session_id,
                    "user_id": int(user_id),
                    "flow": "balance_topup",
                },
                session_started_at=topup_session_started_at,
                session_expires_at=topup_session_expires_at,
            )

            if not bool(payment_verification.get("ok")):
                raw_reason = str(payment_verification.get("reason", "")).strip() or "Пополнение не подтверждено"
                technical_error = is_technical_payment_verification_error(raw_reason)
                localized_reason = localize_payment_reason(raw_reason, language)
                ban_seconds_remaining = 0
                is_user_banned = False
                violation_status = {}
                if technical_error:
                    remaining_attempts = int(attempt_status.get("remaining", PAYMENT_MAX_SCREENSHOT_ATTEMPTS))
                    response_reason = (
                        f"{localized_reason}. 1-2 daqiqadan keyin qayta urinib ko'ring."
                        if language == "uz"
                        else f"{localized_reason}. Попробуйте снова через 1-2 минуты."
                    )
                else:
                    attempt_status = register_payment_attempt_failure(attempt_key, reason=raw_reason)
                    remaining_attempts = int(attempt_status.get("remaining", 0))
                    violation_status = register_user_payment_violation(
                        user_id,
                        reason=raw_reason,
                        payment_session_id=topup_session_id,
                        session_started_at=topup_session_started_at,
                        session_expires_at=topup_session_expires_at,
                    )
                    ban_seconds_remaining = int(violation_status.get("seconds_remaining", 0) or 0)
                    is_user_banned = bool(violation_status.get("banned")) and ban_seconds_remaining > 0
                    if is_user_banned:
                        remaining_attempts = 0
                        response_reason = format_payment_ban_reason(
                            reason=raw_reason,
                            seconds_remaining=ban_seconds_remaining,
                            language=language,
                        )
                    elif remaining_attempts > 0:
                        if language == "uz":
                            response_reason = (
                                f"{localized_reason}. To'g'ri skrinshotni yana yuboring. "
                                f"Qolgan urinishlar: {remaining_attempts} / {PAYMENT_MAX_SCREENSHOT_ATTEMPTS}."
                            )
                        else:
                            response_reason = (
                                f"{localized_reason}. Попробуйте отправить корректный скриншот ещё раз. "
                                f"Осталось попыток: {remaining_attempts} из {PAYMENT_MAX_SCREENSHOT_ATTEMPTS}."
                            )
                    else:
                        response_reason = (
                            f"{localized_reason}. Urinishlar limiti tugadi. "
                            "To'ldirish sessiyasi yakunlandi, qaytadan boshlang."
                            if language == "uz"
                            else (
                                f"{localized_reason}. Лимит попыток исчерпан. "
                                "Сессия пополнения завершена, начните заново."
                            )
                        )

                send_payment_verification_failed_message(
                    user_id=user_id,
                    reason=raw_reason,
                    remaining_attempts=remaining_attempts,
                    max_attempts=PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    language=language,
                    technical_error=technical_error,
                    ban_seconds_remaining=ban_seconds_remaining,
                )
                status_code = 400
                if is_user_banned or remaining_attempts <= 0:
                    status_code = 429
                self._send_json(
                    status_code,
                    {
                        "error": response_reason,
                        "paymentVerified": False,
                        "paymentBanned": bool(is_user_banned),
                        "blockedUntil": int(violation_status.get("blocked_until", 0) or 0)
                        if (not technical_error and is_user_banned)
                        else 0,
                        "secondsRemaining": ban_seconds_remaining if is_user_banned else 0,
                        "remainingAttempts": remaining_attempts,
                        "maxAttempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    },
                )
                return

            credited_amount = int(payment_verification.get("credited_amount", 0) or 0)
            if credited_amount <= 0:
                self._send_json(
                    400,
                    {
                        "error": (
                            "Balans to'ldirish tasdiqlanmadi: summa aniqlanmadi"
                            if language == "uz"
                            else "Пополнение не подтверждено: не удалось определить сумму"
                        )
                    },
                )
                return

            try:
                balance_before, balance_after = adjust_user_balance(
                    user_id,
                    credited_amount,
                    transaction_type="topup",
                    metadata={
                        "session_id": topup_session_id,
                        "verification_mode": str(payment_verification.get("mode", "")),
                    },
                )
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return

            reset_payment_attempts(attempt_key)
            reset_user_payment_violations(user_id, payment_session_id=topup_session_id)

            send_balance_topup_confirmation_message(
                user_id=user_id,
                amount_added=credited_amount,
                balance_before=balance_before,
                balance_after=balance_after,
                language=language,
            )
            send_balance_topup_report_to_group(
                user_id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                amount_added=credited_amount,
                balance_before=balance_before,
                balance_after=balance_after,
                screenshot_bytes=screenshot_bytes,
                screenshot_mime_type=screenshot_mime_type,
                screenshot_name=screenshot_name,
            )

            self._send_json(
                200,
                {
                    "ok": True,
                    "timestamp": int(time.time()),
                    "creditedAmount": int(credited_amount),
                    "balanceBefore": int(balance_before),
                    "balanceAfter": int(balance_after),
                    "paymentVerification": {
                        "ok": bool(payment_verification.get("ok")),
                        "mode": str(payment_verification.get("mode", "")),
                    },
                    "balance": {
                        "balance": int(balance_after),
                        "updatedAt": int(time.time()),
                    },
                },
            )
            return

        if path != "/api/purchase-confirmed":
            self._send_json(404, {"error": "Not found"})
            return

        try:
            user_id = int(payload.get("userId", 0))
        except (TypeError, ValueError):
            user_id = 0

        product_type = str(payload.get("productType", "privilege")).strip().lower()
        if product_type not in {"privilege", "bonus"}:
            product_type = "privilege"

        server_id = str(payload.get("serverId", "")).strip()
        server_name = str(payload.get("server", "")).strip()
        privilege = str(payload.get("privilege", "")).strip()
        duration = str(payload.get("duration", "")).strip()
        identifier_type = normalize_privilege_identifier_type(payload.get("identifierType", "nickname"))
        nickname = str(payload.get("nickname", "")).strip()
        password = str(payload.get("password", "")).strip()
        current_password = str(payload.get("currentPassword", "")).strip()
        steam_id = normalize_steam_id(payload.get("steamId", ""))
        bonus_package_label = str(payload.get("bonusPackageLabel", "")).strip()
        renewal_requested = bool(payload.get("renewalRequested", False))
        change_password = bool(payload.get("changePassword", False))
        try:
            amount = int(payload.get("amount", 0))
        except (TypeError, ValueError):
            amount = 0
        try:
            duration_months = int(payload.get("durationMonths", 0))
        except (TypeError, ValueError):
            duration_months = 0
        try:
            bonus_amount = int(payload.get("bonusAmount", 0))
        except (TypeError, ValueError):
            bonus_amount = 0
        username = str(payload.get("username", "")).strip().lstrip("@")
        first_name = str(payload.get("firstName", "")).strip()
        last_name = str(payload.get("lastName", "")).strip()
        screenshot_data_url = str(payload.get("screenshotDataUrl", "")).strip()
        screenshot_name = str(payload.get("screenshotName", "")).strip()
        screenshot_mime_type = str(payload.get("screenshotMimeType", "")).strip()
        language = str(payload.get("language", "ru")).strip().lower()
        payment_session_id = str(payload.get("paymentSessionId", "")).strip()
        try:
            payment_session_started_at = int(payload.get("paymentSessionStartedAt", 0))
        except (TypeError, ValueError):
            payment_session_started_at = 0
        try:
            payment_session_expires_at = int(payload.get("paymentSessionExpiresAt", 0))
        except (TypeError, ValueError):
            payment_session_expires_at = 0
        use_balance_raw = payload.get("useBalance", False)
        if isinstance(use_balance_raw, bool):
            use_balance = use_balance_raw
        else:
            use_balance = str(use_balance_raw).strip().lower() in {"1", "true", "yes", "on"}
        if language not in {"ru", "uz"}:
            language = "ru"

        if (
            user_id <= 0
            or not server_id
            or not server_name
        ):
            self._send_json(400, {"error": "Missing required fields"})
            return
        if not use_balance and not screenshot_data_url:
            self._send_json(400, {"error": "Missing required fields"})
            return
        if not _is_known_server(server_id):
            self._send_json(400, {"error": "Unknown server"})
            return

        now_ts = int(time.time())

        if product_type == "bonus" and amount <= 0:
            self._send_json(400, {"error": "Missing required fields"})
            return
        if product_type == "privilege" and amount < 0:
            self._send_json(400, {"error": "Missing required fields"})
            return

        if product_type == "bonus":
            expected_bonus_price = BONUS_TARIFF_PRICE_BY_BONUS_AMOUNT.get(int(bonus_amount or 0))
            if expected_bonus_price is None:
                self._send_json(400, {"error": "Unsupported bonus package"})
                return
            if int(amount) != int(expected_bonus_price):
                self._send_json(400, {"error": "Invalid amount for selected bonus package"})
                return

        attempt_key = ""
        screenshot_bytes = b""
        payment_verification = {
            "ok": True,
            "mode": "internal_balance" if use_balance else "",
            "reason": "",
        }

        if not use_balance:
            if (
                not payment_session_id
                or payment_session_started_at <= 0
                or payment_session_expires_at <= 0
                or payment_session_expires_at <= payment_session_started_at
                or (payment_session_expires_at - payment_session_started_at) > (PAYMENT_UPLOAD_SESSION_SECONDS + 15)
            ):
                self._send_json(
                    400,
                    {
                        "error": (
                            "To'lov sessiyasi noto'g'ri. Xaridni qaytadan boshlang."
                            if language == "uz"
                            else "Сессия оплаты недействительна. Начните покупку заново."
                        )
                    },
                )
                return
            if now_ts > payment_session_expires_at:
                self._send_json(
                    400,
                    {
                        "error": (
                            "To'lov sessiyasi tugadi. Xaridni qaytadan boshlang."
                            if language == "uz"
                            else "Сессия оплаты истекла. Начните покупку заново."
                        )
                    },
                )
                return

            violation_status = get_user_payment_violation_status(
                user_id,
                payment_session_id=payment_session_id,
            )
            if violation_status.get("banned"):
                reason = format_payment_ban_reason(
                    reason=violation_status.get("reason", ""),
                    seconds_remaining=violation_status.get("seconds_remaining", 0),
                    language=language,
                )
                send_payment_verification_failed_message(
                    user_id=user_id,
                    reason=reason,
                    remaining_attempts=0,
                    max_attempts=PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    language=language,
                    technical_error=False,
                    ban_seconds_remaining=int(violation_status.get("seconds_remaining", 0) or 0),
                )
                self._send_json(
                    429,
                    {
                        "error": reason,
                        "paymentVerified": False,
                        "paymentBanned": True,
                        "blockedUntil": int(violation_status.get("blocked_until", 0) or 0),
                        "secondsRemaining": int(violation_status.get("seconds_remaining", 0) or 0),
                        "remainingAttempts": 0,
                        "maxAttempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    },
                )
                return

            attempt_key = _build_payment_attempt_key(
                user_id=user_id,
                product_type=product_type,
                server_id=server_id,
                amount=amount,
                privilege=privilege,
                duration_months=duration_months,
                identifier_type=identifier_type,
                nickname=nickname,
                steam_id=steam_id,
                bonus_amount=bonus_amount,
                payment_session_id=payment_session_id,
            )
            attempt_status = get_payment_attempt_status(attempt_key)
            if attempt_status.get("blocked"):
                violation_status = get_user_payment_violation_status(
                    user_id,
                    payment_session_id=payment_session_id,
                )
                ban_seconds_remaining = int(violation_status.get("seconds_remaining", 0) or 0)
                is_banned = bool(violation_status.get("banned")) or ban_seconds_remaining > 0
                reason = (
                    format_payment_ban_reason(
                        reason=violation_status.get("reason", ""),
                        seconds_remaining=ban_seconds_remaining,
                        language=language,
                    )
                    if is_banned
                    else (
                        "Ushbu to'lov sessiyasi uchun urinishlar limiti tugagan"
                        if language == "uz"
                        else "Лимит попыток уже исчерпан для этой сессии оплаты"
                    )
                )
                send_payment_verification_failed_message(
                    user_id=user_id,
                    reason=reason,
                    remaining_attempts=0,
                    max_attempts=PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    language=language,
                    technical_error=False,
                    ban_seconds_remaining=ban_seconds_remaining,
                )
                self._send_json(
                    429,
                    {
                        "error": reason,
                        "paymentVerified": False,
                        "paymentBanned": bool(is_banned),
                        "blockedUntil": int(violation_status.get("blocked_until", 0) or 0) if is_banned else 0,
                        "secondsRemaining": ban_seconds_remaining if is_banned else 0,
                        "remainingAttempts": 0,
                        "maxAttempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    },
                )
                return

            try:
                screenshot_bytes, detected_mime_type = decode_image_data_url(screenshot_data_url)
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return

            if not screenshot_mime_type:
                screenshot_mime_type = detected_mime_type

            payment_verification = PAYMENT_VERIFIER.verify_payment(
                screenshot_bytes=screenshot_bytes,
                screenshot_mime_type=screenshot_mime_type,
                expected_amount=amount,
                payment_key=attempt_key,
                session_started_at=payment_session_started_at,
                session_expires_at=payment_session_expires_at,
                payment_context={
                    "session_id": payment_session_id,
                    "user_id": int(user_id),
                    "product_type": product_type,
                    "server_id": str(server_id),
                    "identifier_type": str(identifier_type),
                    "nickname": str(nickname),
                    "steam_id": str(steam_id),
                    "amount": int(amount),
                    "privilege": str(privilege),
                    "duration_months": int(duration_months),
                },
            )
            if not bool(payment_verification.get("ok")):
                raw_reason = str(payment_verification.get("reason", "")).strip() or "Payment verification failed"
                technical_error = is_technical_payment_verification_error(raw_reason)
                localized_reason = localize_payment_reason(raw_reason, language)
                ban_seconds_remaining = 0
                is_user_banned = False
                violation_status = {}
                if technical_error:
                    remaining_attempts = int(attempt_status.get("remaining", PAYMENT_MAX_SCREENSHOT_ATTEMPTS))
                    response_reason = (
                        f"{localized_reason}. 1-2 daqiqadan keyin qayta urinib ko'ring."
                        if language == "uz"
                        else f"{localized_reason}. Попробуйте снова через 1-2 минуты."
                    )
                else:
                    attempt_status = register_payment_attempt_failure(attempt_key, reason=raw_reason)
                    remaining_attempts = int(attempt_status.get("remaining", 0))
                    violation_status = register_user_payment_violation(
                        user_id,
                        reason=raw_reason,
                        payment_session_id=payment_session_id,
                        session_started_at=payment_session_started_at,
                        session_expires_at=payment_session_expires_at,
                    )
                    ban_seconds_remaining = int(violation_status.get("seconds_remaining", 0) or 0)
                    is_user_banned = bool(violation_status.get("banned")) and ban_seconds_remaining > 0
                    if is_user_banned:
                        remaining_attempts = 0
                        response_reason = format_payment_ban_reason(
                            reason=raw_reason,
                            seconds_remaining=ban_seconds_remaining,
                            language=language,
                        )
                    elif remaining_attempts > 0:
                        if language == "uz":
                            response_reason = (
                                f"{localized_reason}. To'g'ri skrinshotni yana yuboring. "
                                f"Qolgan urinishlar: {remaining_attempts} / {PAYMENT_MAX_SCREENSHOT_ATTEMPTS}."
                            )
                        else:
                            response_reason = (
                                f"{localized_reason}. Попробуйте отправить корректный скриншот ещё раз. "
                                f"Осталось попыток: {remaining_attempts} из {PAYMENT_MAX_SCREENSHOT_ATTEMPTS}."
                            )
                    else:
                        response_reason = (
                            f"{localized_reason}. Urinishlar limiti tugadi. "
                            "To'lov sessiyasi yakunlandi, xaridni qaytadan boshlang."
                            if language == "uz"
                            else (
                                f"{localized_reason}. Лимит попыток исчерпан. "
                                "Сессия оплаты завершена, начните покупку заново."
                            )
                        )
                print(
                    (
                        "[PAYMENT VERIFY] Rejected purchase "
                        f"user_id={user_id} amount={amount} reason={_redact_sensitive_text(raw_reason)} "
                        f"remaining_attempts={remaining_attempts}"
                    ),
                    file=sys.stderr,
                )
                send_payment_verification_failed_message(
                    user_id=user_id,
                    reason=raw_reason,
                    remaining_attempts=remaining_attempts,
                    max_attempts=PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    language=language,
                    technical_error=technical_error,
                    ban_seconds_remaining=ban_seconds_remaining,
                )
                status_code = 400
                if is_user_banned or remaining_attempts <= 0:
                    status_code = 429
                self._send_json(
                    status_code,
                    {
                        "error": response_reason,
                        "paymentVerified": False,
                        "paymentBanned": bool(is_user_banned),
                        "blockedUntil": int(violation_status.get("blocked_until", 0) or 0)
                        if (not technical_error and is_user_banned)
                        else 0,
                        "secondsRemaining": ban_seconds_remaining if is_user_banned else 0,
                        "remainingAttempts": remaining_attempts,
                        "maxAttempts": PAYMENT_MAX_SCREENSHOT_ATTEMPTS,
                    },
                )
                return
            reset_payment_attempts(attempt_key)
            reset_user_payment_violations(user_id, payment_session_id=payment_session_id)
            print(
                (
                    "[PAYMENT VERIFY] Approved purchase "
                    f"user_id={user_id} amount={amount} mode={payment_verification.get('mode', '')}"
                ),
                file=sys.stderr,
            )
        else:
            available_balance = get_user_balance(user_id)
            if int(available_balance) < int(amount):
                missing_amount = int(amount) - int(available_balance)
                self._send_json(
                    400,
                    {
                        "error": "Balansda mablag' yetarli emas" if language == "uz" else "Недостаточно средств на балансе",
                        "insufficientBalance": True,
                        "requiredAmount": int(amount),
                        "balanceAmount": int(available_balance),
                        "missingAmount": int(missing_amount),
                    },
                )
                return

        balance_before_purchase = 0
        balance_after_purchase = 0
        balance_spent = 0
        user_balance_after_operation = int(get_user_balance(user_id))
        cashback_percent = 0
        cashback_amount = 0
        cashback_before = 0
        cashback_after = 0

        if product_type == "bonus":
            if not _is_bonus_supported_server(server_id):
                self._send_json(400, {"error": "Bonus purchase is not available for this server"})
                return
            if not is_valid_steam_id(steam_id) or bonus_amount <= 0:
                self._send_json(400, {"error": "Invalid bonus purchase data"})
                return

            try:
                bonus_result = apply_bonus_purchase(
                    server_id=server_id,
                    steam_id=steam_id,
                    bonus_amount=bonus_amount,
                )
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                print(
                    f"[BONUS ERROR] Failed to apply bonus: {_redact_sensitive_text(error)}",
                    file=sys.stderr,
                )
                self._send_json(502, {"error": "Failed to apply bonus purchase"})
                return

            if not bonus_result:
                self._send_json(404, {"error": "Player with this STEAM_ID not found"})
                return

            if use_balance:
                try:
                    balance_before_purchase, balance_after_purchase = adjust_user_balance(
                        user_id,
                        -int(amount),
                        transaction_type="purchase",
                        metadata={
                            "product_type": "bonus",
                            "server_id": str(server_id),
                            "server_name": str(server_name),
                            "bonus_amount": int(bonus_amount),
                        },
                    )
                    balance_spent = int(amount)
                    user_balance_after_operation = int(balance_after_purchase)
                except ValueError:
                    available_balance = get_user_balance(user_id)
                    missing_amount = max(int(amount) - int(available_balance), 0)
                    self._send_json(
                        400,
                        {
                            "error": "Balansda mablag' yetarli emas" if language == "uz" else "Недостаточно средств на балансе",
                            "insufficientBalance": True,
                            "requiredAmount": int(amount),
                            "balanceAmount": int(available_balance),
                            "missingAmount": int(missing_amount),
                        },
                    )
                    return
            else:
                user_balance_after_operation = int(get_user_balance(user_id))

            notification_sent = send_bonus_confirmation_message(
                user_id=user_id,
                server_name=server_name,
                steam_id=bonus_result["steam_id"],
                nickname=bonus_result["nickname"],
                bonus_added=bonus_result["added"],
                bonus_before=bonus_result["before"],
                bonus_after=bonus_result["after"],
                amount=amount,
                language=language,
            )
            if not notification_sent:
                print(
                    f"[BONUS NOTIFY ERROR] Failed to send user message for user_id={user_id}",
                    file=sys.stderr,
                )

            purchase_record = create_purchase_record(
                user_id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                server_id=server_id,
                privilege=bonus_package_label or f"Бонусы +{bonus_result['added']}",
                server_name=server_name,
                duration="Единоразово",
                duration_months=1,
                nickname=bonus_result["nickname"],
                password="-",
                language=language,
                amount=amount,
                product_type="bonus",
                steam_id=bonus_result["steam_id"],
                bonus_added=bonus_result["added"],
                bonus_before=bonus_result["before"],
                bonus_after=bonus_result["after"],
            )
            purchase_record["payment_verification"] = dict(payment_verification)
        else:
            if (
                not privilege
                or not duration
                or duration_months <= 0
            ):
                self._send_json(400, {"error": "Missing required fields"})
                return

            if (
                identifier_type == PRIVILEGE_IDENTIFIER_STEAM
                and not _is_steam_allowed_for_privilege(privilege)
            ):
                self._send_json(
                    400,
                    {"error": "This privilege can be purchased only with NickName + Password"},
                )
                return

            if identifier_type == PRIVILEGE_IDENTIFIER_STEAM:
                if not is_valid_steam_id(steam_id):
                    self._send_json(400, {"error": "Invalid STEAM_ID format"})
                    return
                password = ""
                current_password = ""
                change_password = False
            else:
                if not nickname or not password:
                    self._send_json(400, {"error": "Missing required fields"})
                    return

            issue_result = None
            try:
                issue_result = issue_privilege_via_ftp_if_required(
                    server_id=server_id,
                    server_name=server_name,
                    privilege=privilege,
                    duration_months=duration_months,
                    identifier_type=identifier_type,
                    nickname=nickname,
                    steam_id=steam_id,
                    password=password,
                    renewal_requested=renewal_requested,
                    current_password=current_password,
                    change_password=change_password,
                    paid_amount=amount,
                )
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                print(f"[FTP ISSUE ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
                self._send_json(502, {"error": "Failed to issue privilege in users.ini"})
                return

            charged_amount = int(issue_result.get("calculated_amount", amount) or amount) if issue_result else int(amount)
            if use_balance and charged_amount > 0:
                try:
                    balance_before_purchase, balance_after_purchase = adjust_user_balance(
                        user_id,
                        -int(charged_amount),
                        transaction_type="purchase",
                        metadata={
                            "product_type": "privilege",
                            "server_id": str(server_id),
                            "server_name": str(server_name),
                            "privilege": str(privilege),
                            "duration_months": int(duration_months),
                        },
                    )
                    balance_spent = int(charged_amount)
                    user_balance_after_operation = int(balance_after_purchase)
                except ValueError:
                    available_balance = get_user_balance(user_id)
                    missing_amount = max(int(charged_amount) - int(available_balance), 0)
                    self._send_json(
                        400,
                        {
                            "error": "Balansda mablag' yetarli emas" if language == "uz" else "Недостаточно средств на балансе",
                            "insufficientBalance": True,
                            "requiredAmount": int(charged_amount),
                            "balanceAmount": int(available_balance),
                            "missingAmount": int(missing_amount),
                        },
                    )
                    return

            issued_privilege_name = str(issue_result.get("privilege", privilege)) if issue_result else str(privilege)
            issued_nickname_value = str(issue_result.get("nickname", nickname)) if issue_result else str(nickname)
            issued_steam_id_value = str(issue_result.get("steam_id", steam_id)) if issue_result else str(steam_id)

            cashback_percent = _get_privilege_cashback_percent(issued_privilege_name)
            cashback_amount = _calculate_privilege_cashback_amount(issued_privilege_name, charged_amount)
            if cashback_amount > 0:
                try:
                    cashback_before, cashback_after = adjust_user_balance(
                        user_id,
                        int(cashback_amount),
                        transaction_type="cashback",
                        metadata={
                            "product_type": "privilege",
                            "server_id": str(server_id),
                            "server_name": str(server_name),
                            "privilege": issued_privilege_name,
                            "duration_months": int(duration_months),
                            "purchase_amount": int(charged_amount),
                            "cashback_percent": int(cashback_percent),
                        },
                    )
                    if use_balance:
                        balance_after_purchase = int(cashback_after)
                    user_balance_after_operation = int(cashback_after)
                except ValueError:
                    cashback_percent = 0
                    cashback_amount = 0
                    cashback_before = 0
                    cashback_after = 0
            elif use_balance:
                user_balance_after_operation = int(balance_after_purchase)
            else:
                user_balance_after_operation = int(get_user_balance(user_id))

            sent = send_purchase_confirmation_message(
                user_id=user_id,
                privilege=issued_privilege_name,
                server_name=server_name,
                duration=duration,
                nickname=issued_nickname_value,
                steam_id=issued_steam_id_value,
                identifier_type=identifier_type,
                password=password,
                language=language,
                issue_mode=str(issue_result.get("mode", "")) if issue_result else "",
                before_days=int(issue_result.get("before_days", 0) or 0) if issue_result else 0,
                after_days=int(issue_result.get("after_days", 0) or 0) if issue_result else 0,
                password_changed=bool(issue_result.get("password_changed", False)) if issue_result else False,
                previous_privilege=str(issue_result.get("previous_privilege", "")) if issue_result else "",
                target_privilege=str(issue_result.get("target_privilege", "")) if issue_result else "",
                credit_amount=int(issue_result.get("credit_amount", 0) or 0) if issue_result else 0,
                paid_amount=charged_amount,
                cashback_amount=int(cashback_amount),
                cashback_percent=int(cashback_percent),
                balance_after=int(user_balance_after_operation),
            )

            if not sent:
                print(
                    f"[PURCHASE NOTIFY ERROR] API endpoint failed for user_id={user_id}",
                    file=sys.stderr,
                )
                self._send_json(502, {"error": "Failed to send Telegram message"})
                return

            purchase_record = create_purchase_record(
                user_id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                server_id=server_id,
                privilege=privilege,
                server_name=server_name,
                duration=duration,
                duration_months=duration_months,
                nickname=(
                    issued_nickname_value
                    if issue_result
                    else (nickname if identifier_type == PRIVILEGE_IDENTIFIER_NICKNAME else "")
                ),
                password=password,
                language=language,
                amount=amount,
                product_type="privilege",
                identifier_type=identifier_type,
                steam_id=(
                    issued_steam_id_value
                    if issue_result
                    else (steam_id if identifier_type == PRIVILEGE_IDENTIFIER_STEAM else "")
                ),
            )
            purchase_record["payment_verification"] = dict(payment_verification)
            if issue_result:
                purchase_record["issued_users_ini_path"] = str(issue_result.get("users_ini_path", ""))
                purchase_record["issued_mode"] = str(issue_result.get("mode", ""))
                purchase_record["issued_before_days"] = int(issue_result.get("before_days", 0) or 0)
                purchase_record["issued_after_days"] = int(issue_result.get("after_days", 0) or 0)
                purchase_record["issued_privilege"] = str(issue_result.get("privilege", ""))
                purchase_record["issued_flags"] = str(issue_result.get("flags", ""))
                purchase_record["issued_was_disabled"] = bool(issue_result.get("was_disabled"))
                purchase_record["issued_password_changed"] = bool(issue_result.get("password_changed"))
                purchase_record["issued_calculated_amount"] = int(issue_result.get("calculated_amount", amount) or 0)
                purchase_record["issued_credit_amount"] = int(issue_result.get("credit_amount", 0) or 0)
                purchase_record["issued_previous_privilege"] = str(issue_result.get("previous_privilege", ""))
                purchase_record["issued_target_privilege"] = str(issue_result.get("target_privilege", ""))
                purchase_record["issued_identifier_type"] = str(issue_result.get("identifier_type", identifier_type))
                purchase_record["issued_identifier_value"] = str(issue_result.get("identifier_value", ""))
            purchase_record["cashback_percent"] = int(cashback_percent)
            purchase_record["cashback_amount"] = int(cashback_amount)
            purchase_record["cashback_before"] = int(cashback_before)
            purchase_record["cashback_after"] = int(cashback_after)
            purchase_record["user_balance_after"] = int(user_balance_after_operation)

        if use_balance:
            if balance_spent <= 0:
                current_balance_value = get_user_balance(user_id)
                balance_before_purchase = int(current_balance_value)
                balance_after_purchase = int(current_balance_value)
                user_balance_after_operation = int(current_balance_value)
            purchase_record["payment_source"] = "balance"
            purchase_record["balance_before"] = int(balance_before_purchase)
            purchase_record["balance_after"] = int(balance_after_purchase)
            purchase_record["balance_spent"] = int(balance_spent)
            purchase_record["user_balance_after"] = int(user_balance_after_operation)
        else:
            purchase_record["payment_source"] = "card"
            purchase_record["user_balance_after"] = int(user_balance_after_operation)

        report_sent = False
        report_message_id = None
        if get_reports_chat_id() is not None:
            report_sent, report_message_id = send_purchase_report_to_group(
                purchase_record,
                screenshot_bytes=screenshot_bytes,
                screenshot_mime_type=screenshot_mime_type,
                screenshot_name=screenshot_name,
            )
            if report_message_id is not None:
                purchase_record["report_message_id"] = int(report_message_id)

        save_purchase_record(purchase_record)
        current_balance_value = int(get_user_balance(user_id))
        if use_balance:
            balance_payload = {
                "balance": int(balance_after_purchase),
                "spent": int(balance_spent),
                "before": int(balance_before_purchase),
                "after": int(balance_after_purchase),
                "source": "balance",
            }
        else:
            balance_payload = {
                "balance": current_balance_value,
                "spent": 0,
                "before": current_balance_value,
                "after": current_balance_value,
                "source": "card",
            }

        self._send_json(
            200,
            {
                "ok": True,
                "timestamp": int(time.time()),
                "purchaseId": purchase_record.get("id"),
                "reportSent": report_sent,
                "productType": product_type,
                "paymentVerification": {
                    "ok": bool(payment_verification.get("ok")),
                    "mode": str(payment_verification.get("mode", "")),
                },
                "balance": balance_payload,
                "cashback": (
                    {
                        "amount": int(cashback_amount),
                        "percent": int(cashback_percent),
                        "before": int(cashback_before),
                        "after": int(cashback_after),
                    }
                    if (product_type == "privilege" and int(cashback_amount) > 0)
                    else None
                ),
                "bonusResult": (
                    {
                        "steamId": purchase_record.get("steam_id", ""),
                        "nickname": purchase_record.get("nickname", ""),
                        "added": int(purchase_record.get("bonus_added", 0) or 0),
                        "before": int(purchase_record.get("bonus_before", 0) or 0),
                        "after": int(purchase_record.get("bonus_after", 0) or 0),
                        "database": "",
                    }
                    if product_type == "bonus"
                    else None
                ),
            },
        )

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query_params = parse_qs(parsed.query)

        if path == "/api/health":
            self._send_json(200, {"ok": True, "timestamp": int(time.time())})
            return

        if path == "/api/balance":
            try:
                user_id = int(str(query_params.get("userId", ["0"])[0]).strip())
            except (TypeError, ValueError):
                user_id = 0
            if user_id <= 0:
                self._send_json(400, {"error": "userId is required"})
                return

            balance_snapshot = get_user_balance_snapshot(user_id)
            self._send_json(
                200,
                {
                    "ok": True,
                    "balance": int(balance_snapshot.get("balance", 0) or 0),
                    "updatedAt": int(balance_snapshot.get("updated_at", 0) or 0),
                    "timestamp": int(time.time()),
                },
            )
            return

        if path == "/api/balance-history":
            try:
                user_id = int(str(query_params.get("userId", ["0"])[0]).strip())
            except (TypeError, ValueError):
                user_id = 0
            if user_id <= 0:
                self._send_json(400, {"error": "userId is required"})
                return

            try:
                limit = int(str(query_params.get("limit", ["120"])[0]).strip())
            except (TypeError, ValueError):
                limit = 120

            history = get_user_balance_transactions(user_id, limit=limit)
            items = []
            for transaction in history:
                meta = transaction.get("meta", {})
                items.append(
                    {
                        "id": str(transaction.get("id", "")).strip(),
                        "createdAt": int(transaction.get("created_at", 0) or 0),
                        "type": str(transaction.get("type", "")).strip().lower(),
                        "delta": int(transaction.get("delta", 0) or 0),
                        "before": int(transaction.get("before", 0) or 0),
                        "after": int(transaction.get("after", 0) or 0),
                        "meta": dict(meta) if isinstance(meta, dict) else {},
                    }
                )

            self._send_json(
                200,
                {
                    "ok": True,
                    "items": items,
                    "total": len(items),
                    "timestamp": int(time.time()),
                },
            )
            return

        if path == "/api/user-privileges":
            try:
                user_id = int(str(query_params.get("userId", ["0"])[0]).strip())
            except (TypeError, ValueError):
                user_id = 0
            if user_id <= 0:
                self._send_json(400, {"error": "userId is required"})
                return

            try:
                limit = int(str(query_params.get("limit", ["30"])[0]).strip())
            except (TypeError, ValueError):
                limit = 30

            snapshots = get_user_privilege_snapshots(user_id, limit=limit)
            self._send_json(
                200,
                {
                    "ok": True,
                    "items": [
                        {
                            "id": str(item.get("id", "")).strip(),
                            "createdAt": int(item.get("created_at", 0) or 0),
                            "serverId": str(item.get("server_id", "")).strip(),
                            "serverName": str(item.get("server_name", "")).strip(),
                            "privilegeKey": str(item.get("privilege_key", "")).strip(),
                            "privilegeLabel": str(item.get("privilege_label", "")).strip(),
                            "identifierType": str(item.get("identifier_type", PRIVILEGE_IDENTIFIER_NICKNAME)),
                            "nickname": str(item.get("nickname", "")).strip(),
                            "steamId": str(item.get("steam_id", "")).strip(),
                            "remainingDays": int(item.get("remaining_days", 0) or 0),
                            "totalDays": int(item.get("total_days", 0) or 0),
                            "daysPassed": int(item.get("days_passed", 0) or 0),
                            "canRenew": bool(item.get("can_renew")),
                        }
                        for item in snapshots
                    ],
                    "total": len(snapshots),
                    "timestamp": int(time.time()),
                },
            )
            return

        if path == "/api/payment-status":
            try:
                user_id = int(str(query_params.get("userId", ["0"])[0]).strip())
            except (TypeError, ValueError):
                user_id = 0
            payment_session_id = str(query_params.get("paymentSessionId", [""])[0]).strip()
            if user_id <= 0:
                self._send_json(400, {"error": "userId is required"})
                return

            violation = get_user_payment_violation_status(
                user_id,
                payment_session_id=payment_session_id,
            )
            self._send_json(
                200,
                {
                    "ok": True,
                    "status": violation,
                    "uploadSessionSeconds": PAYMENT_UPLOAD_SESSION_SECONDS,
                    "timestamp": int(time.time()),
                },
            )
            return

        if path == "/api/bonus-account":
            server_id = str(query_params.get("serverId", [""])[0]).strip()
            steam_id = normalize_steam_id(query_params.get("steamId", [""])[0])
            if not server_id or not steam_id:
                self._send_json(400, {"error": "serverId and steamId are required"})
                return
            if not _is_known_server(server_id):
                self._send_json(400, {"error": "Unknown server"})
                return
            if not _is_bonus_supported_server(server_id):
                self._send_json(400, {"error": "Bonus lookup is not available for this server"})
                return

            if not is_valid_steam_id(steam_id):
                self._send_json(400, {"error": "Invalid STEAM_ID format"})
                return

            try:
                account = fetch_bonus_account(server_id, steam_id)
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                print(f"[BONUS LOOKUP ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
                self._send_json(502, {"error": "Failed to fetch bonus account"})
                return

            if not account:
                self._send_json(404, {"error": "Player with this STEAM_ID not found"})
                return

            self._send_json(
                200,
                {
                    "ok": True,
                    "account": {
                        "steamId": account["steam_id"],
                        "nickname": account["nickname"],
                        "bonusCount": int(account["bonus_count"]),
                        "database": "",
                    },
                    "timestamp": int(time.time()),
                },
            )
            return

        if path == "/api/privilege-account":
            server_id = str(query_params.get("serverId", [""])[0]).strip()
            server_name = str(query_params.get("serverName", [""])[0]).strip()
            identifier_type = normalize_privilege_identifier_type(
                query_params.get("identifierType", ["nickname"])[0]
            )
            nickname = str(query_params.get("nickname", [""])[0]).strip()
            steam_id = normalize_steam_id(query_params.get("steamId", [""])[0])
            if not server_id:
                self._send_json(400, {"error": "serverId is required"})
                return
            if not _is_known_server(server_id):
                self._send_json(400, {"error": "Unknown server"})
                return
            if identifier_type == PRIVILEGE_IDENTIFIER_STEAM:
                if not steam_id:
                    self._send_json(400, {"error": "serverId and steamId are required"})
                    return
            elif not nickname:
                self._send_json(400, {"error": "serverId and nickname are required"})
                return

            try:
                account = _extract_privilege_account_from_users_ini(
                    server_id=server_id,
                    server_name=server_name,
                    identifier_type=identifier_type,
                    nickname=nickname,
                    steam_id=steam_id,
                )
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return
            except Exception as error:
                print(f"[PRIVILEGE LOOKUP ERROR] {_redact_sensitive_text(error)}", file=sys.stderr)
                self._send_json(502, {"error": "Failed to lookup privilege account"})
                return

            self._send_json(
                200,
                {
                    "ok": True,
                    "account": {
                        "supported": bool(account.get("supported")),
                        "exists": bool(account.get("exists")),
                        "identifierType": str(account.get("identifier_type", identifier_type)),
                        "nickname": str(account.get("nickname", "")),
                        "steamId": str(account.get("steam_id", "")),
                        "password": str(account.get("password", "")),
                        "privilege": str(account.get("privilege", "")),
                        "flags": str(account.get("flags", "")),
                        "days": int(account.get("days", 0) or 0),
                        "isPermanent": bool(account.get("is_permanent")),
                        "isDisabled": bool(account.get("is_disabled")),
                        "isExpired": bool(account.get("is_expired")),
                    },
                    "timestamp": int(time.time()),
                },
            )
            return

        if path == "/api/servers":
            servers = get_servers_sync()
            self._send_json(200, {
                "servers": servers,
                "total": len(servers),
                "timestamp": int(time.time()),
            })
            return

        server_match = re.match(r"^/api/servers/(\d+)$", path)
        if server_match:
            port = int(server_match.group(1))
            if port not in KNOWN_PORTS:
                self._send_json(404, {"error": "Unknown server port"})
                return

            info = get_server_info(port)
            server_payload = {
                "id": str(port),
                "port": port,
                "name": info["name"],
                "players": info["players"],
                "max": info["max"],
                "maxPlayers": info["max"],
                "map": info["map"],
                "ip": f"{BASE_IP}:{port}",
                "status": "online" if (info["max"] or info["players"]) else "offline",
            }
            self._send_json(200, {"server": server_payload, "timestamp": int(time.time())})
            return

        players_match = re.match(r"^/api/servers/(\d+)/players$", path)
        if players_match:
            port = int(players_match.group(1))
            if port not in KNOWN_PORTS:
                self._send_json(404, {"error": "Unknown server port"})
                return

            info = get_server_info(port)
            players = get_players(port)
            server_payload = {
                "id": str(port),
                "port": port,
                "name": info["name"],
                "players": info["players"],
                "max": info["max"],
                "maxPlayers": info["max"],
                "map": info["map"],
                "ip": f"{BASE_IP}:{port}",
                "status": "online" if (info["max"] or info["players"]) else "offline",
            }
            self._send_json(200, {
                "server": server_payload,
                "players": players,
                "timestamp": int(time.time()),
            })
            return

        self._send_json(404, {"error": "Not found"})

    def log_message(self, format, *args):
        return


def start_api_server():
    try:
        httpd = ThreadingHTTPServer((API_HOST, API_PORT), MiniAppAPIHandler)
    except OSError as e:
        print(
            f"[API ERROR] Could not start API on {API_HOST}:{API_PORT} -> {_redact_sensitive_text(e)}",
            file=sys.stderr,
        )
        return

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    print(f"MiniApp API started at http://{API_HOST}:{API_PORT}")

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text

    if text == "🌐 Servers":
        await server(update, context)

    elif text == "📱 App":
        await miniapp(update, context)

    elif text == "🎮 Start CS":
        await message.reply_text(
            "🎮 Раздел /startcs будет добавлен следующим шагом 😉",
            reply_markup=build_main_keyboard()
        )


def build_application():
    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .connect_timeout(20)
        .read_timeout(20)
        .write_timeout(20)
        .pool_timeout(20)
        .post_init(configure_chat_menu_button)
        .build()
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("server", server))
    application.add_handler(CommandHandler("info", info))
    application.add_handler(CommandHandler("vip", vip))
    application.add_handler(CommandHandler("miniapp", miniapp))
    application.add_handler(CommandHandler("bind_reports", bind_reports_command))

    application.add_handler(MessageHandler(filters.ChatType.GROUPS, group_reports_autobind))

    application.add_handler(
        MessageHandler(filters.TEXT & filters.Regex("^👥 Players$"), players_button)
    )
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler))

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, buttons))
    application.add_handler(CommandHandler("players", players))
    application.add_handler(
        CallbackQueryHandler(players_category_callback, pattern="^players_category:")
    )
    application.add_handler(
        CallbackQueryHandler(players_server_callback, pattern="^players_server:")
    )
    application.add_handler(
        CallbackQueryHandler(
            cancel_purchase_prompt_callback,
            pattern="^cancel_purchase_prompt:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            cancel_purchase_prompt_callback,
            pattern="^cancel_purchase:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            cancel_purchase_confirm_callback,
            pattern="^cancel_purchase_confirm:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            cancel_purchase_abort_callback,
            pattern="^cancel_purchase_abort:",
        )
    )
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))
    return application


def run_telegram_bot_forever():
    retry_delay = int(os.getenv("BOT_RETRY_DELAY", "5"))

    while True:
        app = build_application()
        try:
            print("Бот запущен и ждёт команды...")
            app.run_polling()
            return
        except KeyboardInterrupt:
            return
        except Exception as e:
            print(f"[BOT ERROR] {_redact_sensitive_text(e)}", file=sys.stderr)
            print(f"[BOT] Retrying in {retry_delay} seconds...", file=sys.stderr)
            time.sleep(retry_delay)


def main():
    start_api_server()

    run_telegram = os.getenv("RUN_TELEGRAM_BOT", "1").strip().lower() not in {"0", "false", "no"}
    if not run_telegram:
        print("RUN_TELEGRAM_BOT is disabled. API-only mode is active.")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            return

    if not TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")

    reports_scheduler = threading.Thread(
        target=run_reports_scheduler_forever,
        daemon=True,
        name="reports-scheduler",
    )
    reports_scheduler.start()
    print("Reports scheduler started (Asia/Tashkent: 21:00 daily, Sunday 21:01 weekly, month-end 21:02 monthly).")

    run_telegram_bot_forever()


if __name__ == "__main__":
    main()

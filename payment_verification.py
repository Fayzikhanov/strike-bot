import base64
import asyncio
import json
import os
import re
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

try:
    from telethon.sessions import StringSession
    from telethon.sync import TelegramClient
except Exception:  # pragma: no cover - optional dependency at import time
    StringSession = None
    TelegramClient = None

try:
    import certifi
except Exception:  # pragma: no cover - optional dependency at import time
    certifi = None


AMOUNT_PATTERN = re.compile(r"(?<!\d)(\d{1,3}(?:[ \u00A0.,']\d{3})+|\d{4,9})(?!\d)")
BALANCE_PATTERN = re.compile(
    r"(?:баланс|остаток|доступно|available|💰|💵)[^0-9]{0,24}(\d[\d \u00A0.,']{2,})",
    re.IGNORECASE,
)
CARD_LAST4_PATTERN = re.compile(r"(?:\*{1,6}|x{1,6}|X{1,6}|•{1,6})\s*(\d{4})(?!\d)")
CARD_LINE_LAST4_PATTERN = re.compile(
    r"(?:💳|card|карта)[^\n]{0,48}?(?:\*{1,6}|x{1,6}|X{1,6}|•{1,6})\s*(\d{4})(?!\d)",
    re.IGNORECASE,
)
TRANSFER_KEYWORDS = (
    "пополн",
    "зачисл",
    "поступ",
    "перевод",
    "perevod",
    "otkazma",
    "perechislen",
    "перечислен",
    "credited",
    "incoming",
    "payment",
    "p2p",
)
BALANCE_KEYWORDS = (
    "баланс",
    "остаток",
    "доступно",
    "available",
)
MONEY_AMOUNT_HINTS = (
    "uzs",
    "сум",
    "sum",
    "som",
    "so'm",
    "soʻm",
    "сўм",
)
MONEY_EMOJIS = ("💵", "💸", "💰", "💴", "💶", "💷")
DEFAULT_TIMEZONE = ZoneInfo("Asia/Tashkent")


def _to_bool(raw_value, default=False):
    text = str(raw_value if raw_value is not None else "").strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "true", "yes", "on"}


def _to_int(raw_value, default=0):
    try:
        return int(str(raw_value).strip())
    except (TypeError, ValueError):
        return int(default)


def _to_float(raw_value, default=0.0):
    try:
        return float(str(raw_value).strip())
    except (TypeError, ValueError):
        return float(default)


def _normalize_confidence_01(raw_value):
    value = _to_float(raw_value, default=0.0)
    if value > 1.0:
        if value <= 100.0:
            value = value / 100.0
        else:
            value = 1.0
    return max(min(float(value), 1.0), 0.0)


def _normalize_username(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return "@CardXabarBot"
    if not text.startswith("@"):
        return f"@{text}"
    return text


def _split_csv(raw_value):
    values = []
    for piece in str(raw_value or "").split(","):
        token = piece.strip()
        if token:
            values.append(token)
    return values


def _normalize_amount_text(raw_amount):
    text = str(raw_amount or "").strip()
    if not text:
        return None

    normalized = text.replace("\u00A0", " ").replace(" ", "")
    normalized = re.sub(r"[^0-9,.\-+]", "", normalized)
    normalized = normalized.lstrip("+")
    if normalized.startswith("-"):
        normalized = normalized[1:]
    if not normalized:
        return None

    thousand_sep = ""
    decimal_sep = ""
    has_comma = "," in normalized
    has_dot = "." in normalized
    if has_comma and has_dot:
        if normalized.rfind(",") > normalized.rfind("."):
            decimal_sep = ","
            thousand_sep = "."
        else:
            decimal_sep = "."
            thousand_sep = ","
    elif has_comma:
        if re.search(r",\d{1,2}$", normalized):
            decimal_sep = ","
        else:
            thousand_sep = ","
    elif has_dot:
        if re.search(r"\.\d{1,2}$", normalized):
            decimal_sep = "."
        else:
            thousand_sep = "."

    amount_text = normalized
    if thousand_sep:
        amount_text = amount_text.replace(thousand_sep, "")
    if decimal_sep:
        parts = amount_text.split(decimal_sep)
        if len(parts) == 2:
            integer_part = re.sub(r"[^\d]", "", parts[0])
            if not integer_part:
                return None
            try:
                value = int(integer_part)
            except (TypeError, ValueError):
                return None
        else:
            digits = re.sub(r"[^\d]", "", amount_text)
            if not digits:
                return None
            try:
                value = int(digits)
            except (TypeError, ValueError):
                return None
    else:
        digits = re.sub(r"[^\d]", "", amount_text)
        if not digits:
            return None
        try:
            value = int(digits)
        except (TypeError, ValueError):
            return None

    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    if value < 1 or value > 500_000_000:
        return None
    return value


def _extract_amount_candidates(raw_text):
    text = str(raw_text or "")
    found = []
    seen = set()
    for match in AMOUNT_PATTERN.finditer(text):
        value = _normalize_amount_text(match.group(1))
        if value is None or value in seen:
            continue
        seen.add(value)
        found.append(value)
    return found


def _extract_balance_amount(raw_text):
    text = str(raw_text or "")
    for line in text.splitlines():
        lowered = line.casefold()
        if "💰" in line or "💵" in line or any(keyword in lowered for keyword in BALANCE_KEYWORDS):
            amounts = _extract_amount_candidates(line)
            if amounts:
                return max(amounts)

    balance_match = BALANCE_PATTERN.search(text)
    if balance_match:
        value = _normalize_amount_text(balance_match.group(1))
        if value is not None:
            return value

    # Do NOT fall back to extracting any number from the text.
    # "Подождите..." and other service messages may contain numbers (operation IDs, etc.)
    # that would be falsely treated as the card balance.
    return None


def _extract_transfer_amount(raw_text):
    text = str(raw_text or "")
    for line in text.splitlines():
        row = line.strip()
        if not row:
            continue
        if row.startswith("+") or row.startswith("➕") or "➕" in row:
            amounts = _extract_amount_candidates(row)
            if amounts:
                return amounts[0]
    return None


def _extract_message_card_last4(raw_text):
    text = str(raw_text or "")
    if not text:
        return ""

    line_match = CARD_LINE_LAST4_PATTERN.search(text)
    if line_match:
        return str(line_match.group(1))

    generic_match = CARD_LAST4_PATTERN.search(text)
    if generic_match:
        return str(generic_match.group(1))

    return ""


def _safe_json_loads(raw_text):
    if isinstance(raw_text, (dict, list)):
        return raw_text
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return {}
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            return {}


def _parse_ocr_datetime(raw_value):
    raw_text = str(raw_value or "").strip()
    text = raw_text
    if not text:
        return None
    normalized_iso = text.replace("Z", "+00:00")
    try:
        parsed_iso = datetime.fromisoformat(normalized_iso)
        if parsed_iso.tzinfo is None:
            return parsed_iso.replace(tzinfo=DEFAULT_TIMEZONE)
        return parsed_iso.astimezone(DEFAULT_TIMEZONE)
    except ValueError:
        pass
    text = text.replace("T", " ").replace("/", ".")
    text = re.sub(r"\s+", " ", text)

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d.%m.%y %H:%M:%S",
        "%d.%m.%y %H:%M",
        "%d.%m.%y",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=DEFAULT_TIMEZONE)
        return parsed.astimezone(DEFAULT_TIMEZONE)
    return None


def _extract_transfer_datetime(raw_text):
    text = str(raw_text or "")
    if not text:
        return None

    formats = (
        "%H:%M %d.%m.%y",
        "%H:%M:%S %d.%m.%y",
        "%d.%m.%y %H:%M",
        "%d.%m.%y %H:%M:%S",
        "%H:%M %d.%m.%Y",
        "%H:%M:%S %d.%m.%Y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    )
    patterns = (
        re.compile(r"(\d{1,2}:\d{2}(?::\d{2})?)\s+(\d{2}[./]\d{2}[./](?:\d{2}|\d{4}))"),
        re.compile(r"(\d{2}[./]\d{2}[./](?:\d{2}|\d{4}))\s+(\d{1,2}:\d{2}(?::\d{2})?)"),
    )

    for line in text.splitlines():
        row = str(line or "").strip()
        if not row:
            continue
        row = row.replace("/", ".")
        for pattern in patterns:
            match = pattern.search(row)
            if not match:
                continue
            left, right = match.group(1), match.group(2)
            candidates = (
                f"{left} {right}",
                f"{right} {left}",
            )
            for candidate in candidates:
                for fmt in formats:
                    try:
                        parsed = datetime.strptime(candidate, fmt)
                    except ValueError:
                        continue
                    return parsed.replace(tzinfo=DEFAULT_TIMEZONE)
    return None


@dataclass
class PaymentVerificationConfig:
    required: bool
    openai_api_key: str
    openai_model: str
    amount_tolerance: int
    recipient_card_last4: list
    recipient_names: list
    recipient_card_hints: list
    telegram_api_id: int
    telegram_api_hash: str
    telegram_session_string: str
    telegram_session_file: str
    card_bot_username: str
    card_bot_balance_command: str
    card_bot_message_limit: int
    card_bot_lookback_hours: int
    card_bot_wait_seconds: float
    ocr_transfer_time_tolerance_minutes: int
    transfer_recent_without_ocr_minutes: int
    transfer_max_message_age_minutes: int
    session_time_grace_seconds: int
    screenshot_max_age_hours: int
    balance_state_path: str
    consumed_state_path: str
    session_balance_state_path: str
    confidence_accept_min: float
    confidence_manual_min: float

    @property
    def recipient_hints_ready(self):
        return bool(self.recipient_card_last4 or self.recipient_names or self.recipient_card_hints)

    @classmethod
    def from_env(cls):
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)
        legacy_time_tolerance_hours = max(
            _to_int(os.getenv("PAYMENT_TRANSFER_TIME_TOLERANCE_HOURS", "0"), default=0),
            0,
        )
        ocr_tolerance_default_minutes = legacy_time_tolerance_hours * 60 if legacy_time_tolerance_hours > 0 else 15
        return cls(
            required=_to_bool(os.getenv("PAYMENT_VERIFICATION_REQUIRED", "1"), default=True),
            openai_api_key=str(os.getenv("OPENAI_API_KEY", "")).strip(),
            openai_model=str(os.getenv("PAYMENT_OPENAI_MODEL", "gpt-4o-mini")).strip() or "gpt-4o-mini",
            amount_tolerance=max(_to_int(os.getenv("PAYMENT_AMOUNT_TOLERANCE", "0"), default=0), 0),
            recipient_card_last4=_split_csv(os.getenv("PAYMENT_RECIPIENT_CARD_LAST4", "")),
            recipient_names=_split_csv(os.getenv("PAYMENT_RECIPIENT_NAMES", "")),
            recipient_card_hints=_split_csv(os.getenv("PAYMENT_RECIPIENT_CARD_HINTS", "")),
            telegram_api_id=_to_int(os.getenv("TELEGRAM_APP_API_ID", "0"), default=0),
            telegram_api_hash=str(os.getenv("TELEGRAM_APP_API_HASH", "")).strip(),
            telegram_session_string=str(os.getenv("TELEGRAM_SESSION_STRING", "")).strip(),
            telegram_session_file=str(
                os.getenv(
                    "TELEGRAM_SESSION_FILE",
                    os.path.join(data_dir, "cardxabar_telegram.session"),
                )
            ).strip()
            or os.path.join(data_dir, "cardxabar_telegram.session"),
            card_bot_username=_normalize_username(
                os.getenv(
                    "CARD_BOT_USERNAME",
                    os.getenv("HUMO_BOT_USERNAME", "@CardXabarBot"),
                )
            ),
            card_bot_balance_command=str(
                os.getenv(
                    "CARD_BOT_BALANCE_COMMAND",
                    os.getenv("HUMO_BALANCE_COMMAND", "💰 Баланс карты"),
                )
            ).strip()
            or "💰 Баланс карты",
            card_bot_message_limit=max(
                _to_int(
                    os.getenv("CARD_BOT_MESSAGE_LIMIT", os.getenv("HUMO_MESSAGE_LIMIT", "60")),
                    default=60,
                ),
                20,
            ),
            card_bot_lookback_hours=max(
                _to_int(
                    os.getenv("CARD_BOT_LOOKBACK_HOURS", os.getenv("HUMO_LOOKBACK_HOURS", "24")),
                    default=24,
                ),
                1,
            ),
            card_bot_wait_seconds=max(
                _to_float(
                    os.getenv(
                        "CARD_BOT_BALANCE_WAIT_SECONDS",
                        os.getenv("HUMO_BALANCE_WAIT_SECONDS", "5"),
                    ),
                    default=5.0,
                ),
                2.0,
            ),
            ocr_transfer_time_tolerance_minutes=max(
                _to_int(
                    os.getenv(
                        "PAYMENT_OCR_TRANSFER_TIME_TOLERANCE_MINUTES",
                        str(ocr_tolerance_default_minutes),
                    ),
                    default=ocr_tolerance_default_minutes,
                ),
                1,
            ),
            transfer_recent_without_ocr_minutes=max(
                _to_int(os.getenv("PAYMENT_TRANSFER_RECENT_WINDOW_MINUTES", "20"), default=20),
                1,
            ),
            transfer_max_message_age_minutes=max(
                _to_int(os.getenv("PAYMENT_TRANSFER_MAX_MESSAGE_AGE_MINUTES", "30"), default=30),
                1,
            ),
            session_time_grace_seconds=max(
                _to_int(os.getenv("PAYMENT_SESSION_TIME_GRACE_SECONDS", "120"), default=120),
                0,
            ),
            screenshot_max_age_hours=max(
                _to_int(os.getenv("PAYMENT_SCREENSHOT_MAX_AGE_HOURS", "72"), default=72),
                1,
            ),
            balance_state_path=str(
                os.getenv(
                    "CARD_BOT_BALANCE_STATE_PATH",
                    os.getenv("HUMO_BALANCE_STATE_PATH", os.path.join(data_dir, "cardxabar_balance_state.json")),
                )
            ).strip()
            or os.path.join(data_dir, "cardxabar_balance_state.json"),
            consumed_state_path=str(
                os.getenv(
                    "CARD_BOT_CONSUMED_STATE_PATH",
                    os.getenv(
                        "HUMO_CONSUMED_STATE_PATH",
                        os.path.join(data_dir, "cardxabar_consumed_payments.json"),
                    ),
                )
            ).strip()
            or os.path.join(data_dir, "cardxabar_consumed_payments.json"),
            session_balance_state_path=str(
                os.getenv(
                    "PAYMENT_SESSION_BALANCE_STATE_PATH",
                    os.path.join(data_dir, "cardxabar_session_balances.json"),
                )
            ).strip()
            or os.path.join(data_dir, "cardxabar_session_balances.json"),
            confidence_accept_min=max(
                min(_to_float(os.getenv("PAYMENT_CONFIDENCE_ACCEPT_MIN", "0.82"), default=0.82), 1.0),
                0.0,
            ),
            confidence_manual_min=max(
                min(_to_float(os.getenv("PAYMENT_CONFIDENCE_MANUAL_MIN", "0.55"), default=0.55), 1.0),
                0.0,
            ),
        )


class PaymentVerificationService:
    def __init__(self, config=None):
        self.config = config or PaymentVerificationConfig.from_env()
        if self.config.confidence_manual_min > self.config.confidence_accept_min:
            self.config.confidence_manual_min = self.config.confidence_accept_min
        self._lock = threading.Lock()

    def verify_payment(
        self,
        screenshot_bytes,
        screenshot_mime_type,
        expected_amount,
        *,
        payment_key="",
        payment_context=None,
        session_started_at=0,
        session_expires_at=0,
    ):
        amount_value = _to_int(expected_amount, default=0)
        result = {
            "ok": False,
            "reason": "Оплата не подтверждена",
            "timestamp": int(time.time()),
            "expected_amount": amount_value,
            "decision": "REJECT",
            "confidence": 0.0,
        }

        if not self.config.required:
            result["ok"] = True
            result["mode"] = "verification_disabled"
            result["reason"] = ""
            return result

        if amount_value <= 0:
            result["ok"] = True
            result["mode"] = "zero_amount"
            result["reason"] = ""
            result["decision"] = "ACCEPT"
            result["confidence"] = 1.0
            return result

        if not self.config.openai_api_key:
            result["reason"] = "Проверка оплаты недоступна: не настроен OPENAI_API_KEY"
            return result

        if not self.config.recipient_hints_ready:
            result["reason"] = (
                "Проверка оплаты недоступна: укажите PAYMENT_RECIPIENT_CARD_LAST4 "
                "или PAYMENT_RECIPIENT_NAMES/PAYMENT_RECIPIENT_CARD_HINTS"
            )
            return result

        if TelegramClient is None or StringSession is None:
            result["reason"] = "Проверка оплаты недоступна: установите зависимость telethon"
            return result

        if self.config.telegram_api_id <= 0 or not self.config.telegram_api_hash:
            result["reason"] = "Проверка оплаты недоступна: не настроены TELEGRAM_APP_API_ID/TELEGRAM_APP_API_HASH"
            return result

        try:
            with self._lock:
                session_ts = float(session_started_at or 0)
                ocr = self._analyze_screenshot_with_openai(
                    screenshot_bytes=screenshot_bytes,
                    screenshot_mime_type=screenshot_mime_type,
                    expected_amount=amount_value,
                )
                result["ocr"] = ocr
                session_expires_ts = float(session_expires_at or 0)
                is_valid, reason = self._validate_ocr_result(
                    ocr,
                    amount_value,
                    session_started_at=session_ts,
                    session_expires_at=session_expires_ts,
                )
                if not is_valid:
                    result["reason"] = reason
                    result["decision"] = "REJECT"
                    return result

                card_bot_result = self._verify_with_card_bot(
                    ocr,
                    amount_value,
                    payment_key=payment_key,
                    payment_context=payment_context,
                    session_started_at=session_ts,
                    session_expires_at=session_expires_ts,
                )
                if payment_key and isinstance(card_bot_result, dict):
                    card_bot_result["payment_key"] = str(payment_key).strip()
                result["card_bot"] = card_bot_result
                result["humo"] = card_bot_result
                result["decision"] = str(card_bot_result.get("decision", "REJECT")).strip().upper() or "REJECT"
                result["confidence"] = float(card_bot_result.get("confidence", 0) or 0)
                if not card_bot_result.get("ok"):
                    result["reason"] = str(card_bot_result.get("reason", "Оплата не найдена в @CardXabarBot"))
                    return result
        except Exception as error:
            result["reason"] = f"Проверка оплаты завершилась ошибкой: {error}"
            return result

        result["ok"] = True
        result["mode"] = str(result.get("card_bot", {}).get("mode", "unknown"))
        result["reason"] = ""
        result["decision"] = "ACCEPT"
        result["confidence"] = float(result.get("card_bot", {}).get("confidence", 0.95) or 0.95)
        return result

    def verify_topup(
        self,
        screenshot_bytes,
        screenshot_mime_type,
        *,
        payment_key="",
        payment_context=None,
        session_started_at=0,
        session_expires_at=0,
    ):
        result = {
            "ok": False,
            "reason": "Пополнение не подтверждено",
            "timestamp": int(time.time()),
            "credited_amount": 0,
            "decision": "REJECT",
            "confidence": 0.0,
        }

        if not self.config.required:
            result["ok"] = True
            result["mode"] = "verification_disabled"
            result["reason"] = ""
            return result

        if not self.config.openai_api_key:
            result["reason"] = "Проверка оплаты недоступна: не настроен OPENAI_API_KEY"
            return result

        if not self.config.recipient_hints_ready:
            result["reason"] = (
                "Проверка оплаты недоступна: укажите PAYMENT_RECIPIENT_CARD_LAST4 "
                "или PAYMENT_RECIPIENT_NAMES/PAYMENT_RECIPIENT_CARD_HINTS"
            )
            return result

        if TelegramClient is None or StringSession is None:
            result["reason"] = "Проверка оплаты недоступна: установите зависимость telethon"
            return result

        if self.config.telegram_api_id <= 0 or not self.config.telegram_api_hash:
            result["reason"] = "Проверка оплаты недоступна: не настроены TELEGRAM_APP_API_ID/TELEGRAM_APP_API_HASH"
            return result

        try:
            with self._lock:
                session_ts = float(session_started_at or 0)
                session_expires_ts = float(session_expires_at or 0)
                ocr = self._analyze_screenshot_with_openai(
                    screenshot_bytes=screenshot_bytes,
                    screenshot_mime_type=screenshot_mime_type,
                    expected_amount=0,
                )
                result["ocr"] = ocr

                is_valid, reason, detected_amount = self._validate_topup_ocr_result(
                    ocr,
                    session_started_at=session_ts,
                    session_expires_at=session_expires_ts,
                )
                if not is_valid:
                    result["reason"] = reason
                    result["decision"] = "REJECT"
                    return result

                card_bot_result = self._verify_with_card_bot(
                    ocr,
                    detected_amount,
                    payment_key=payment_key,
                    payment_context=payment_context,
                    session_started_at=session_ts,
                    session_expires_at=session_expires_ts,
                )
                if payment_key and isinstance(card_bot_result, dict):
                    card_bot_result["payment_key"] = str(payment_key).strip()
                result["card_bot"] = card_bot_result
                result["humo"] = card_bot_result
                result["decision"] = str(card_bot_result.get("decision", "REJECT")).strip().upper() or "REJECT"
                result["confidence"] = float(card_bot_result.get("confidence", 0) or 0)
                if not card_bot_result.get("ok"):
                    result["reason"] = str(card_bot_result.get("reason", "Оплата не найдена в @CardXabarBot"))
                    return result

                result["credited_amount"] = int(detected_amount)
        except Exception as error:
            result["reason"] = f"Проверка оплаты завершилась ошибкой: {error}"
            return result

        result["ok"] = True
        result["mode"] = str(result.get("card_bot", {}).get("mode", "unknown"))
        result["reason"] = ""
        result["decision"] = "ACCEPT"
        result["confidence"] = float(result.get("card_bot", {}).get("confidence", 0.95) or 0.95)
        return result

    def prime_balance_session(
        self,
        *,
        session_id,
        user_id=0,
        flow="",
        session_started_at=0,
        session_expires_at=0,
    ):
        safe_session_id = str(session_id or "").strip()
        safe_flow = str(flow or "").strip() or "unknown"
        safe_user_id = _to_int(user_id, default=0)
        result = {
            "ok": False,
            "session_id": safe_session_id,
            "flow": safe_flow,
            "user_id": safe_user_id,
            "timestamp": int(time.time()),
        }

        if not safe_session_id:
            result["reason"] = "Не удалось подготовить сессию: отсутствует session_id"
            return result

        if TelegramClient is None or StringSession is None:
            result["reason"] = "Проверка оплаты недоступна: установите зависимость telethon"
            return result

        if self.config.telegram_api_id <= 0 or not self.config.telegram_api_hash:
            result["reason"] = "Проверка оплаты недоступна: не настроены TELEGRAM_APP_API_ID/TELEGRAM_APP_API_HASH"
            return result

        target_card_last4 = self._select_target_card_last4()
        if not target_card_last4:
            result["reason"] = "Проверка оплаты недоступна: укажите PAYMENT_RECIPIENT_CARD_LAST4"
            return result

        existing_record = self._get_session_balance_record(safe_session_id)
        existing_pre_balance = (
            existing_record.get("pre_balance")
            if isinstance(existing_record, dict)
            else None
        )
        if existing_pre_balance is not None:
            result["ok"] = True
            result["target_card_last4"] = target_card_last4
            result["pre_balance"] = int(existing_pre_balance)
            result["reason"] = ""
            return result

        try:
            with self._lock:
                now_local = datetime.now(DEFAULT_TIMEZONE)
                since_local = now_local - timedelta(hours=self.config.card_bot_lookback_hours)
                with self._open_card_bot_client() as client_info:
                    client, card_bot_entity = client_info
                    pre_balance, capture_meta = self._capture_current_balance(
                        client=client,
                        card_bot_entity=card_bot_entity,
                        since_local=since_local,
                        target_card_last4=target_card_last4,
                    )

                if pre_balance is None:
                    result["reason"] = "Не удалось получить стартовый баланс карты"
                    return result

                self._save_session_balance_record(
                    session_id=safe_session_id,
                    user_id=safe_user_id,
                    flow=safe_flow,
                    session_started_at=_to_int(session_started_at, default=0),
                    session_expires_at=_to_int(session_expires_at, default=0),
                    pre_balance=int(pre_balance),
                    pre_balance_captured_at=int(time.time()),
                )

                self._write_balance_state(int(pre_balance))
                result["ok"] = True
                result["target_card_last4"] = target_card_last4
                result["pre_balance"] = int(pre_balance)
                if isinstance(capture_meta, dict):
                    result["capture"] = capture_meta
                result["reason"] = ""
                return result
        except Exception as error:
            result["reason"] = f"Не удалось подготовить сессию проверки: {error}"
            return result

    def _analyze_screenshot_with_openai(self, *, screenshot_bytes, screenshot_mime_type, expected_amount):
        encoded_image = base64.b64encode(bytes(screenshot_bytes)).decode("ascii")
        mime_type = str(screenshot_mime_type or "image/jpeg").strip() or "image/jpeg"

        recipient_hints = {
            "cardLast4": list(self.config.recipient_card_last4),
            "cardHints": list(self.config.recipient_card_hints),
            "recipientNames": list(self.config.recipient_names),
        }
        prompt = (
            "Проверь, является ли изображение реальным скриншотом перевода денег на карту. "
            "Определи сумму перевода, дату/время (если видны), получателя и признаки подделки. "
            "Ответ строго JSON объектом без markdown с полями: "
            "isPaymentProof (bool), detectedAmountUzs (number|null), detectedDateTime (string|null), "
            "detectedDateRaw (string|null), recipientText (string), recipientMatched (bool), "
            "cardLast4 (string|null), confidence (number), suspiciousFlags (array of strings), notes (string). "
            "recipientMatched=true только если на скриншоте явно видно совпадение с подсказками получателя."
        )
        user_text = (
            f"Ожидаемая сумма платежа: {int(expected_amount)} UZS.\n"
            f"Подсказки получателя: {json.dumps(recipient_hints, ensure_ascii=False)}"
        )
        payload = {
            "model": self.config.openai_model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_text},
                        {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{encoded_image}"}},
                    ],
                },
            ],
        }

        response = self._post_json(
            "https://api.openai.com/v1/chat/completions",
            payload,
            headers={
                "Authorization": f"Bearer {self.config.openai_api_key}",
                "Content-Type": "application/json",
            },
            timeout=90,
        )
        choices = response.get("choices", []) if isinstance(response, dict) else []
        content = ""
        if choices:
            content = str(choices[0].get("message", {}).get("content", "")).strip()
        parsed = _safe_json_loads(content)
        if not isinstance(parsed, dict):
            parsed = {}

        parsed_amount = _normalize_amount_text(parsed.get("detectedAmountUzs"))
        parsed_datetime = _parse_ocr_datetime(parsed.get("detectedDateTime") or parsed.get("detectedDateRaw"))
        return {
            "is_payment_proof": bool(parsed.get("isPaymentProof", False)),
            "detected_amount_uzs": parsed_amount,
            "detected_date_time": parsed_datetime.isoformat() if parsed_datetime else "",
            "detected_date_raw": str(parsed.get("detectedDateRaw", "")).strip(),
            "recipient_text": str(parsed.get("recipientText", "")).strip(),
            "recipient_matched": bool(parsed.get("recipientMatched", False)),
            "card_last4": str(parsed.get("cardLast4", "")).strip(),
            "confidence": float(parsed.get("confidence", 0) or 0),
            "suspicious_flags": parsed.get("suspiciousFlags", []),
            "notes": str(parsed.get("notes", "")).strip(),
        }

    def _validate_ocr_result(self, ocr, expected_amount, *, session_started_at=0, session_expires_at=0):
        if not bool(ocr.get("is_payment_proof")):
            return False, "Оплата не подтверждена: на скриншоте не найден перевод"

        detected_amount = _to_int(ocr.get("detected_amount_uzs"), default=0)
        if detected_amount <= 0:
            return False, "Оплата не подтверждена: не удалось определить сумму перевода"

        if abs(detected_amount - int(expected_amount)) > self.config.amount_tolerance:
            return (
                False,
                f"Оплата не подтверждена: сумма на скриншоте ({detected_amount}) "
                f"не совпадает с ожидаемой ({int(expected_amount)})",
            )

        if not bool(ocr.get("recipient_matched")):
            return False, "Оплата не подтверждена: получатель на скриншоте не совпадает с вашей картой"

        # Screenshot datetime is advisory only.
        # Real anti-fraud decision is made against @CardXabarBot transfer/balance data
        # tied to the current payment session window.
        _ = _parse_ocr_datetime(ocr.get("detected_date_time"))
        _ = session_started_at
        _ = session_expires_at

        return True, ""

    def _validate_topup_ocr_result(self, ocr, *, session_started_at=0, session_expires_at=0):
        if not bool(ocr.get("is_payment_proof")):
            return False, "Пополнение не подтверждено: на скриншоте не найден перевод", 0

        detected_amount = _to_int(ocr.get("detected_amount_uzs"), default=0)
        if detected_amount <= 0:
            return False, "Пополнение не подтверждено: не удалось определить сумму перевода", 0

        if not bool(ocr.get("recipient_matched")):
            return False, "Пополнение не подтверждено: получатель на скриншоте не совпадает с вашей картой", 0

        _ = _parse_ocr_datetime(ocr.get("detected_date_time"))
        _ = session_started_at
        _ = session_expires_at
        return True, "", int(detected_amount)

    def _verify_with_card_bot(
        self,
        ocr,
        expected_amount,
        *,
        payment_key="",
        payment_context=None,
        session_started_at=0,
        session_expires_at=0,
    ):
        now_local = datetime.now(DEFAULT_TIMEZONE)
        since_local = now_local - timedelta(hours=self.config.card_bot_lookback_hours)
        ocr_datetime = _parse_ocr_datetime(ocr.get("detected_date_time"))
        consumed_state = self._read_consumed_state()
        self._cleanup_consumed_state(consumed_state)
        target_card_last4 = self._select_target_card_last4()
        if not target_card_last4:
            return {
                "ok": False,
                "mode": "setup_error",
                "decision": "REJECT",
                "confidence": 0.0,
                "reason": "Проверка оплаты недоступна: не задана целевая карта (PAYMENT_RECIPIENT_CARD_LAST4)",
            }

        context = payment_context if isinstance(payment_context, dict) else {}
        session_id = str(context.get("session_id", "")).strip()
        flow = str(context.get("flow", "")).strip()
        user_id = _to_int(context.get("user_id"), default=0)
        key_context = str(payment_key or session_id).strip()
        ocr_confidence = _normalize_confidence_01(ocr.get("confidence"))

        with self._open_card_bot_client() as client_info:
            client, card_bot_entity = client_info
            messages_before = self._fetch_card_bot_messages(
                client=client,
                card_bot_entity=card_bot_entity,
                since_local=since_local,
                limit=self.config.card_bot_message_limit,
            )
            transfer_match = self._find_transfer_match(
                messages=messages_before,
                expected_amount=expected_amount,
                ocr_datetime=ocr_datetime,
                consumed_state=consumed_state,
                target_card_last4=target_card_last4,
                session_started_at=session_started_at,
                session_expires_at=session_expires_at,
            )
            if transfer_match:
                message_balance = transfer_match.get("balance_after")
                if message_balance is not None:
                    self._write_balance_state(message_balance)
                    if session_id:
                        self._save_session_balance_record(
                            session_id=session_id,
                            user_id=user_id,
                            flow=flow or "unknown",
                            session_started_at=_to_int(session_started_at, default=0),
                            session_expires_at=_to_int(session_expires_at, default=0),
                            post_balance=int(message_balance),
                            post_balance_captured_at=int(time.time()),
                        )

                confidence = 0.85 + (0.08 * ocr_confidence)
                if ocr_datetime is not None:
                    delta_seconds = transfer_match.get("ocr_delta_seconds")
                    if delta_seconds is not None:
                        if delta_seconds <= (self.config.ocr_transfer_time_tolerance_minutes * 60):
                            confidence += 0.06
                        else:
                            confidence -= 0.10
                if bool(ocr.get("recipient_matched")):
                    confidence += 0.02
                confidence = max(min(confidence, 0.99), 0.0)

                decision = self._classify_decision(confidence, prefer_manual=True)
                if decision == "ACCEPT":
                    self._consume_transfer_message(
                        consumed_state=consumed_state,
                        transfer_match=transfer_match,
                    )
                    self._write_consumed_state(consumed_state)
                result = {
                    "ok": decision == "ACCEPT",
                    "mode": "transfer_message",
                    "decision": decision,
                    "confidence": confidence,
                    "matched_message_id": transfer_match.get("id"),
                    "matched_amount": transfer_match.get("amount"),
                    "matched_at": transfer_match.get("date"),
                    "matched_card_last4": transfer_match.get("card_last4"),
                    "source_bot": self.config.card_bot_username,
                    "signals": {
                        "ocr_datetime_present": bool(ocr_datetime),
                        "transfer_match": True,
                        "balance_fallback_used": False,
                    },
                }
                if decision != "ACCEPT":
                    result["reason"] = (
                        "Пополнение требует ручной проверки: недостаточная уверенность автоматической сверки."
                    )
                return result

            pre_balance = None
            pre_balance_source = "session_state"
            if session_id:
                session_record = self._get_session_balance_record(session_id)
                if isinstance(session_record, dict):
                    pre_balance = session_record.get("pre_balance")
            if pre_balance is None:
                pre_balance_source = "missing"
                return {
                    "ok": False,
                    "mode": "balance_diff",
                    "decision": "MANUAL_REVIEW",
                    "confidence": 0.45,
                    "reason": (
                        "Пополнение не найдено в @CardXabarBot и нет стартового баланса сессии. "
                        "Требуется ручная проверка."
                    ),
                    "source_bot": self.config.card_bot_username,
                    "signals": {
                        "transfer_match": False,
                        "balance_fallback_used": True,
                        "pre_balance_source": pre_balance_source,
                    },
                }

            post_balance, capture_meta = self._capture_current_balance(
                client=client,
                card_bot_entity=card_bot_entity,
                since_local=since_local,
                target_card_last4=target_card_last4,
            )
            if post_balance is None:
                return {
                    "ok": False,
                    "mode": "balance_diff",
                    "decision": "MANUAL_REVIEW",
                    "confidence": 0.45,
                    "reason": (
                        "Пополнение не найдено в @CardXabarBot, а текущий баланс карты получить не удалось. "
                        "Требуется ручная проверка."
                    ),
                    "source_bot": self.config.card_bot_username,
                    "signals": {
                        "transfer_match": False,
                        "balance_fallback_used": True,
                        "pre_balance_source": pre_balance_source,
                    },
                }

            if session_id:
                self._save_session_balance_record(
                    session_id=session_id,
                    user_id=user_id,
                    flow=flow or "unknown",
                    session_started_at=_to_int(session_started_at, default=0),
                    session_expires_at=_to_int(session_expires_at, default=0),
                    post_balance=int(post_balance),
                    post_balance_captured_at=int(time.time()),
                )
            self._write_balance_state(int(post_balance))

            difference = int(post_balance) - int(pre_balance)
            if abs(difference - int(expected_amount)) <= self.config.amount_tolerance:
                if self._is_balance_diff_consumed(
                    consumed_state=consumed_state,
                    previous_balance=int(pre_balance),
                    current_balance=int(post_balance),
                    expected_amount=int(expected_amount),
                    key_context=key_context,
                ):
                    return {
                        "ok": False,
                        "mode": "balance_diff",
                        "decision": "REJECT",
                        "confidence": 0.20,
                        "reason": "Оплата уже привязана к другой покупке и не может быть использована повторно",
                        "previous_balance": int(pre_balance),
                        "current_balance": int(post_balance),
                        "difference": int(difference),
                        "source_bot": self.config.card_bot_username,
                    }

                confidence = 0.80 + (0.06 * ocr_confidence)
                if ocr_datetime is not None:
                    confidence += 0.04
                else:
                    confidence += 0.02
                if bool(ocr.get("recipient_matched")):
                    confidence += 0.03
                if bool(ocr.get("is_payment_proof")):
                    confidence += 0.03
                confidence = max(min(confidence, 0.97), 0.0)
                decision = self._classify_decision(confidence, prefer_manual=True)
                if decision == "ACCEPT":
                    self._consume_balance_diff(
                        consumed_state=consumed_state,
                        previous_balance=int(pre_balance),
                        current_balance=int(post_balance),
                        expected_amount=int(expected_amount),
                        difference=int(difference),
                        key_context=key_context,
                    )
                    self._write_consumed_state(consumed_state)
                result = {
                    "ok": decision == "ACCEPT",
                    "mode": "balance_diff",
                    "decision": decision,
                    "confidence": confidence,
                    "previous_balance": int(pre_balance),
                    "current_balance": int(post_balance),
                    "difference": int(difference),
                    "source_bot": self.config.card_bot_username,
                    "signals": {
                        "transfer_match": False,
                        "balance_fallback_used": True,
                        "pre_balance_source": pre_balance_source,
                    },
                }
                if isinstance(capture_meta, dict):
                    result["capture"] = capture_meta
                if decision != "ACCEPT":
                    result["reason"] = (
                        "Пополнение обнаружено по изменению баланса, но уверенность ниже порога. "
                        "Требуется ручная проверка."
                    )
                return result

            if difference == 0:
                return {
                    "ok": False,
                    "mode": "balance_diff",
                    "decision": "MANUAL_REVIEW",
                    "confidence": 0.40,
                    "reason": (
                        "Пополнение не найдено в @CardXabarBot и баланс карты не изменился. "
                        "Требуется ручная проверка."
                    ),
                    "previous_balance": int(pre_balance),
                    "current_balance": int(post_balance),
                    "difference": int(difference),
                    "source_bot": self.config.card_bot_username,
                    "signals": {
                        "transfer_match": False,
                        "balance_fallback_used": True,
                        "pre_balance_source": pre_balance_source,
                    },
                }

            return {
                "ok": False,
                "mode": "balance_diff",
                "decision": "REJECT",
                "confidence": 0.15,
                "reason": (
                    f"Оплата не подтверждена: разница баланса ({difference}) "
                    f"не равна сумме платежа ({int(expected_amount)})"
                ),
                "previous_balance": int(pre_balance),
                "current_balance": int(post_balance),
                "difference": int(difference),
                "source_bot": self.config.card_bot_username,
                "signals": {
                    "transfer_match": False,
                    "balance_fallback_used": True,
                    "pre_balance_source": pre_balance_source,
                },
            }

    def _verify_with_humo(
        self,
        ocr,
        expected_amount,
        *,
        payment_key="",
        payment_context=None,
        session_started_at=0,
        session_expires_at=0,
    ):
        # Backward-compatible alias for legacy integrations.
        return self._verify_with_card_bot(
            ocr,
            expected_amount,
            payment_key=payment_key,
            payment_context=payment_context,
            session_started_at=session_started_at,
            session_expires_at=session_expires_at,
        )

    def _classify_decision(self, confidence, *, prefer_manual=False):
        safe_confidence = max(min(float(confidence or 0), 1.0), 0.0)
        if safe_confidence >= float(self.config.confidence_accept_min):
            return "ACCEPT"
        if safe_confidence >= float(self.config.confidence_manual_min):
            return "MANUAL_REVIEW"
        if prefer_manual:
            return "MANUAL_REVIEW"
        return "REJECT"

    def _find_transfer_match(
        self,
        *,
        messages,
        expected_amount,
        ocr_datetime,
        consumed_state,
        target_card_last4="",
        session_started_at=0,
        session_expires_at=0,
    ):
        now_local = datetime.now(DEFAULT_TIMEZONE)
        session_start_dt = (
            datetime.fromtimestamp(float(session_started_at), tz=DEFAULT_TIMEZONE)
            if session_started_at > 0
            else None
        )
        session_end_dt = (
            datetime.fromtimestamp(float(session_expires_at), tz=DEFAULT_TIMEZONE)
            if session_expires_at > session_started_at > 0
            else None
        )
        session_grace = timedelta(seconds=self.config.session_time_grace_seconds)
        for item in messages:
            text = str(item.get("text", ""))
            lowered = text.casefold()
            if not any(keyword in lowered for keyword in TRANSFER_KEYWORDS):
                continue

            detected_card_last4 = _extract_message_card_last4(text)
            if target_card_last4:
                if not detected_card_last4:
                    continue
                if str(detected_card_last4) != str(target_card_last4):
                    continue

            amount_value = _extract_transfer_amount(text)
            if amount_value is None:
                amounts = _extract_amount_candidates(text)
                if not amounts:
                    continue
                amount_value = amounts[0]
                if any(keyword in lowered for keyword in BALANCE_KEYWORDS) and len(amounts) >= 2:
                    amount_value = min(amounts)

            if abs(int(amount_value) - int(expected_amount)) > self.config.amount_tolerance:
                continue

            message_id = int(item.get("id", 0) or 0)
            if message_id <= 0:
                continue
            if self._is_transfer_message_consumed(consumed_state, message_id):
                continue

            message_datetime = _parse_ocr_datetime(item.get("date"))
            transfer_datetime = _extract_transfer_datetime(text) or message_datetime
            # Rule: Card bot message must always be recent relative to NOW (1 payment = 1 privilege).
            # This prevents old payments (made by other users) from being matched to new purchases.
            max_age_seconds = self.config.transfer_max_message_age_minutes * 60
            if transfer_datetime:
                age_from_now = (now_local - transfer_datetime).total_seconds()
                if age_from_now > max_age_seconds:
                    continue
                # KEY RULE: payment must have been made AFTER the 5-minute payment session started.
                # CardXabar reports minute-level timestamps, so allow small negative skew.
                # This prevents dropping valid transfers done in the same minute as session start.
                if session_start_dt is not None and transfer_datetime < (session_start_dt - session_grace):
                    continue
                # Payment must belong to this payment session window (with small clock tolerance).
                if session_end_dt is not None and transfer_datetime > (session_end_dt + session_grace):
                    continue
                # Additional check: if OCR timestamp present, message date must be close to it.
                if ocr_datetime:
                    delta_seconds = abs((transfer_datetime - ocr_datetime).total_seconds())
                    max_delta = self.config.ocr_transfer_time_tolerance_minutes * 60
                    # OCR datetime is advisory. Do not hard-reject the transfer here,
                    # because some apps/systems can output partial or shifted timestamps.
                else:
                    # If OCR has no timestamp, allow only very recent transfers.
                    recent_window = self.config.transfer_recent_without_ocr_minutes * 60
                    if age_from_now > recent_window:
                        continue
            elif ocr_datetime:
                # No message datetime, but OCR timestamp present: OCR time must be recent.
                age_from_now = (now_local - ocr_datetime).total_seconds()
                if age_from_now > max_age_seconds:
                    continue
            else:
                # No reliable timestamps => skip to avoid false matches.
                continue

            return {
                "id": message_id,
                "date": transfer_datetime.isoformat() if transfer_datetime else str(item.get("date", "")),
                "amount": int(amount_value),
                "card_last4": str(detected_card_last4 or ""),
                "ocr_delta_seconds": (
                    abs((transfer_datetime - ocr_datetime).total_seconds())
                    if (transfer_datetime is not None and ocr_datetime is not None)
                    else None
                ),
                "balance_after": _extract_balance_amount(text),
                "text": text[:200],
            }
        return None

    @staticmethod
    def _transfer_message_key(message_id):
        return str(int(message_id))

    @staticmethod
    def _balance_diff_key(previous_balance, current_balance, expected_amount, key_context=""):
        context_token = str(key_context or "").strip()
        base = f"{int(previous_balance)}:{int(current_balance)}:{int(expected_amount)}"
        if context_token:
            return f"{context_token}:{base}"
        return base

    def _read_consumed_state(self):
        path = self.config.consumed_state_path
        try:
            with open(path, "r", encoding="utf-8") as source:
                payload = json.load(source)
        except Exception:
            payload = {}

        if not isinstance(payload, dict):
            payload = {}

        transfer = payload.get("used_transfer_messages", {})
        balance = payload.get("used_balance_diffs", {})
        return {
            "used_transfer_messages": transfer if isinstance(transfer, dict) else {},
            "used_balance_diffs": balance if isinstance(balance, dict) else {},
        }

    def _cleanup_consumed_state(self, consumed_state):
        now_ts = int(time.time())
        transfer_records = consumed_state.get("used_transfer_messages", {})
        if isinstance(transfer_records, dict):
            stale_transfer = []
            for key, value in transfer_records.items():
                if not isinstance(value, dict):
                    stale_transfer.append(key)
                    continue
                created_at = _to_int(value.get("created_at"), default=0)
                if created_at > 0 and (now_ts - created_at) > (self.config.card_bot_lookback_hours * 3600 + 3600):
                    stale_transfer.append(key)
            for key in stale_transfer:
                transfer_records.pop(key, None)

        balance_records = consumed_state.get("used_balance_diffs", {})
        if isinstance(balance_records, dict):
            stale_balance = []
            for key, value in balance_records.items():
                if not isinstance(value, dict):
                    stale_balance.append(key)
                    continue
                created_at = _to_int(value.get("created_at"), default=0)
                if created_at > 0 and (now_ts - created_at) > (self.config.card_bot_lookback_hours * 3600 + 3600):
                    stale_balance.append(key)
            for key in stale_balance:
                balance_records.pop(key, None)

    def _write_consumed_state(self, consumed_state):
        path = self.config.consumed_state_path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = {
            "used_transfer_messages": consumed_state.get("used_transfer_messages", {}),
            "used_balance_diffs": consumed_state.get("used_balance_diffs", {}),
            "updated_at": datetime.now(DEFAULT_TIMEZONE).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as target:
            json.dump(payload, target, ensure_ascii=False, indent=2)

    def _is_transfer_message_consumed(self, consumed_state, message_id):
        transfer_records = consumed_state.get("used_transfer_messages", {})
        if not isinstance(transfer_records, dict):
            return False
        record = transfer_records.get(self._transfer_message_key(message_id))
        return isinstance(record, dict)

    def _consume_transfer_message(self, *, consumed_state, transfer_match):
        transfer_records = consumed_state.setdefault("used_transfer_messages", {})
        if not isinstance(transfer_records, dict):
            transfer_records = {}
            consumed_state["used_transfer_messages"] = transfer_records
        key = self._transfer_message_key(transfer_match.get("id", 0))
        transfer_records[key] = {
            "created_at": int(time.time()),
            "message_id": int(transfer_match.get("id", 0) or 0),
            "amount": int(transfer_match.get("amount", 0) or 0),
            "matched_at": str(transfer_match.get("date", "")).strip(),
        }

    def _is_balance_diff_consumed(
        self,
        *,
        consumed_state,
        previous_balance,
        current_balance,
        expected_amount,
        key_context="",
    ):
        balance_records = consumed_state.get("used_balance_diffs", {})
        if not isinstance(balance_records, dict):
            return False
        key = self._balance_diff_key(previous_balance, current_balance, expected_amount, key_context)
        record = balance_records.get(key)
        return isinstance(record, dict)

    def _consume_balance_diff(
        self,
        *,
        consumed_state,
        previous_balance,
        current_balance,
        expected_amount,
        difference,
        key_context="",
    ):
        balance_records = consumed_state.setdefault("used_balance_diffs", {})
        if not isinstance(balance_records, dict):
            balance_records = {}
            consumed_state["used_balance_diffs"] = balance_records
        key = self._balance_diff_key(previous_balance, current_balance, expected_amount, key_context)
        balance_records[key] = {
            "created_at": int(time.time()),
            "previous_balance": int(previous_balance),
            "current_balance": int(current_balance),
            "expected_amount": int(expected_amount),
            "difference": int(difference),
        }

    def _fetch_card_bot_messages(self, *, client, card_bot_entity, since_local, limit):
        records = []
        for message in client.iter_messages(card_bot_entity, limit=limit):
            message_id = int(getattr(message, "id", 0) or 0)
            raw_text = getattr(message, "raw_text", None)
            if raw_text is None:
                raw_text = getattr(message, "message", "")
            text = str(raw_text or "").strip()
            if not text:
                continue
            date_value = getattr(message, "date", None)
            if not date_value:
                continue
            local_date = date_value.astimezone(DEFAULT_TIMEZONE)
            if local_date < since_local:
                break
            records.append(
                {
                    "id": message_id,
                    "date": local_date.isoformat(),
                    "text": text,
                }
            )
        return records

    def _fetch_humo_messages(self, *, client, humo_entity, since_local, limit):
        # Backward-compatible alias.
        return self._fetch_card_bot_messages(
            client=client,
            card_bot_entity=humo_entity,
            since_local=since_local,
            limit=limit,
        )

    @staticmethod
    def _extract_card_balances_from_snapshot(raw_text):
        text = str(raw_text or "")
        balances = {}
        pending_cards = []
        for line in text.splitlines():
            row = str(line or "").strip()
            if not row:
                continue
            lowered = row.casefold()

            card_matches = CARD_LAST4_PATTERN.findall(row)
            if card_matches:
                pending_cards = [str(card) for card in card_matches]

            if not pending_cards:
                continue

            if "копировать" in lowered or "copy" in lowered:
                continue

            amounts = _extract_amount_candidates(row)
            if not amounts:
                continue
            amount = int(max(amounts))
            has_money_hint = any(token in lowered for token in MONEY_AMOUNT_HINTS) or any(
                icon in row for icon in MONEY_EMOJIS
            )
            if not has_money_hint:
                continue

            if pending_cards:
                for card in pending_cards:
                    balances[str(card)] = amount
                pending_cards = []

        return balances

    def _extract_message_balance(self, raw_text, *, snapshot_only=False, target_card_last4=None):
        text = str(raw_text or "")

        snapshot_balances = self._extract_card_balances_from_snapshot(text)
        if snapshot_balances:
            if target_card_last4:
                target_value = snapshot_balances.get(str(target_card_last4))
                if target_value is not None:
                    return int(target_value)
                # Snapshot exists, but target card is absent in this message.
                # Do not fallback to another card balance.
                return None
            return int(max(snapshot_balances.values()))

        if snapshot_only:
            return None

        if target_card_last4:
            # For target-card mode, avoid accidental fallback to total/other-card values.
            return None

        fallback_balance = _extract_balance_amount(text)
        if fallback_balance is None:
            return None
        return int(fallback_balance)

    def _extract_latest_balance(self, messages, *, snapshot_only=False, target_card_last4=None):
        for item in messages:
            amount = self._extract_message_balance(
                item.get("text", ""),
                snapshot_only=snapshot_only,
                target_card_last4=target_card_last4,
            )
            if amount is not None:
                return int(amount)
        return None

    def _capture_current_balance(self, *, client, card_bot_entity, since_local, target_card_last4):
        messages_before = self._fetch_card_bot_messages(
            client=client,
            card_bot_entity=card_bot_entity,
            since_local=since_local,
            limit=self.config.card_bot_message_limit,
        )
        before_top_id = max((item.get("id", 0) for item in messages_before), default=0)
        client.send_message(card_bot_entity, self.config.card_bot_balance_command)

        max_poll_seconds = max(self.config.card_bot_wait_seconds * 3, 15.0)
        poll_interval = 1.5
        elapsed = 0.0
        while elapsed < max_poll_seconds:
            time.sleep(poll_interval)
            elapsed += poll_interval
            messages_after = self._fetch_card_bot_messages(
                client=client,
                card_bot_entity=card_bot_entity,
                since_local=since_local,
                limit=self.config.card_bot_message_limit,
            )
            new_messages = [item for item in messages_after if int(item.get("id", 0)) > before_top_id]
            if not new_messages:
                continue
            balance_amount = self._extract_latest_balance(
                new_messages,
                snapshot_only=True,
                target_card_last4=target_card_last4,
            )
            if balance_amount is None:
                balance_amount = self._extract_latest_balance(
                    new_messages,
                    snapshot_only=False,
                    target_card_last4=target_card_last4,
                )
            if balance_amount is None:
                continue
            newest_message = new_messages[0] if new_messages else {}
            return int(balance_amount), {
                "message_id": int(newest_message.get("id", 0) or 0),
                "captured_at": str(newest_message.get("date", "")).strip(),
            }
        return None, {}

    def _read_session_balance_state(self):
        path = self.config.session_balance_state_path
        try:
            with open(path, "r", encoding="utf-8") as source:
                payload = json.load(source)
        except Exception:
            payload = {}

        if not isinstance(payload, dict):
            payload = {}
        sessions = payload.get("sessions", {})
        if not isinstance(sessions, dict):
            sessions = {}
        return {
            "sessions": sessions,
            "updated_at": str(payload.get("updated_at", "")).strip(),
        }

    def _write_session_balance_state(self, state):
        path = self.config.session_balance_state_path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = {
            "sessions": state.get("sessions", {}) if isinstance(state, dict) else {},
            "updated_at": datetime.now(DEFAULT_TIMEZONE).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as target:
            json.dump(payload, target, ensure_ascii=False, indent=2)

    def _cleanup_session_balance_state(self, state):
        if not isinstance(state, dict):
            return
        sessions = state.get("sessions", {})
        if not isinstance(sessions, dict):
            state["sessions"] = {}
            return

        now_ts = int(time.time())
        stale_keys = []
        max_ttl = max(self.config.card_bot_lookback_hours * 3600 + 6 * 3600, 12 * 3600)
        for session_id, record in sessions.items():
            if not isinstance(record, dict):
                stale_keys.append(session_id)
                continue
            expires_at = _to_int(record.get("session_expires_at"), default=0)
            updated_at = _to_int(record.get("updated_at"), default=0)
            if expires_at > 0 and now_ts > (expires_at + 6 * 3600):
                stale_keys.append(session_id)
                continue
            if updated_at > 0 and (now_ts - updated_at) > max_ttl:
                stale_keys.append(session_id)
        for session_id in stale_keys:
            sessions.pop(session_id, None)

    def _save_session_balance_record(
        self,
        *,
        session_id,
        user_id=0,
        flow="",
        session_started_at=0,
        session_expires_at=0,
        pre_balance=None,
        pre_balance_captured_at=0,
        post_balance=None,
        post_balance_captured_at=0,
    ):
        safe_session_id = str(session_id or "").strip()
        if not safe_session_id:
            return
        state = self._read_session_balance_state()
        self._cleanup_session_balance_state(state)
        sessions = state.setdefault("sessions", {})
        if not isinstance(sessions, dict):
            sessions = {}
            state["sessions"] = sessions

        existing = sessions.get(safe_session_id, {})
        if not isinstance(existing, dict):
            existing = {}
        record = dict(existing)
        record["session_id"] = safe_session_id
        record["user_id"] = _to_int(user_id, default=0)
        record["flow"] = str(flow or "").strip() or str(record.get("flow", "")).strip() or "unknown"
        record["session_started_at"] = _to_int(
            session_started_at if session_started_at else record.get("session_started_at", 0),
            default=0,
        )
        record["session_expires_at"] = _to_int(
            session_expires_at if session_expires_at else record.get("session_expires_at", 0),
            default=0,
        )

        if pre_balance is not None:
            if record.get("pre_balance") is None:
                record["pre_balance"] = _to_int(pre_balance, default=0)
                record["pre_balance_captured_at"] = _to_int(pre_balance_captured_at, default=int(time.time()))
        elif "pre_balance" not in record:
            record["pre_balance"] = None

        if post_balance is not None:
            record["post_balance"] = _to_int(post_balance, default=0)
            record["post_balance_captured_at"] = _to_int(post_balance_captured_at, default=int(time.time()))
        elif "post_balance" not in record:
            record["post_balance"] = None

        record["updated_at"] = int(time.time())
        sessions[safe_session_id] = record
        self._write_session_balance_state(state)

    def _get_session_balance_record(self, session_id):
        safe_session_id = str(session_id or "").strip()
        if not safe_session_id:
            return {}
        state = self._read_session_balance_state()
        self._cleanup_session_balance_state(state)
        sessions = state.get("sessions", {})
        if not isinstance(sessions, dict):
            return {}
        return sessions.get(safe_session_id, {}) if isinstance(sessions.get(safe_session_id), dict) else {}

    def _select_target_card_last4(self):
        for value in list(self.config.recipient_card_last4 or []):
            token = str(value or "").strip()
            if not token:
                continue
            match = re.search(r"(\d{4})", token)
            if match:
                return match.group(1)
        return None

    def _get_previous_balance_from_transfer_messages(self, messages):
        """Return the post-transfer balance from the most recent TRANSFER message (💰 line).

        Transfer messages (пополнение, зачисление, etc.) embed the card balance
        after the transaction on the 💰 line.  This is the most reliable source
        for previous_balance because balance-check responses (💵) always show the
        *current* balance and would give the wrong (already-updated) value.
        Messages are assumed to be sorted newest-first.
        """
        for item in messages:
            text = str(item.get("text", ""))
            lowered = text.casefold()
            if not any(keyword in lowered for keyword in TRANSFER_KEYWORDS):
                continue
            for line in text.splitlines():
                if "💰" in line:
                    amounts = _extract_amount_candidates(line)
                    if amounts:
                        return max(amounts)
        return None

    def _open_card_bot_client(self):
        created_loop = None

        def _close_created_loop():
            if created_loop is None:
                return
            try:
                asyncio.set_event_loop(None)
            except Exception:
                pass
            created_loop.close()

        # Always use a dedicated loop for this sync Telethon flow.
        # ThreadingHTTPServer handles requests in worker threads that may have no loop.
        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)
        created_loop = event_loop

        if self.config.telegram_session_string:
            session = StringSession(self.config.telegram_session_string)
        else:
            session_path = self.config.telegram_session_file
            parent = os.path.dirname(session_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            session = session_path

        client = TelegramClient(
            session,
            self.config.telegram_api_id,
            self.config.telegram_api_hash,
            loop=event_loop,
        )
        try:
            client.connect()
            if not client.is_user_authorized():
                raise RuntimeError(
                    "Telegram session не авторизована. Сначала войдите в аккаунт и сохраните TELEGRAM_SESSION_STRING"
                )

            username = self.config.card_bot_username.lstrip("@")
            entity = client.get_entity(self.config.card_bot_username)
            entity_username = str(getattr(entity, "username", "") or "").strip().lower()
            if entity_username != username.lower():
                raise RuntimeError("Ошибка доступа: разрешён только чат @CardXabarBot")
        except Exception:
            try:
                client.disconnect()
            finally:
                _close_created_loop()
            raise

        class _ClientContext:
            def __init__(self, bound_client, bound_entity, bound_loop):
                self.bound_client = bound_client
                self.bound_entity = bound_entity
                self.bound_loop = bound_loop

            def __enter__(self):
                return self.bound_client, self.bound_entity

            def __exit__(self, exc_type, exc, exc_tb):
                try:
                    self.bound_client.disconnect()
                finally:
                    if self.bound_loop is not None:
                        _close_created_loop()
                    return False

        return _ClientContext(client, entity, created_loop)

    def _open_humo_client(self):
        # Backward-compatible alias.
        return self._open_card_bot_client()

    def _read_balance_state(self):
        path = self.config.balance_state_path
        try:
            with open(path, "r", encoding="utf-8") as source:
                payload = json.load(source)
        except Exception:
            return {"last_balance": None, "updated_at": ""}

        if not isinstance(payload, dict):
            return {"last_balance": None, "updated_at": ""}

        return {
            "last_balance": _to_int(payload.get("last_balance"), default=0)
            if str(payload.get("last_balance", "")).strip()
            else None,
            "updated_at": str(payload.get("updated_at", "")).strip(),
        }

    def _write_balance_state(self, balance_value):
        path = self.config.balance_state_path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = {
            "last_balance": int(balance_value),
            "updated_at": datetime.now(DEFAULT_TIMEZONE).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as target:
            json.dump(payload, target, ensure_ascii=False, indent=2)

    @staticmethod
    def _post_json(url, payload, *, headers, timeout):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(url=url, data=body, method="POST")
        for key, value in headers.items():
            request.add_header(key, value)

        ssl_context = None
        if certifi is not None:
            try:
                ssl_context = ssl.create_default_context(cafile=certifi.where())
            except Exception:
                ssl_context = None

        try:
            if ssl_context is not None:
                response_context = urlopen(request, timeout=timeout, context=ssl_context)
            else:
                response_context = urlopen(request, timeout=timeout)
            with response_context as response:
                raw = response.read()
        except HTTPError as error:
            try:
                detail = error.read().decode("utf-8", errors="ignore").strip()
            except Exception:
                detail = str(error)
            raise RuntimeError(f"HTTP {error.code}: {detail[:300]}") from error
        except URLError as error:
            raise RuntimeError(f"Network error: {error}") from error

        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as error:
            raise RuntimeError("Invalid JSON from external API") from error

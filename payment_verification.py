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
CARD_LAST4_PATTERN = re.compile(r"\*(\d{4})(?!\d)")
TRANSFER_KEYWORDS = (
    "пополн",
    "зачисл",
    "поступ",
    "перевод",
    "перечислен",
    "credited",
    "incoming",
    "payment",
)
BALANCE_KEYWORDS = (
    "баланс",
    "остаток",
    "доступно",
    "available",
)
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


def _normalize_username(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return "@HUMOcardbot"
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
        "%H:%M %d.%m.%Y",
        "%H:%M:%S %d.%m.%Y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    )
    patterns = (
        re.compile(r"(\d{1,2}:\d{2}(?::\d{2})?)\s+(\d{2}[./]\d{2}[./]\d{4})"),
        re.compile(r"(\d{2}[./]\d{2}[./]\d{4})\s+(\d{1,2}:\d{2}(?::\d{2})?)"),
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
    humo_username: str
    humo_balance_command: str
    humo_message_limit: int
    humo_lookback_hours: int
    humo_wait_seconds: float
    transfer_time_tolerance_hours: int
    transfer_recent_without_ocr_minutes: int
    transfer_max_message_age_minutes: int
    session_time_grace_seconds: int
    screenshot_max_age_hours: int
    balance_state_path: str
    consumed_state_path: str

    @property
    def recipient_hints_ready(self):
        return bool(self.recipient_card_last4 or self.recipient_names or self.recipient_card_hints)

    @classmethod
    def from_env(cls):
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(data_dir, exist_ok=True)
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
                os.getenv("TELEGRAM_SESSION_FILE", os.path.join(data_dir, "humo_telegram.session"))
            ).strip()
            or os.path.join(data_dir, "humo_telegram.session"),
            humo_username=_normalize_username(os.getenv("HUMO_BOT_USERNAME", "@HUMOcardbot")),
            humo_balance_command=str(os.getenv("HUMO_BALANCE_COMMAND", "💰 Баланс")).strip() or "💰 Баланс",
            humo_message_limit=max(_to_int(os.getenv("HUMO_MESSAGE_LIMIT", "60"), default=60), 20),
            humo_lookback_hours=max(_to_int(os.getenv("HUMO_LOOKBACK_HOURS", "24"), default=24), 1),
            humo_wait_seconds=max(float(str(os.getenv("HUMO_BALANCE_WAIT_SECONDS", "5")).strip() or "5"), 2.0),
            transfer_time_tolerance_hours=max(
                _to_int(os.getenv("PAYMENT_TRANSFER_TIME_TOLERANCE_HOURS", "36"), default=36),
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
                os.getenv("HUMO_BALANCE_STATE_PATH", os.path.join(data_dir, "humo_balance_state.json"))
            ).strip()
            or os.path.join(data_dir, "humo_balance_state.json"),
            consumed_state_path=str(
                os.getenv("HUMO_CONSUMED_STATE_PATH", os.path.join(data_dir, "humo_consumed_payments.json"))
            ).strip()
            or os.path.join(data_dir, "humo_consumed_payments.json"),
        )


class PaymentVerificationService:
    def __init__(self, config=None):
        self.config = config or PaymentVerificationConfig.from_env()
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
                    return result

                humo_result = self._verify_with_humo(
                    ocr,
                    amount_value,
                    session_started_at=session_ts,
                    session_expires_at=session_expires_ts,
                )
                if payment_key and isinstance(humo_result, dict):
                    humo_result["payment_key"] = str(payment_key).strip()
                result["humo"] = humo_result
                if not humo_result.get("ok"):
                    result["reason"] = str(humo_result.get("reason", "Оплата не найдена в @HUMOcardbot"))
                    return result
        except Exception as error:
            result["reason"] = f"Проверка оплаты завершилась ошибкой: {error}"
            return result

        result["ok"] = True
        result["mode"] = str(result.get("humo", {}).get("mode", "unknown"))
        result["reason"] = ""
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
                    return result

                humo_result = self._verify_with_humo(
                    ocr,
                    detected_amount,
                    session_started_at=session_ts,
                    session_expires_at=session_expires_ts,
                )
                if payment_key and isinstance(humo_result, dict):
                    humo_result["payment_key"] = str(payment_key).strip()
                result["humo"] = humo_result
                if not humo_result.get("ok"):
                    result["reason"] = str(humo_result.get("reason", "Оплата не найдена в @HUMOcardbot"))
                    return result

                result["credited_amount"] = int(detected_amount)
        except Exception as error:
            result["reason"] = f"Проверка оплаты завершилась ошибкой: {error}"
            return result

        result["ok"] = True
        result["mode"] = str(result.get("humo", {}).get("mode", "unknown"))
        result["reason"] = ""
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
        # Real anti-fraud decision is made against @HUMOcardbot transfer/balance data
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

    def _verify_with_humo(self, ocr, expected_amount, *, session_started_at=0, session_expires_at=0):
        now_local = datetime.now(DEFAULT_TIMEZONE)
        since_local = now_local - timedelta(hours=self.config.humo_lookback_hours)
        ocr_datetime = _parse_ocr_datetime(ocr.get("detected_date_time"))
        consumed_state = self._read_consumed_state()
        self._cleanup_consumed_state(consumed_state)
        target_card_last4 = self._select_target_card_last4()

        with self._open_humo_client() as client_info:
            client, humo_entity = client_info
            messages_before = self._fetch_humo_messages(
                client=client,
                humo_entity=humo_entity,
                since_local=since_local,
                limit=self.config.humo_message_limit,
            )
            transfer_match = self._find_transfer_match(
                messages=messages_before,
                expected_amount=expected_amount,
                ocr_datetime=ocr_datetime,
                consumed_state=consumed_state,
                session_started_at=session_started_at,
                session_expires_at=session_expires_at,
            )
            if transfer_match:
                self._consume_transfer_message(
                    consumed_state=consumed_state,
                    transfer_match=transfer_match,
                )
                message_balance = transfer_match.get("balance_after")
                if message_balance is not None:
                    self._write_balance_state(message_balance)
                self._write_consumed_state(consumed_state)
                return {
                    "ok": True,
                    "mode": "transfer_message",
                    "matched_message_id": transfer_match.get("id"),
                    "matched_amount": transfer_match.get("amount"),
                    "matched_at": transfer_match.get("date"),
                }

            before_top_id = max((item.get("id", 0) for item in messages_before), default=0)
            # Baseline must be taken from the latest known balance BEFORE we send a new command.
            previous_balance = self._extract_latest_balance(
                messages_before,
                snapshot_only=False,
                target_card_last4=target_card_last4,
            )
            if previous_balance is None:
                prev_state = self._read_balance_state()
                previous_balance = prev_state.get("last_balance")

            client.send_message(humo_entity, self.config.humo_balance_command)

            # Poll for the actual balance response.
            # HUMO bot first sends "Подождите..." (with a service number), then the real balance.
            # We wait for a new message (id > before_top_id) that contains an explicit balance amount.
            max_poll_seconds = max(self.config.humo_wait_seconds * 3, 15.0)
            poll_interval = 1.5
            elapsed = 0.0
            new_balance = None
            messages_after = []
            while elapsed < max_poll_seconds:
                time.sleep(poll_interval)
                elapsed += poll_interval
                messages_after = self._fetch_humo_messages(
                    client=client,
                    humo_entity=humo_entity,
                    since_local=since_local,
                    limit=self.config.humo_message_limit,
                )
                new_messages = [
                    item for item in messages_after
                    if int(item.get("id", 0)) > before_top_id
                ]
                candidate = self._extract_latest_balance(
                    new_messages,
                    snapshot_only=True,
                    target_card_last4=target_card_last4,
                )
                if candidate is not None:
                    new_balance = candidate
                    break

            if previous_balance is None or new_balance is None:
                return {
                    "ok": False,
                    "mode": "balance_diff",
                    "reason": "Оплата не подтверждена: нет данных для сравнения баланса @HUMOcardbot",
                    "previous_balance": previous_balance,
                    "current_balance": new_balance,
                }

            difference = int(new_balance) - int(previous_balance)
            if abs(difference - int(expected_amount)) <= self.config.amount_tolerance:
                if self._is_balance_diff_consumed(
                    consumed_state=consumed_state,
                    previous_balance=int(previous_balance),
                    current_balance=int(new_balance),
                    expected_amount=int(expected_amount),
                ):
                    return {
                        "ok": False,
                        "mode": "balance_diff",
                        "reason": "Оплата уже привязана к другой покупке и не может быть использована повторно",
                        "previous_balance": int(previous_balance),
                        "current_balance": int(new_balance),
                        "difference": int(difference),
                    }

                self._consume_balance_diff(
                    consumed_state=consumed_state,
                    previous_balance=int(previous_balance),
                    current_balance=int(new_balance),
                    expected_amount=int(expected_amount),
                    difference=int(difference),
                )
                self._write_balance_state(int(new_balance))
                self._write_consumed_state(consumed_state)
                return {
                    "ok": True,
                    "mode": "balance_diff",
                    "previous_balance": int(previous_balance),
                    "current_balance": int(new_balance),
                    "difference": int(difference),
                }

            if difference == 0:
                reason = "Оплата не подтверждена: баланс карты не изменился"
            else:
                reason = (
                    f"Оплата не подтверждена: разница баланса ({difference}) "
                    f"не равна сумме платежа ({int(expected_amount)})"
                )
            return {
                "ok": False,
                "mode": "balance_diff",
                "reason": reason,
                "previous_balance": int(previous_balance),
                "current_balance": int(new_balance),
                "difference": int(difference),
            }

    def _find_transfer_match(
        self,
        *,
        messages,
        expected_amount,
        ocr_datetime,
        consumed_state,
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
            # Rule: HUMO message must always be recent relative to NOW (1 payment = 1 privilege).
            # This prevents old payments (made by other users) from being matched to new purchases.
            max_age_seconds = self.config.transfer_max_message_age_minutes * 60
            if transfer_datetime:
                age_from_now = (now_local - transfer_datetime).total_seconds()
                if age_from_now > max_age_seconds:
                    continue
                # KEY RULE: payment must have been made AFTER the 5-minute payment session started.
                # This prevents reuse of a real old payment that was made before this purchase session.
                if session_start_dt is not None and transfer_datetime < session_start_dt:
                    continue
                # Payment must belong to this payment session window (with small clock tolerance).
                if session_end_dt is not None and transfer_datetime > (session_end_dt + session_grace):
                    continue
                # Additional check: if OCR timestamp present, message date must be close to it.
                if ocr_datetime:
                    delta_seconds = abs((transfer_datetime - ocr_datetime).total_seconds())
                    max_delta = self.config.transfer_time_tolerance_hours * 3600
                    if delta_seconds > max_delta:
                        continue
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
                "balance_after": _extract_balance_amount(text),
                "text": text[:200],
            }
        return None

    @staticmethod
    def _transfer_message_key(message_id):
        return str(int(message_id))

    @staticmethod
    def _balance_diff_key(previous_balance, current_balance, expected_amount):
        return f"{int(previous_balance)}:{int(current_balance)}:{int(expected_amount)}"

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
                if created_at > 0 and (now_ts - created_at) > (self.config.humo_lookback_hours * 3600 + 3600):
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
                if created_at > 0 and (now_ts - created_at) > (self.config.humo_lookback_hours * 3600 + 3600):
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

    def _is_balance_diff_consumed(self, *, consumed_state, previous_balance, current_balance, expected_amount):
        balance_records = consumed_state.get("used_balance_diffs", {})
        if not isinstance(balance_records, dict):
            return False
        key = self._balance_diff_key(previous_balance, current_balance, expected_amount)
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
    ):
        balance_records = consumed_state.setdefault("used_balance_diffs", {})
        if not isinstance(balance_records, dict):
            balance_records = {}
            consumed_state["used_balance_diffs"] = balance_records
        key = self._balance_diff_key(previous_balance, current_balance, expected_amount)
        balance_records[key] = {
            "created_at": int(time.time()),
            "previous_balance": int(previous_balance),
            "current_balance": int(current_balance),
            "expected_amount": int(expected_amount),
            "difference": int(difference),
        }

    def _fetch_humo_messages(self, *, client, humo_entity, since_local, limit):
        records = []
        for message in client.iter_messages(humo_entity, limit=limit):
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

    @staticmethod
    def _extract_card_balances_from_snapshot(raw_text):
        text = str(raw_text or "")
        if "💵" not in text:
            return {}

        balances = {}
        pending_cards = []
        for line in text.splitlines():
            row = str(line or "").strip()
            if not row:
                continue

            card_matches = CARD_LAST4_PATTERN.findall(row)
            if card_matches:
                pending_cards = card_matches

            if "💵" not in row:
                continue

            amounts = _extract_amount_candidates(row)
            if not amounts:
                continue
            amount = int(max(amounts))

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
            return int(max(snapshot_balances.values()))

        if snapshot_only:
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

    def _open_humo_client(self):
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

            username = self.config.humo_username.lstrip("@")
            entity = client.get_entity(self.config.humo_username)
            entity_username = str(getattr(entity, "username", "") or "").strip().lower()
            if entity_username != username.lower():
                raise RuntimeError("Ошибка доступа: разрешён только чат @HUMOcardbot")
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

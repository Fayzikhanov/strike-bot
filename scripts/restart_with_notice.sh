#!/usr/bin/env bash
set -euo pipefail

INSTANCE="${1:-strike_main}"
BASE_DIR="/opt/strike-bots/${INSTANCE}"
REPO_DIR="${BASE_DIR}/repo"
ENV_FILE="${BASE_DIR}/.env"
PYTHON_BIN="${BASE_DIR}/venv/bin/python"
NOTICE_SCRIPT="${REPO_DIR}/scripts/send_restart_notice.py"
SERVICE_NAME="strikebot@${INSTANCE}.service"

NOTICE_MINUTES="${NOTICE_MINUTES:-2}"
NOTICE_REASON="${NOTICE_REASON:-Обновление / Yangilanish}"
NOTICE_DELAY_SECONDS="${NOTICE_DELAY_SECONDS:-15}"

read_env_value() {
  local key="$1"
  local file="$2"
  awk -F= -v target_key="$key" '
    $1 == target_key {
      value = substr($0, index($0, "=") + 1)
      gsub(/\r/, "", value)
      print value
      exit
    }
  ' "$file"
}

BOT_TOKEN_VALUE="${BOT_TOKEN:-}"
WEB_APP_URL_VALUE="${WEB_APP_URL:-}"

if [[ -f "${ENV_FILE}" ]]; then
  if [[ -z "${BOT_TOKEN_VALUE}" ]]; then
    BOT_TOKEN_VALUE="$(read_env_value "BOT_TOKEN" "${ENV_FILE}")"
  fi
  if [[ -z "${WEB_APP_URL_VALUE}" ]]; then
    WEB_APP_URL_VALUE="$(read_env_value "WEB_APP_URL" "${ENV_FILE}")"
  fi
fi

if [[ -x "${PYTHON_BIN}" && -f "${NOTICE_SCRIPT}" ]]; then
  "${PYTHON_BIN}" "${NOTICE_SCRIPT}" \
    --data-file "${REPO_DIR}/data/purchase_reports.json" \
    --bot-token "${BOT_TOKEN_VALUE}" \
    --web-app-url "${WEB_APP_URL_VALUE}" \
    --minutes "${NOTICE_MINUTES}" \
    --reason "${NOTICE_REASON}" || true
else
  echo "[RESTART NOTICE] script skipped: missing ${PYTHON_BIN} or ${NOTICE_SCRIPT}" >&2
fi

sleep "${NOTICE_DELAY_SECONDS}"

systemctl restart "${SERVICE_NAME}"
systemctl --no-pager --full status "${SERVICE_NAME}" | sed -n '1,25p'

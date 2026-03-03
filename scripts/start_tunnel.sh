#!/usr/bin/env bash
# Uses cloudflared for a stable tunnel (no URL changes on reconnect within a session).

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${REPO_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
LOG_FILE="${TUNNEL_LOG_FILE:-$REPO_DIR/tunnel.log}"
URL_FILE="${WEB_APP_URL_FILE:-$REPO_DIR/data/web_app_url.txt}"
MINIAPP_BASE="${MINIAPP_BASE:-https://miniapp-beta-two.vercel.app/}"
MINIAPP_VERSION="${MINIAPP_VERSION:-$(date '+%Y%m%d%H%M%S')}"
API_PORT="${API_PORT:-8090}"
CLOUDFLARED_BIN="${CLOUDFLARED_BIN:-cloudflared}"

cd "$REPO_DIR" || exit 1
mkdir -p "$(dirname "$URL_FILE")"

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting cloudflared tunnel → localhost:${API_PORT}" >> "$LOG_FILE"

  "$CLOUDFLARED_BIN" tunnel --url "http://127.0.0.1:${API_PORT}" --no-autoupdate 2>&1 | tee -a "$LOG_FILE" | \
  while IFS= read -r line; do
    if echo "$line" | grep -qE 'trycloudflare\.com'; then
      tunnel_url=$(echo "$line" | grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com')
      if [ -n "$tunnel_url" ]; then
        encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$tunnel_url")
        full_url="${MINIAPP_BASE}?v=${MINIAPP_VERSION}&api=${encoded}"
        echo "$full_url" > "$URL_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Tunnel URL saved: $tunnel_url" >> "$LOG_FILE"
      fi
    fi
  done

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] cloudflared exited, restarting in 3s" >> "$LOG_FILE"
  sleep 3
done

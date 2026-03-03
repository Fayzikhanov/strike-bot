#!/usr/bin/env bash
# Uses cloudflared for a stable tunnel (no URL changes on reconnect within a session).

set -u

REPO_DIR="/Users/fayzikhanov/strike_bot"
LOG_FILE="$REPO_DIR/tunnel.log"
URL_FILE="$REPO_DIR/data/web_app_url.txt"
MINIAPP_BASE="https://miniapp-beta-two.vercel.app/"
MINIAPP_VERSION="20260303190229"

cd "$REPO_DIR" || exit 1

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting cloudflared tunnel → localhost:8090" >> "$LOG_FILE"

  cloudflared tunnel --url http://localhost:8090 --no-autoupdate 2>&1 | tee -a "$LOG_FILE" | \
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

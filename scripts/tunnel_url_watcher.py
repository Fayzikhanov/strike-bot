#!/usr/bin/env python3
"""
Watches tunnel.log for new tunnel URLs (trycloudflare.com or lhr.life) and writes web_app_url.txt.
The bot reads web_app_url.txt on every keyboard/button build, so no restart needed.
"""

import os
import re
import time
import urllib.parse

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = os.path.join(REPO_DIR, "tunnel.log")
URL_FILE = os.path.join(REPO_DIR, "data", "web_app_url.txt")
MINIAPP_BASE = "https://miniapp-beta-two.vercel.app/"
MINIAPP_VERSION = "20260302120000"
# Match both trycloudflare.com and lhr.life tunnel URLs
URL_PATTERN = re.compile(r"https://(?:[a-z0-9-]+\.trycloudflare\.com|[a-f0-9]{14}\.lhr\.life)")

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[mGKH]")


def strip_ansi(text):
    return ANSI_ESCAPE.sub("", text)


def write_url(tunnel_url):
    encoded = urllib.parse.quote(tunnel_url, safe="")
    full_url = f"{MINIAPP_BASE}?v={MINIAPP_VERSION}&api={encoded}"
    os.makedirs(os.path.dirname(URL_FILE), exist_ok=True)
    with open(URL_FILE, "w") as f:
        f.write(full_url)
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] Tunnel URL updated: {tunnel_url}", flush=True)


def tail_log():
    """Open log file, seek to end, then yield new lines as they arrive."""
    # First scan the whole file for the most recent URL (catch-up)
    try:
        with open(LOG_FILE, "rb") as f:
            raw = f.read()
        text = strip_ansi(raw.decode("utf-8", errors="replace"))
        matches = URL_PATTERN.findall(text)
        if matches:
            write_url(matches[-1])
    except FileNotFoundError:
        pass

    while True:
        try:
            with open(LOG_FILE, "rb") as f:
                f.seek(0, 2)  # seek to end
                while True:
                    raw_line = f.readline()
                    if raw_line:
                        line = strip_ansi(raw_line.decode("utf-8", errors="replace"))
                        match = URL_PATTERN.search(line)
                        if match:
                            write_url(match.group(0))
                    else:
                        time.sleep(0.5)
        except FileNotFoundError:
            time.sleep(2)
        except Exception as e:
            print(f"[watcher] error: {e}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    print(f"[watcher] Watching {LOG_FILE}", flush=True)
    tail_log()

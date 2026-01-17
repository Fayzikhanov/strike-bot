import os
import requests
from bs4 import BeautifulSoup
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ==================================================
# 🔐 TOKEN (ТОЛЬКО ИЗ ENV)
# ==================================================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

# ==================================================
# 🌐 CONFIG
# ==================================================
URL = "https://strike.uz/"

# ==================================================
# 📝 TEXTS
# ==================================================
START_TEXT = """<b>👋 Assalomu alaykum! Strike.Uz ga xush kelibsiz!</b>

<b>Mavjud buyruqlar:</b>
<b>/info</b> — Strike.Uz loyihasi haqida ma’lumot
<b>/server</b> — Serverlar ro‘yxati
<b>/vip</b> — VIP haqida ma’lumot

<b>ℹ️ Qo‘shimcha ma’lumot uchun:</b>
@MccallStrike

<b>📣 Telegram kanalimiz:</b>
@STRIKEUZCHANNEL

<b>💬 Telegram guruhlarimiz:</b>
@STRIKEUZCOMMUNITY
@STRIKECW
@STRIKEUZREPORTS

────────────────────

<b>👋 Добро пожаловать в Strike.Uz!</b>

<b>Доступные команды:</b>
<b>/info</b> — Информация о проекте
<b>/server</b> — Список серверов
<b>/vip</b> — Информация о VIP

<b>ℹ️ Контакт:</b>
@MccallStrike
"""

INFO_TEXT = """<b>🇺🇿 Strike.Uz ga hush kelibsiz!</b>

Strike.Uz — O‘zbekistondagi eng sifatli Counter-Strike 1.6 serverlari.

<b>🌐 Server IP manzillari:</b>
`/server` buyrug‘i orqali ko‘rishingiz mumkin.

<b>🔥 Sizni serverlarimizda kutamiz!</b>

────────────────────

<b>🇷🇺 Добро пожаловать в Strike.Uz!</b>

Strike.Uz — качественные и стабильные CS 1.6 сервера Узбекистана.

<b>🌐 IP адреса серверов:</b>
Доступны по команде `/server`.
"""

VIP_TEXT = """<b>🇺🇿 VIP haqida ma’lumot</b>

VIP xizmatlari bo‘yicha:
👉 @MccallStrike

────────────────────

<b>🇷🇺 Информация о VIP</b>

По VIP-услугам:
👉 @MccallStrike
"""

# ==================================================
# ⌨️ KEYBOARD
# ==================================================
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["ℹ️ Info", "🌐 Servers"],
        ["⭐ VIP"],
    ],
    resize_keyboard=True
)

# ==================================================
# 🧠 HELPERS
# ==================================================
def percent(a: int, b: int) -> int:
    return int((a / b) * 100) if b else 0


def get_servers():
    response = requests.get(URL, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    servers = []

    for row in soup.find_all("tr"):
        text = row.get_text(" ", strip=True)

        if "Strike.Uz |" in text and "из" in text and ":" in text:
            parts = text.split()

            try:
                name = " ".join(parts[:-5])
                players = int(parts[-5])
                max_players = int(parts[-3])
                game_map = parts[-2]
                ip = parts[-1]

                servers.append({
                    "name": name,
                    "players": players,
                    "max": max_players,
                    "map": game_map,
                    "ip": ip,
                })
            except Exception:
                continue

    return servers

# ==================================================
# 🤖 HANDLERS
# ==================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        START_TEXT,
        parse_mode="HTML",
        reply_markup=MAIN_KEYBOARD
    )


async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(INFO_TEXT, parse_mode="HTML")


async def vip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(VIP_TEXT, parse_mode="HTML")


async def server(update: Update, context: ContextTypes.DEFAULT_TYPE):
    servers = get_servers()

    if not servers:
        await update.message.reply_text("❌ Не удалось получить список серверов.")
        return

    total_players = sum(s["players"] for s in servers)
    total_max = sum(s["max"] for s in servers)

    message = (
        f"<b>📊 Общая статистика:</b> "
        f"{total_players}/{total_max} "
        f"[{percent(total_players, total_max)}%]\n\n"
    )

    for s in servers:
        message += (
            f"⚡ <b>Server:</b> {s['name']}\n"
            f"🌐 <b>IP:</b> {s['ip']}\n"
            f"📍 <b>Map:</b> {s['map']}\n"
            f"👥 <b>Players:</b> {s['players']} из {s['max']} "
            f"[{percent(s['players'], s['max'])}%]\n\n"
        )

    await update.message.reply_text(message, parse_mode="HTML")


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "ℹ️ Info":
        await info(update, context)
    elif text == "🌐 Servers":
        await server(update, context)
    elif text == "⭐ VIP":
        await vip(update, context)

# ==================================================
# 🚀 APP
# ==================================================
app = ApplicationBuilder().token(TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("info", info))
app.add_handler(CommandHandler("server", server))
app.add_handler(CommandHandler("vip", vip))

app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, buttons))

print("✅ Strike.Uz bot запущен")
app.run_polling()

# 🔐 Токен берём ТОЛЬКО из переменной окружения
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

import requests
import asyncio
import html
from telegram.ext import MessageHandler, filters
from bs4 import BeautifulSoup
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler

BASE_IP = "83.69.139.205"
URL = "https://strike.uz/"

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


START_TEXT = """<b>👋 Assalomu alaykum! Strike.Uz ga xush kelibsiz!</b>

<b>Mavjud buyruqlar:</b>
<b>/info</b> — Strike.Uz loyihasi haqida ma’lumot
<b>/server</b> — Serverlar ro‘yxati
<b>/players</b> — Serverlardagi o‘yinchilar ro‘yxati
<b>/vip</b> — VIP haqida ma’lumot

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

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["ℹ️ Info", "🌐 Servers", "👥 Players"],
        ["⭐ VIP"],
    ],
    resize_keyboard=True
)


def get_servers():
    html = requests.get(URL, timeout=10).text
    soup = BeautifulSoup(html, "html.parser")

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
                    "ip": ip
                })
            except:
                continue

    return servers

def percent(a, b):
    return int((a / b) * 100) if b else 0

import a2s

def get_server_info(port):
    try:
        info = a2s.info((BASE_IP, port), timeout=1.5)

        return {
            "name": info.server_name,
            "map": info.map_name,
            "players": info.player_count,
            "max": info.max_players,
        }

    except Exception:
        return {
            "name": f"Server {port}",
            "map": "unknown",
            "players": 0,
            "max": 0,
        }

def main_inline_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 Players", callback_data="menu_players"),
            InlineKeyboardButton("🌐 Servers", callback_data="menu_servers"),
        ],
        [
            InlineKeyboardButton("ℹ️ Info", callback_data="menu_info"),
            InlineKeyboardButton("⭐ VIP", callback_data="menu_vip"),
        ]
    ])


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎮 <b>Strike.Uz меню</b>",
        reply_markup=main_inline_keyboard(),
        parse_mode="HTML"
    )


async def get_players_async(port):
    try:
        players = await asyncio.to_thread(
            a2s.players,
            (BASE_IP, port),
            timeout=1.5
        )

        result = []
        for p in players:
            result.append({
                "name": p.name.strip() if p.name else "unnamed",
                "kills": p.score,
                "time": int(p.duration // 60),
            })

        return result

    except Exception as e:
        print(f"[PLAYERS ERROR] {BASE_IP}:{port} -> {e}")
        return []


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
        await update.message.reply_text(
            START_TEXT,
            parse_mode="HTML",
            reply_markup=MAIN_KEYBOARD
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
    servers = get_servers()

    if not servers:
        await update.message.reply_text("Не удалось получить сервера.")
        return

    total_players = sum(s["players"] for s in servers)
    total_max = sum(s["max"] for s in servers)

    message = f"<b>📊 Statistics:</b> {total_players}/{total_max} [{percent(total_players, total_max)}%]\n\n"

    for s in servers:
        message += (
            f"⚡<b>️Server:</b> {s['name']}\n"
            f"🌐<b>IP:</b> {s['ip']}\n"
            f"📍<b>Map:</b> {s['map']}\n"
            f"👥<b>Players:</b> {s['players']} из {s['max']} [{percent(s['players'], s['max'])}%]\n\n\n"
        )

    await update.message.reply_text(message, parse_mode="HTML")

async def players_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await players(update, context)


async def players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = []

    for key, category in SERVERS.items():
        keyboard.append([
            InlineKeyboardButton(
                category["title"],
                callback_data=f"players_category:{key}"
            )
        ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
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
    await query.answer()

    if query.data == "menu_players":
        await players(query, context)

    elif query.data == "menu_servers":
        await server(query, context)

    elif query.data == "menu_info":
        await query.edit_message_text(INFO_TEXT, parse_mode="HTML")

    elif query.data == "menu_vip":
        await query.edit_message_text(VIP_TEXT, parse_mode="HTML")



async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if text == "ℹ️ Info":
        await info(update, context)

    elif text == "🌐 Servers":
        await server(update, context)

    elif text == "⭐ VIP":
        await vip(update, context)

    elif text == "🎮 Start CS":
        await update.message.reply_text(
            "🎮 Раздел /startcs будет добавлен следующим шагом 😉",
            reply_markup=MAIN_KEYBOARD
        )


app = ApplicationBuilder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("server", server))
app.add_handler(CommandHandler("info", info))
app.add_handler(CommandHandler("vip", vip))

app.add_handler(
    MessageHandler(filters.TEXT & filters.Regex("^👥 Players$"), players_button)
)

app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, buttons))

app.add_handler(CommandHandler("players", players))
app.add_handler(
    CallbackQueryHandler(players_category_callback, pattern="^players_category:")
)

app.add_handler(
    CallbackQueryHandler(players_server_callback, pattern="^players_server:")
)

app.add_handler(CommandHandler("menu", menu))
app.add_handler(CallbackQueryHandler(menu_callback, pattern="^menu_"))


print("Бот запущен и ждёт команды...")
app.run_polling()

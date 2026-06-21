import re
import asyncio
import logging
import requests
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from telethon import TelegramClient, events
import uvicorn


# Данные Telegram API: https://my.telegram.org
API_ID = 32297855
API_HASH = "6c3dc1cbc09517cec9b023e18aa61acf"
PHONE = "+79109185266"

# Данные твоего Telegram-бота
BOT_TOKEN = "8537830827:AAH8qerFzDsN5smHWjb7pAIfj_3lcyoDqFY"
CHAT_IDS = ["382945139", "1306091284"]

SERVER_TOKEN = "secret123"

HOST = "0.0.0.0"
PORT = 8000
MAX_HISTORY = 1000

MONITORED_CHATS = [
    "in_israel_with_pets",
    "kiryatmotzkin_gorod",
    "Mozkin",
    "haifa_live",
    "Kiryatiam",
]

KEYWORDS = [
    "пансион",
    "передержк",
    "оставить собак",
    "оставить пса",
    "куда деть собак",
    "куда пристроить собак",
    "кто возьмет собак",
    "присмотреть за собак",
    "кинолог",
    "дрессиров",
    "поведение собак",
    "собака тянет",
    "собака лает",
    "щенок кусается",
    "פנסיון",
    "דוגסיטר",
    "דוג סיטר",
    "אילוף",
    "מאלף",
    "מאלפת",
    "בעיות התנהגות",
]

EXCLUDE_WORDS = [
    "реклама",
    "предлагаю",
    "берем",
    "берём",
    "беру",
    "котенок",
    "котёнок",
    "котенка",
    "котёнка",
    "кошк",
    "услуги передержки",
    "услуги кинолога",
    "приглашаю",
    "ищет дом",
    "ищу подработку",
    "פרסום",
    "מבצע",
]

EXCLUDE_PATTERNS = [
    r"\bкот\b",
]


app = FastAPI()


class LocationUpdate(BaseModel):
    token: str
    user_id: str
    name: str
    lat: float
    lng: float
    accuracy: Optional[float] = None


telegram_client = TelegramClient(
    "haifa_monitor_session",
    API_ID,
    API_HASH,
)

ws_clients: set[WebSocket] = set()
message_history: list[str] = []
locations = {}

main_loop = None


def normalize(text: str) -> str:
    return text.lower().replace("ё", "е")


def is_interesting(text: str) -> bool:
    text = normalize(text)

    if any(word in text for word in EXCLUDE_WORDS):
        return False

    if any(re.search(pattern, text) for pattern in EXCLUDE_PATTERNS):
        return False

    return any(keyword in text for keyword in KEYWORDS)


def short_text(text: str, limit: int = 1000) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit] + ("..." if len(text) > limit else "")


def add_to_history(message: str):
    message_history.append(message)

    if len(message_history) > MAX_HISTORY:
        del message_history[:-MAX_HISTORY]


async def broadcast(message: str):
    add_to_history(message)

    dead_clients = []

    for websocket in list(ws_clients):
        try:
            await websocket.send_text(message)
        except Exception:
            dead_clients.append(websocket)

    for websocket in dead_clients:
        ws_clients.discard(websocket)


async def log_message(message: str):
    print(message)
    await broadcast(f"🖥 {message}")


class WebSocketLogHandler(logging.Handler):
    def emit(self, record):
        global main_loop

        try:
            msg = self.format(record)
            msg = f"🖥 SERVER: {msg}"

            if main_loop and main_loop.is_running():
                asyncio.run_coroutine_threadsafe(broadcast(msg), main_loop)

        except Exception:
            pass


def setup_logging_to_websocket():
    handler = WebSocketLogHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
        logger = logging.getLogger(logger_name)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)


def send_to_bot(message: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    for chat_id in CHAT_IDS:
        try:
            response = requests.post(
                url,
                data={
                    "chat_id": chat_id,
                    "text": message,
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )

            if not response.ok:
                print(f"Ошибка отправки в chat_id={chat_id}: {response.text}")

        except Exception as e:
            print(f"Ошибка Telegram Bot API: {e}")


@app.get("/")
def health():
    return {
        "status": "ok",
        "ws": "/ws?token=secret123",
        "history": len(message_history),
        "clients": len(ws_clients),
    }


@app.post("/location/update")
async def update_location(data: LocationUpdate):
    if data.token != SERVER_TOKEN:
        return {"status": "error", "message": "bad token"}

    locations[data.user_id] = {
        "user_id": data.user_id,
        "name": data.name,
        "lat": data.lat,
        "lng": data.lng,
        "accuracy": data.accuracy,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    await broadcast(
        f"📍 Координаты обновлены: {data.name} {data.lat}, {data.lng}"
    )

    return {"status": "ok"}


@app.get("/locations")
def get_locations(token: str):
    if token != SERVER_TOKEN:
        return {"status": "error", "message": "bad token"}

    return {
        "status": "ok",
        "locations": list(locations.values()),
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.query_params.get("token", "")

    if token != SERVER_TOKEN:
        print(f"❌ Неверный token: {token}")
        await websocket.close(code=1008)
        return

    await websocket.accept()
    ws_clients.add(websocket)

    try:
        await websocket.send_text("✅ Подключено к мониторингу Telegram\n")

        if message_history:
            await websocket.send_text(
                f"📜 История с момента запуска сервера: {len(message_history)} сообщений\n"
            )

            for old_message in message_history:
                await websocket.send_text(old_message)
        else:
            await websocket.send_text("📜 История пока пустая\n")

        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        ws_clients.discard(websocket)

    except Exception as e:
        print(f"Ошибка WebSocket: {e}")
        ws_clients.discard(websocket)


@telegram_client.on(events.NewMessage(chats=MONITORED_CHATS))
async def handler(event):
    text = event.raw_text or ""

    chat = await event.get_chat()
    sender = await event.get_sender()

    chat_username = getattr(chat, "username", None)
    chat_title = getattr(chat, "title", None) or chat_username or "Unknown"

    sender_name = (
        getattr(sender, "first_name", None)
        or getattr(sender, "username", None)
        or "Unknown"
    )

    if chat_username:
        channel_link = f"https://t.me/{chat_username}"
        message_link = f"https://t.me/{chat_username}/{event.message.id}"
    else:
        channel_link = "Нет публичной ссылки"
        message_link = "Нет публичной ссылки"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    console_message = f"""📩 Новое сообщение

Канал: {chat_title}
Ссылка на канал: {channel_link}
Время: {now}
Автор: {sender_name}
Ссылка на сообщение: {message_link}

Текст:
{short_text(text)}
"""

    print(console_message)
    await broadcast(console_message)

    if not is_interesting(text):
        return

    bot_message = f"""🐶 Возможная заявка

Канал: {chat_title}
Ссылка на канал: {channel_link}
Время: {now}
Автор: {sender_name}
Ссылка на сообщение: {message_link}

Текст:
{short_text(text)}
"""

    send_to_bot(bot_message)


async def telegram_main():
    await telegram_client.start(phone=PHONE)

    await log_message("Мониторинг Telegram запущен")
    await log_message("Каналы:")

    for chat_name in MONITORED_CHATS:
        try:
            entity = await telegram_client.get_entity(chat_name)
            title = getattr(entity, "title", chat_name)
            username = getattr(entity, "username", chat_name)

            await log_message(f"- {title}: https://t.me/{username}")

        except Exception as e:
            await log_message(f"- Ошибка канала {chat_name}: {e}")

    await telegram_client.run_until_disconnected()


@app.on_event("startup")
async def startup_event():
    global main_loop

    main_loop = asyncio.get_running_loop()
    setup_logging_to_websocket()

    await broadcast("🚀 Сервер запущен")
    asyncio.create_task(telegram_main())


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
    )

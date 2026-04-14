import os
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.http import SessionManager
from utils.exchanges import get_exchange_api
from utils.telegram import TelegramManager

load_dotenv()

# Logging configuration
logger = setup_logger("funding_monitor")

# Telegram configuration
FUNDING_BOT_TOKEN = os.getenv("FUNDING_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHAT_IDS = [TELEGRAM_CHAT_ID] if TELEGRAM_CHAT_ID else []
 
def format_funding_symbol(symbol: str, exchange: str) -> str:
    exchange = exchange.lower().strip()
    if exchange in ["bybit", "binance", "asterdex", "bitget", "bitmart"]:
        return f"{symbol}USDT"
    if exchange == "okx":
        return f"{symbol}-USDT-SWAP"
    if exchange == "kucoin":
        return f"{symbol}USDTM"
    if exchange == "bingx":
        return f"{symbol}-USDT"
    if exchange in ["gate", "mexc"]:
        return f"{symbol}_USDT"
    if exchange == "htx":
        return f"{symbol}-USDT"
    if exchange == "hyperliquid":
        return symbol # Hyperliquid uses just the base name for perps
    return f"{symbol}USDT"


async def send_funding_message(message):
    try:
        await TelegramManager.send_message(
            FUNDING_BOT_TOKEN,
            CHAT_IDS,
            "🔴↓ Funding DOWN\n\n" + message,
            parse_mode="MarkdownV2"
        )
        logger.info("Funding сообщение отправлено")
    except Exception as e:
        logger.error(f"Ошибка отправки funding сообщения: {e}")

class ConfigManager:
    # IMPORTANT: Set this to the ID of your new document in the funding_monitor database
    ACTUAL_CONFIG = ObjectId("69bd53983b00a572d2935eb5") 

    def __init__(self):
        self.mongo_uri = os.getenv("MONGO_URI")
        self.database_name = os.getenv("MONGO_DB_NAME_FUNDING_MONITOR", "funding_monitor")
        self.collection_name = os.getenv("MONGO_COLLECTION_NAME_FUNDING_MONITOR", "funding_monitor_collection")
        self.cached_config = None
        logger.info(f"ConfigManager initialized for {self.database_name}")

    def load(self):
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.database_name]
            collection = db[self.collection_name]
            
            if self.ACTUAL_CONFIG:
                config_document = collection.find_one({"_id": self.ACTUAL_CONFIG})
            else:
                # Fallback: take the first document if ID is not specified
                config_document = collection.find_one()
                
            if not config_document:
                logger.error("Configuration not found in MongoDB")
                return None
            
            # Use the document structure (expects 'tokens' at top level)
            self.cached_config = config_document
            return self.cached_config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
        return None

config_manager = ConfigManager()

async def get_funding_rate(exchange_name, symbol):
    try:
        api = get_exchange_api(exchange_name)
        session = await SessionManager.get_session()
        return await api.fetch_funding_rate(session, symbol)
    except Exception as e:
        logger.error(f"Error fetching funding rate from {exchange_name} for {symbol}: {e}")
    return None, None

# last_funding_notifications[token] = (datetime, timedelta)
last_funding_notifications: Dict[str, Tuple[datetime, timedelta]] = {}

async def check_funding_rates():
    logger.info("Starting funding rates monitoring")
    while True:
        try:
            now = datetime.now(timezone.utc)
            current_config = config_manager.load()
            if not current_config:
                logger.error("Не удалось загрузить конфиг для funding rates")
                await asyncio.sleep(30)
                continue

            if not current_config.get("global_tracking", True):
                logger.info("Глобальный мониторинг фандинга отключен в БД (global_tracking=false)")
                await asyncio.sleep(60)
                continue

            monitored_tokens = current_config.get("tokens", {})

            # Фильтруем только активные токены
            active_tokens = {
                token: cfg for token, cfg in monitored_tokens.items()
                if cfg.get("active", True)
            }

            if not active_tokens:
                logger.warning("Нет активных токенов для мониторинга")
                await asyncio.sleep(30)
                continue

            # Очищаем кэш для токенов, которых нет в текущем конфиге
            current_tokens = set(active_tokens.keys())
            cached_tokens = set(last_funding_notifications.keys())
            for token in cached_tokens - current_tokens:
                last_funding_notifications.pop(token, None)
                logger.info(f"Токен {token} удален из кэша funding мониторинга")

            for token, token_config in active_tokens.items():
                if not isinstance(token, str):
                    continue
                threshold = token_config.get("threshold", -0.0001)
                time_funding_left = token_config.get("time-funding_left") # в минутах
                logger.debug(f"Проверка {token} (порог: {threshold}, окно: {time_funding_left})")

                funding_rates = {}
                min_rate = None
                earliest_next_funding = None

                for exchange in token_config.get("exchanges", []):
                    # rate: Optional[float], next_time_ms: Optional[int]
                    formatted_symbol = format_funding_symbol(token, exchange)
                    rate_data = await get_funding_rate(exchange, formatted_symbol)
                    rate, next_time_ms = rate_data
                    if rate is not None:
                        funding_rates[exchange] = rate
                        if min_rate is None or rate < min_rate:
                            min_rate = rate
                        
                        if next_time_ms is not None:
                            if earliest_next_funding is None or int(next_time_ms) < int(earliest_next_funding):
                                earliest_next_funding = int(next_time_ms)

                if min_rate is None:
                    continue

                # Проверка временного окна
                if time_funding_left is not None and earliest_next_funding is not None:
                    next_funding_dt = datetime.fromtimestamp(float(earliest_next_funding) / 1000.0, tz=timezone.utc)
                    time_until_funding = (next_funding_dt - now).total_seconds() / 60.0
                    
                    if time_until_funding > time_funding_left:
                        logger.debug(f"{token}: вне временного окна ({time_until_funding:.1f} мин > {time_funding_left} мин). Пропуск.")
                        continue

                if min_rate >= threshold:
                    logger.debug(f"Токен {token} не превысил порог: {min_rate:.4f}% >= {threshold}%")
                    continue

                # Определяем интервал уведомлений по текущему min_rate
                if -0.1 < min_rate <= threshold:
                    notification_interval = timedelta(hours=8)
                    notification_frequency = "1 раз в 8 часов"
                elif -0.3 < min_rate <= -0.1:
                    notification_interval = timedelta(hours=4)
                    notification_frequency = "1 раз в 4 часа"
                elif -3 < min_rate <= -0.3:
                    notification_interval = timedelta(hours=2)
                    notification_frequency = "1 раз в 2 часа"
                else:
                    continue

                # Проверяем, можно ли отправлять уведомление
                last_notif_info = last_funding_notifications.get(token)
                if last_notif_info is None:
                    can_notify = True
                else:
                    last_notif_time, last_notif_interval = last_notif_info
                    can_notify = (now - last_notif_time) >= notification_interval

                if can_notify:
                    message = f"Токен: {token}\n"
                    message += f"Текущая ставка: {min_rate:.4f}%\n\n"
                    message += f"📊 Ставки на биржах:\n"
                    for exchange, rate in funding_rates.items():
                        message += f"🔹 {exchange}: {rate:.4f}%\n"

                    message += f"\nЧастота уведомлений: {notification_frequency}"

                    await send_funding_message(message)
                    last_funding_notifications[token] = (now, notification_interval)
                    logger.info(f"Отправлено уведомление для {token} (текущий funding: {min_rate:.4f}%, порог: {threshold:.4f}%, интервал: {notification_interval})")
                else:
                    last_notif_time, last_notif_interval = last_funding_notifications[token]
                    remaining_time = (last_notif_time + notification_interval - now).total_seconds() / 60
                    logger.debug(f"Для {token} еще не наступило время уведомления. Осталось: {remaining_time:.1f} минут")
        except Exception as e:
            logger.error(f"Error in check_funding_rates: {e}")
        await asyncio.sleep(30)

async def run_funding_monitor():
    logger.info("=== ЗАПУСК FUNDING MONITOR ===")
    try:
        await check_funding_rates()
    except Exception as e:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА В FUNDING MONITOR: {e}", exc_info=True)

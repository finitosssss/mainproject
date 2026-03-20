import os
import json
import asyncio
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.http import SessionManager
from utils.exchanges import get_exchange_api
from utils.telegram import TelegramManager

load_dotenv()

# Logging configuration
logger = setup_logger("hedge_strategy")

# Telegram configuration
HEDGE_BOT_TOKEN = os.getenv("HEDGE_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CHAT_IDS = [TELEGRAM_CHAT_ID] if TELEGRAM_CHAT_ID else []

async def send_hedge_message(message):
    try:
        await TelegramManager.send_message(
            HEDGE_BOT_TOKEN,
            CHAT_IDS,
            "🔵 0.01% Strategy\n\n" + message,
            parse_mode="MarkdownV2"
        )
        logger.info("Hedge сообщение отправлено")
    except Exception as e:
        logger.error(f"Ошибка отправки hedge сообщения: {e}")

class ConfigManager:
    ACTUAL_CONFIG = ObjectId("69bd4e70e9200a1455df4d59")

    def __init__(self):
        self.mongo_uri = os.getenv("MONGO_URI")
        self.database_name = os.getenv("MONGO_DB_NAME_HEDGE_STRATEGY", "hedge_strategy")
        self.collection_name = os.getenv("MONGO_COLLECTION_NAME_HEDGE_STRATEGY", "hedge_strategy_collection")
        self.cached_config = None
        logger.info(f"ConfigManager initialized for {self.database_name}")

    def load(self):
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.database_name]
            collection = db[self.collection_name]
            config_document = collection.find_one({"_id": self.ACTUAL_CONFIG})
            if not config_document or "tokens" not in config_document:
                logger.error("Configuration not found in MongoDB")
                return None
            self.cached_config = config_document
            return self.cached_config
        except Exception as e:
            logger.error(f"Error loading config: {e}")
        return None

    def _update_config_in_db(self):
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.database_name]
            collection = db[self.collection_name]
            collection.update_one(
                {"_id": self.ACTUAL_CONFIG},
                {"$set": self.cached_config}
            )
        except Exception as e:
            logger.error(f"Error updating config: {e}")

    def remove_deal(self, token, exchange, deal_id):
        try:
            if not self.cached_config:
                logger.error("Конфиг не загружен")
                return False
            
            if token not in self.cached_config["tokens"]:
                logger.warning(f"Токен {token} не найден в конфиге")
                return False
                
            if exchange not in self.cached_config["tokens"][token]["exchanges"]:
                logger.warning(f"Биржа {exchange} не найдена для токена {token}")
                return False
                
            if "deals" not in self.cached_config["tokens"][token]["exchanges"][exchange]:
                logger.warning(f"Структура сделок не найдена для {token}/{exchange}")
                return False
                
            if deal_id not in self.cached_config["tokens"][token]["exchanges"][exchange]["deals"]:
                logger.warning(f"Сделка {deal_id} не найдена для {token}/{exchange}")
                return False
                
            # Удаляем сделку
            del self.cached_config["tokens"][token]["exchanges"][exchange]["deals"][deal_id]
            
            # Если это была последняя сделка на бирже, удаляем всю биржу
            if not self.cached_config["tokens"][token]["exchanges"][exchange]["deals"]:
                del self.cached_config["tokens"][token]["exchanges"][exchange]
                
                # Если это была последняя биржа для токена, удаляем весь токен
                if not self.cached_config["tokens"][token]["exchanges"]:
                    del self.cached_config["tokens"][token]
            
            self._update_config_in_db()
            logger.info(f"Сделка {deal_id} для {token} на {exchange} успешно удалена")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка удаления сделки: {e}")
            return False

    def deactivate_deal(self, token, exchange, deal_id):
        try:
            if not self.cached_config: return False
            if (token not in self.cached_config["tokens"] or 
                exchange not in self.cached_config["tokens"][token]["exchanges"] or 
                deal_id not in self.cached_config["tokens"][token]["exchanges"][exchange]["deals"]):
                return False
            
            deal_data = self.cached_config["tokens"][token]["exchanges"][exchange]["deals"].get(deal_id)
            if deal_data:
                deal_data["active"] = False
                self._update_config_in_db()
                return True
            return False
        except Exception as e:
            logger.error(f"Error deactivating deal: {e}")
            return False

config_manager = ConfigManager()

async def get_funding_rate(exchange_name, symbol):
    try:
        api = get_exchange_api(exchange_name)
        session = await SessionManager.get_session()
        return await api.fetch_funding_rate(session, symbol)
    except Exception as e:
        logger.error(f"Error fetching funding rate from {exchange_name} for {symbol}: {e}")
    return None

async def get_current_price(exchange_name, symbol):
    try:
        api = get_exchange_api(exchange_name)
        session = await SessionManager.get_session()
        return await api.fetch_ticker_price(session, symbol)
    except Exception as e:
        logger.error(f"Error fetching price from {exchange_name} for {symbol}: {e}")
    return None

# Глобальные переменные для уведомлений
last_hedge_unprofit_notifications = {}
hedge_grace_period_start = {}
HEDGE_GRACE_PERIOD = timedelta(minutes=30)

async def check_liquidation_prices(token, token_config):  
    current_config = config_manager.load()
    if not current_config or token not in current_config.get("tokens", {}):
        logger.warning(f"Токен {token} отсутствует в конфиге")
        return
    
    logger.info(f"Проверка ликвидации для {token}")
    
    all_deals = []
    for exchange, exchange_config in token_config["exchanges"].items():
        deals = exchange_config.get("deals", {})
        for deal_id, deal_config in deals.items():
            if not deal_config.get("active", True):
                continue
            all_deals.append({
                "exchange": exchange,
                "deal_id": deal_id,
                "config": deal_config
            })
    
    unique_deal_ids = set(d["deal_id"] for d in all_deals)
    total_deals = len(unique_deal_ids)
    liquidation_number = 0
    liquidation_occurred = False

    for deal in all_deals:
        exchange = deal["exchange"]
        deal_id = deal["deal_id"]
        deal_config = deal["config"]
        
        liquidation_price = deal_config.get("liquidation_price")
        position = deal_config.get("position")
        tokens_amount = deal_config.get("tokens", "N/A")
        
        if None in (liquidation_price, position):
            continue
            
        current_price = await get_current_price(exchange, token)
        if current_price is None:
            continue

        if (position == "long" and current_price <= liquidation_price) or \
           (position == "short" and current_price >= liquidation_price):
            
            liquidation_number += 1
            logger.critical(f"ЛИКВИДАЦИЯ: {token} на {exchange} (Сделка {deal_id})")
            
            message = (
                f"🔥 *ПОЗИЦИЯ ЛИКВИДИРОВАНА!*\n\n"
                f"*Токен:* {token}\n"
                f"*Биржа:* {exchange}\n"
                f"*Сделка №:* {liquidation_number}/{total_deals}\n"
                f"*Тип позиции:* {position}\n"
                f"*Количество токенов:* {tokens_amount}\n"
                f"*Цена ликвидации:* `{liquidation_price:.4f}`\n"
                f"*Текущая цена:* `{current_price:.4f}`"
            )
            
            if liquidation_number == total_deals:
                message += "\n\n🔴🔴🔴 *ПОСЛЕДНЯЯ СДЕЛКА* 🔴🔴🔴"
            
            await send_hedge_message(message)
            
            if not config_manager.deactivate_deal(token, exchange, deal_id):
                logger.error(f"Не удалось деактивировать сделку {deal_id} для {token} на {exchange}")
            
            for paired_exchange, paired_config in token_config["exchanges"].items():
                if paired_exchange != exchange and deal_id in paired_config["deals"]:
                    config_manager.deactivate_deal(token, paired_exchange, deal_id)
            
            liquidation_occurred = True
            await asyncio.sleep(1)
    
    active_deals_after_check = False
    if token in current_config["tokens"]:
        for ex_config in current_config["tokens"][token]["exchanges"].values():
            if any(deal.get("active", True) for deal in ex_config["deals"].values()):
                active_deals_after_check = True
                break

    if not active_deals_after_check:
        tokens_map = current_config.get("tokens", {})
        if token in tokens_map:
            del tokens_map[token]
            config_manager._update_config_in_db()
    
    if liquidation_occurred:
        config_manager.load()

async def check_hedge_unprofitability():
    global last_hedge_unprofit_notifications
    global hedge_grace_period_start

    current_config = config_manager.load()
    if not current_config:
        return

    tokens_cfg = current_config.get("tokens", {})
    now = datetime.now(timezone.utc)

    for token, token_cfg in tokens_cfg.items():
        if not token_cfg.get("active", True):
            continue

        exchanges = token_cfg.get("exchanges", {})
        deals_by_exchange = {}
        for exch, exch_cfg in exchanges.items():
            for deal_id, deal_cfg in exch_cfg.get("deals", {}).items():
                if not deal_cfg.get("active", True):
                    continue
                deals_by_exchange.setdefault(deal_id, []).append((exch, deal_cfg))

        pairs = []
        for deal_id, deals in deals_by_exchange.items():
            if len(deals) < 2:
                continue
            deal_long = next((d for d in deals if d[1].get("position") == "long"), None)
            deal_short = next((d for d in deals if d[1].get("position") == "short"), None)
            if deal_long and deal_short:
                pairs.append((deal_long, deal_short))

        if pairs:
            if token not in hedge_grace_period_start:
                hedge_grace_period_start[token] = now
            if now - hedge_grace_period_start[token] < HEDGE_GRACE_PERIOD:
                if token in last_hedge_unprofit_notifications:
                    last_hedge_unprofit_notifications.pop(token, None)
                continue
        else:
            hedge_grace_period_start.pop(token, None)
            last_hedge_unprofit_notifications.pop(token, None)
            continue

        max_total_cost = None
        max_level = None
        max_interval = None
        max_pair = None
        max_funding_long = None
        max_funding_short = None

        for (exch_long, cfg_long), (exch_short, cfg_short) in pairs:
            funding_long_val = await get_funding_rate(exch_long, token)
            funding_short_val = await get_funding_rate(exch_short, token)
            if funding_long_val is None or funding_short_val is None:
                continue
            total_cost = float(funding_long_val) - float(funding_short_val)

            if total_cost > 0:
                if total_cost < 0.01:
                    interval, level, level_text = timedelta(hours=24), "low", "🟡 *Хедж стал невыгодным*"
                elif 0.01 <= total_cost < 0.05:
                    interval, level, level_text = timedelta(hours=8), "medium", "🟠 *Средне-сильная невыгодность хеджа*"
                else:
                    interval, level, level_text = timedelta(hours=3), "high", "🔴 *Экстремально невыгодный хедж*"
                
                if (max_total_cost is None) or (total_cost > max_total_cost):
                    max_total_cost = total_cost
                    max_level = level
                    max_level_text = level_text
                    max_interval = interval
                    max_pair = (exch_long, exch_short)
                    max_funding_long = funding_long_val
                    max_funding_short = funding_short_val

        if max_total_cost is not None:
            last_notif_info = last_hedge_unprofit_notifications.get(token)
            can_notify = False
            reason = ""
            if last_notif_info is None:
                can_notify, reason = True, "Первое уведомление"
            else:
                last_notif_time, last_notif_interval, last_level = last_notif_info
                level_priority = {"low": 1, "medium": 2, "high": 3}
                if max_level is not None and level_priority[max_level] > level_priority.get(last_level, 0):
                    can_notify, reason = True, f"Рост уровня: {last_level} → {max_level}"
                else:
                    can_notify = (now - last_notif_time) >= max_interval
                    if can_notify: reason = "Таймер истёк"
            
            if can_notify:
                msg = (
                    f"{max_level_text}\n\n"
                    f"🪙 *Токен:* {token}\n"
                    f"📈 *Биржа (лонг):* {max_pair[0]}\n"
                    f"📉 *Биржа (шорт):* {max_pair[1]}\n"
                    f"💸 *Текущий funding (лонг):* `{max_funding_long:.4f}%`\n"
                    f"💰 *Текущий funding (шорт):* `{max_funding_short:.4f}%`\n"
                    f"🧮 *Суммарная стоимость хеджа:* `{max_total_cost:.4f}%`\n"
                    f"\n⏰ *Частота уведомлений:* {max_interval}"
                )
                await send_hedge_message(msg)
                last_hedge_unprofit_notifications[token] = (now, max_interval, max_level)
        else:
            last_hedge_unprofit_notifications.pop(token, None)

async def check_hedge_positions():
    logger.info("Старт мониторинга хедж-позиций")
    while True:
        current_config = config_manager.load()
        if not current_config:
            await asyncio.sleep(60)
            continue
            
        if not current_config.get("global_tracking", True):
            logger.info("Глобальный хедж-мониторинг отключен в БД (global_tracking=false)")
            await asyncio.sleep(60)
            continue

        # Фильтруем только активные токены
        active_tokens = {
            token: cfg for token, cfg in current_config.get("tokens", {}).items()
            if cfg.get("active", True)
        }
        
        if not active_tokens:
            await asyncio.sleep(60)
            continue
            
        for token, token_config in active_tokens.items():
            await check_liquidation_prices(token, token_config)
        
        await check_hedge_unprofitability()
        await asyncio.sleep(60)

async def run_hedge_strategy():
    logger.info("=== ЗАПУСК БОТА HEDGE STRATEGY ===")
    try:
        while True:
            try:
                config = config_manager.load()
                if not config:
                    await asyncio.sleep(60)
                    continue
                await check_hedge_positions()
            except Exception as e:
                logger.error(f"Ошибка в главном цикле hedge: {e}", exc_info=True)
                await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    finally:
        logger.info("=== РАБОТА БОТА ЗАВЕРШЕНА ===")
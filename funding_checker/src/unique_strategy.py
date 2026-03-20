import asyncio
import os
import json
import time
from datetime import datetime
from pymongo import MongoClient
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set, Any
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.http import SessionManager
from utils.exchanges import get_exchange_api
from utils.telegram import TelegramManager

# Load variables from .env file
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME_UNIQUE_STRATEGY", "unique_strategy")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME_UNIQUE_STRATEGY", "unique_strategy_collection")

# Telegram bot configuration
TOKEN = os.getenv("UNIQUE_STRATEGY_TOKEN")
CHAT_IDS = [cid.strip() for cid in os.getenv("UNIQUE_STRATEGY_CHAT_IDS", "").split(",") if cid.strip()]

# Logging configuration
logger = setup_logger("unique_strategy")

# Cache configuration
CACHE_TTL = 60
CONFIG_CACHE_TTL = 300

@dataclass
class CacheEntry:
    data: Any
    timestamp: float
    ttl: float
    
    def is_expired(self) -> bool:
        return time.time() - self.timestamp > self.ttl

_api_cache: Dict[str, CacheEntry] = {}
_config_cache: Dict[str, CacheEntry] = {}

def get_cached_data(cache_key: str, cache_dict: Dict[str, CacheEntry]) -> Optional[Any]:
    if cache_key in cache_dict:
        entry = cache_dict[cache_key]
        if not entry.is_expired(): return entry.data
        else: del cache_dict[cache_key]
    return None

def set_cache_data(cache_key: str, data: Any, ttl: float, cache_dict: Dict[str, CacheEntry]):
    cache_dict[cache_key] = CacheEntry(data=data, timestamp=time.time(), ttl=ttl)

def get_unique_strategy_config():
    cached = get_cached_data("unique_strategy_config", _config_cache)
    if cached is not None: return cached
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        doc = col.find_one({"global_tracking": True})
        client.close()
        result = doc.get("1hour_manipulation", []) if doc else []
        set_cache_data("unique_strategy_config", result, CONFIG_CACHE_TTL, _config_cache)
        return result
    except Exception as e:
        logger.error(f"Error fetching unique strategy config: {e}")
        return []

def calculate_candle_volatility(candle, api):
    ts, o, h, l, c = api.parse_kline(candle)
    if o and h and l and o > 0: return (h - l) / o
    return 0.0

def count_consecutive_red_candles(candles, api):
    if len(candles) < 2: return 0
    red_count = 0
    for i in range(1, len(candles)):
        ts, o, h, l, c = api.parse_kline(candles[i])
        if o and c and c < o: red_count += 1
        else: break
    return red_count

async def check_1hour_manipulation():
    while True:
        try:
            session = await SessionManager.get_session()
            configs = get_unique_strategy_config()
            for cfg in configs:
                if not cfg.get("enabled", True): continue
                
                exchange = cfg.get("exchange", "bybit")
                symbol = cfg.get("symbol")
                timeframe = cfg.get("timeframe", 60)
                min_volatility = cfg.get("min_volatility", 0.008)
                red_threshold = cfg.get("red_candles", 5)
                
                api = get_exchange_api(exchange)
                # Fetch more than needed to ensure we have enough history
                candles = await api.fetch_klines(session, symbol, str(timeframe), red_threshold + 5)
                
                if not candles or len(candles) < red_threshold + 1: continue
                
                # Bybit returns newest first, Binance oldest first. Let's normalize to newest first for logic.
                if exchange.lower() == "binance":
                    candles = candles[::-1]
                
                last_completed = candles[1]
                volatility = calculate_candle_volatility(last_completed, api)
                consecutive_red = count_consecutive_red_candles(candles, api)
                
                ts, o, h, l, c = api.parse_kline(last_completed)
                candle_time = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M')
                
                logger.info(f"Checking {symbol} on {exchange}: Vol={volatility:.4%}, Red={consecutive_red}")
                
                if volatility >= min_volatility and consecutive_red >= red_threshold:
                    cache_key = f"alert_1h_{exchange}_{symbol}_{ts}"
                    if get_cached_data(cache_key, _api_cache) is None:
                        price_change = ((c - o) / o) * 100
                        direction = "📉" if c < o else "📈"
                        msg = (f"🔴 UNIQUE STRATEGY: 1 HOUR MANIPULATION!\n\n"
                               f"🎯 Symbol: {symbol} ({exchange.upper()})\n"
                               f"⏰ Candle Time: {candle_time}\n"
                               f"📊 Consecutive Red: {consecutive_red}\n"
                               f"⚡ Volatility: {volatility:.4%}\n"
                               f"📉 Price Change: {price_change:.2f}%\n"
                               f"{direction} O:{o} -> C:{c}\n"
                               f"💡 Strategy: Found {red_threshold}+ red candles and high volatility.")
                        
                        await TelegramManager.send_message(TOKEN, CHAT_IDS, msg)
                        set_cache_data(cache_key, True, 3600, _api_cache)
                        
        except Exception as e:
            logger.error(f"Error in unique strategy loop: {e}")
        await asyncio.sleep(60)

async def run_unique_strategy():
    logger.info("Starting Unique Strategy Monitor...")
    await check_1hour_manipulation()

if __name__ == "__main__":
    asyncio.run(run_unique_strategy())

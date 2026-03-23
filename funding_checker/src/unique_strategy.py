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

def get_unique_strategy_config(config_key="1hour_manipulation"):
    cache_key = f"unique_strategy_config_{config_key}"
    cached = get_cached_data(cache_key, _config_cache)
    if cached is not None: return cached
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        doc = col.find_one({"global_tracking": True})
        client.close()
        result = doc.get(config_key, []) if doc else []
        set_cache_data(cache_key, result, CONFIG_CACHE_TTL, _config_cache)
        return result
    except Exception as e:
        logger.error(f"Error fetching unique strategy config ({config_key}): {e}")
        return []

def calculate_candle_volatility(candle, api):
    res = api.parse_kline(candle)
    ts, o, h, l, c = res[0], res[1], res[2], res[3], res[4]
    if o and h and l and o > 0: return (h - l) / o
    return 0.0

def count_consecutive_red_candles(candles, api):
    if len(candles) < 2: return 0
    red_count = 0
    for i in range(1, len(candles)):
        res = api.parse_kline(candles[i])
        ts, o, h, l, c = res[0], res[1], res[2], res[3], res[4]
        if o and c and c < o: red_count += 1
        else: break
    return red_count

async def check_1hour_manipulation():
    while True:
        try:
            session = await SessionManager.get_session()
            configs = get_unique_strategy_config("1hour_manipulation")
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
                
                res_l = api.parse_kline(last_completed)
                ts, o, h, l, c = res_l[0], res_l[1], res_l[2], res_l[3], res_l[4]
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
            logger.error(f"Error in 1hour strategy loop: {e}")
        await asyncio.sleep(60)

def get_spot_futures_manipulation_config():
    return get_unique_strategy_config("spot_futures_manipulation")

async def spot_futures_manipulation():
    logger.info("Starting Spot Futures Manipulation loop...")
    while True:
        try:
            session = await SessionManager.get_session()
            configs = get_spot_futures_manipulation_config()
            for cfg in configs:
                if not cfg.get("enabled", True): continue
                
                exchange_name = cfg.get("exchange", "bybit")
                market_type = cfg.get("market_type", "spot")
                symbol_cfg = cfg.get("symbol", "BTCUSDT")
                timeframe = cfg.get("timeframe", 1)
                green_threshold = cfg.get("green_candles", 5)
                vol_multiplier = cfg.get("vol_multiplier", 2.5)
                vol_avg_period = cfg.get("vol_avg_period", 20)
                strict_highs = cfg.get("strict_highs", True)
                
                api = get_exchange_api(exchange_name)
                
                symbols_to_check = []
                if symbol_cfg.lower() in ["spot", "futures", "linear", "all"]:
                    # Fetch available symbols for this category
                    cat = "spot" if symbol_cfg.lower() == "spot" else "linear" if exchange_name.lower() == "bybit" else "futures"
                    symbols_to_check = list(await api.get_available_symbols(session, category=cat))
                    # Limit to top symbols or just first 50 to avoid hammering
                    symbols_to_check = symbols_to_check[:50] 
                else:
                    symbols_to_check = [symbol_cfg]
                for symbol in symbols_to_check:
                    try:
                        logger.info(f"Checking {symbol} for spot/futures manipulation on {exchange_name}...")
                        # Fetch enough candles for both detection and volume average
                        candles = await api.fetch_klines(session, symbol, str(timeframe), max(green_threshold, vol_avg_period) + 5, category=market_type)
                        if not candles or len(candles) < max(green_threshold, vol_avg_period) + 1: continue
                        
                        # Normalize: newest first
                        if not isinstance(candles, list): continue
                        processed_candles = [c for c in candles]
                        if exchange_name.lower() in ["binance", "okx", "bitget", "bingx"]:
                            # Most return oldest first (Binance). Bybit returns newest first.
                            processed_candles.reverse()
                        
                        # Conditions:
                        # 1. Recent green_threshold candles must be green
                        # 2. Each high must be higher than previous (if strict_highs is True)
                        # 3. Last candle volume > multiplier * average volume of previous candles
                        
                        is_pump = True
                        
                        # Check consecutive green and increasing highs
                        for i in range(green_threshold):
                            res = api.parse_kline(processed_candles[i])
                            # ts, o, h, l, c, v
                            o = res[1]
                            c = res[4]
                            h = res[2]
                            
                            if o is None or c is None or c <= o:
                                is_pump = False
                                break
                            if strict_highs and i < green_threshold - 1:
                                res_prev = api.parse_kline(processed_candles[i+1])
                                h_prev = res_prev[2]
                                if h is not None and h_prev is not None and h <= h_prev:
                                    is_pump = False
                                    break
                        
                        if not is_pump: continue
                        
                        # Check abnormal volume on the newest candle (index 0)
                        res_curr = api.parse_kline(processed_candles[0])
                        ts_curr = res_curr[0]
                        v_curr = res_curr[5]
                        
                        volumes = []
                        for i in range(1, vol_avg_period + 1):
                            res_vol = api.parse_kline(processed_candles[i])
                            vol = res_vol[5]
                            if vol is not None:
                                volumes.append(float(vol))
                        
                        if len(volumes) > 3:
                            # Ignore highest and lowest to be more robust (ignoring errors)
                            volumes.sort()
                            robust_volumes = volumes[1:-1]
                            avg_vol = sum(robust_volumes) / len(robust_volumes)
                        elif len(volumes) > 0:
                            avg_vol = sum(volumes) / len(volumes)
                        else:
                            avg_vol = 0.0
                        
                        if v_curr is not None and avg_vol > 0:
                            if float(v_curr) < avg_vol * vol_multiplier:
                                is_pump = False
                        elif v_curr is None:
                            is_pump = False
                        
                        if is_pump and ts_curr is not None and v_curr is not None:
                            candle_time = datetime.fromtimestamp(float(ts_curr)/1000).strftime('%H:%M')
                            cache_key = f"alert_spot_futures_{exchange_name}_{symbol}_{ts_curr}"
                            
                            if get_cached_data(cache_key, _api_cache) is None:
                                msg = (f"🚀 SPOT/FUTURES MANIPULATION! ({market_type.upper()})\n\n"
                                       f"🎯 Symbol: {symbol} ({exchange_name.upper()})\n"
                                       f"⏰ Time: {candle_time}\n"
                                       f"📊 Consecutive Green: {green_threshold}\n"
                                       f"📈 Breaking Highs: {'Yes' if strict_highs else 'No'}\n"
                                       f"🔊 Volume: {v_curr:.2f} (Avg: {avg_vol:.2f}, x{v_curr/avg_vol:.1f})\n"
                                       f"⚡ Status: Slow pump detected on {timeframe}m timeframe.")
                                
                                await TelegramManager.send_message(TOKEN, CHAT_IDS, msg)
                                set_cache_data(cache_key, True, 300, _api_cache)
                                
                    except Exception as sym_e:
                        logger.error(f"Error checking symbol {symbol}: {sym_e}")
                        
        except Exception as e:
            logger.error(f"Error in spot_futures_manipulation loop: {e}")
        await asyncio.sleep(60)

async def run_unique_strategy():
    logger.info("Starting Unique Strategy Monitor...")
    await asyncio.gather(
        check_1hour_manipulation(),
        spot_futures_manipulation()
    )

if __name__ == "__main__":
    asyncio.run(run_unique_strategy())

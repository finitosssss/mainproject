import asyncio
import os
import json
import time
import statistics
from datetime import datetime
from pymongo import MongoClient
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Set, Any
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.http import SessionManager
from utils.exchanges import get_exchange_api, ExchangeAPI
from utils.telegram import TelegramManager

# Load variables from .env file
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME_VOLUME_TRACKER", "volume_tracker")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME_VOLUME_TRACKER", "volume_tracker_collection")

# Telegram bot configuration
TOKEN = os.getenv("VOLUME_TRACKER_TOKEN")
CHAT_IDS = [cid.strip() for cid in os.getenv("VOLUME_TRACKER_CHAT_IDS", "").split(",") if cid.strip()]
bot = TelegramManager.get_bot(TOKEN) if TOKEN else None

# Logging configuration
logger = setup_logger("volume_tracker")

# Cache configuration
CACHE_TTL = 60
CONFIG_CACHE_TTL = 60

@dataclass
class CacheEntry:
    data: Any
    timestamp: float
    ttl: float
    
    def is_expired(self) -> bool:
        return time.time() - self.timestamp > self.ttl

_api_cache: Dict[str, CacheEntry] = {}
_config_cache: Dict[str, CacheEntry] = {}
_active_monitors: Dict[str, asyncio.Task] = {}

def get_cached_data(cache_key: str, cache_dict: Dict[str, CacheEntry]) -> Optional[Any]:
    if cache_key in cache_dict:
        entry = cache_dict[cache_key]
        if not entry.is_expired(): return entry.data
        else: del cache_dict[cache_key]
    return None

def set_cache_data(cache_key: str, data: Any, ttl: float, cache_dict: Dict[str, CacheEntry]):
    cache_dict[cache_key] = CacheEntry(data=data, timestamp=time.time(), ttl=ttl)

def get_all_exchanges_config_from_mongo() -> List[Dict]:
    cached = get_cached_data("all_exchange_configs", _config_cache)
    if cached is not None:
        return cached
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        docs = list(collection.find({"global_tracking": True}))
        client.close()
        
        results = []
        for doc in docs:
            # Support both string and list of exchanges
            exchanges = doc.get("exchange", doc.get("exchanges", "bybit"))
            if isinstance(exchanges, str):
                exchanges = [exchanges]
            
            for ex in exchanges:
                new_doc = doc.copy()
                new_doc["exchange"] = ex.lower().strip()
                results.append(new_doc)
            
        set_cache_data("all_exchange_configs", results, CONFIG_CACHE_TTL, _config_cache)
        return results
    except Exception as e:
        logger.error(f"Error fetching configs from Mongo: {e}")
        return []

def format_symbol_for_exchange(symbol: str, exchange: str) -> str:
    if not symbol: return symbol
    exchange = exchange.lower().strip()
    # OKX, KuCoin and BingX common format: EDGE-USDT
    if exchange in ["okx", "kucoin", "bingx"]:
        if "USDT" in symbol and "-" not in symbol:
            return symbol.replace("USDT", "-USDT")
    # MEXC, Gate and Bitmart often use underscore: EDGE_USDT
    if exchange in ["mexc", "gate", "bitmart"]:
        if "USDT" in symbol and "_" not in symbol:
            return symbol.replace("USDT", "_USDT")
    # HTX uses lowercase: edgeusdt
    if exchange == "htx":
        return symbol.lower()
    # Hyperliquid Spot uses just the base name: EDGE
    if exchange == "hyperliquid":
        if "USDT" in symbol:
            return symbol.replace("USDT", "")
    return symbol

def get_watchlist_for_exchange(exchange_doc: Dict) -> List[str]:
    result = []
    exchange = exchange_doc.get("exchange", "bybit").lower()
    if "tokens" in exchange_doc:
        tokens = exchange_doc["tokens"]
        for t in tokens:
            if isinstance(t, str):
                symbol = t
                enabled = True
            else:
                symbol = t.get(f"{exchange}_symbol", t.get("symbol"))
                enabled = t.get("enabled", True)
            
            if symbol and enabled:
                formatted = format_symbol_for_exchange(symbol, exchange)
                if formatted:
                    result.append(formatted)
    return result

def get_token_config_for_exchange(exchange_doc: Dict) -> Dict:
    result = {}
    exchange = exchange_doc.get("exchange", "bybit").lower()
    
    if "tokens" in exchange_doc:
        for token in exchange_doc["tokens"]:
            if isinstance(token, str):
                continue
            else:
                raw_symbol = token.get(f"{exchange}_symbol", token.get("symbol"))
                symbol = format_symbol_for_exchange(raw_symbol, exchange)
                
                enabled = token.get("enabled", True)
                if symbol and enabled:
                    config = {
                        "minute_volume": token.get("minute_volume", 0),
                        "volatility": token.get("volatility", 0.0),
                        "enabled": enabled
                    }
                    
                    if "green_candles" in token:
                        config["green_candles"] = token["green_candles"]
                    if "volty_handels" in token:
                        config["volty_handels"] = token["volty_handels"]
                    elif "solo_volatility" in token:
                        config["volty_handels"] = token["solo_volatility"]
                    if "tokens_transactions" in token:
                        config["tokens_transactions"] = token["tokens_transactions"]
                    elif "number_of_handles" in token:
                        config["green_candles"] = {
                            "count": token["number_of_handles"],
                            "volatility_thresholds": [config["volatility"]] * token["number_of_handles"]
                        }
                    result[symbol] = config
    return result

# Exchange API implementations are now imported from utils.exchanges

def calculate_candle_volatility(candle, api: ExchangeAPI):
    ts, o, h, l, c, _ = api.parse_kline(candle)
    if l is not None and h is not None and l > 0:
        return (h - l) / l
    return 0.0

def analyze_green_candles_with_volatility(api: ExchangeAPI, candles, required_count, volatility_thresholds):
    if len(candles) < required_count + 1: return False, []
    if len(volatility_thresholds) != required_count: return False, []
    parsed_candles = []
    for c in candles:
        parsed = api.parse_kline(c)
        if parsed[0] is not None:
            parsed_candles.append((c, parsed))
    sorted_candles = sorted(parsed_candles, key=lambda x: x[1][0], reverse=True)
    consecutive_green = 0
    candle_details = []
    volatility_conditions_met = True
    for i in range(1, min(required_count + 1, len(sorted_candles))):
        raw_c, parsed = sorted_candles[i]
        threshold_index = i - 1
        ts, o, h, l, c, _ = parsed
        if o is None or c is None:
            volatility_conditions_met = False
            break
        is_green = c > o
        vol = calculate_candle_volatility(raw_c, api)
        vol_threshold = volatility_thresholds[threshold_index]
        vol_ok = vol <= vol_threshold
        candle_details.append({
            "time": ts,
            "open": o,
            "close": c,
            "is_green": is_green,
            "volatility": vol,
            "volatility_threshold": vol_threshold,
            "volatility_ok": vol_ok
        })
        if is_green and vol_ok: consecutive_green += 1
        else:
            volatility_conditions_met = False
            break
    pattern_found = consecutive_green >= required_count and volatility_conditions_met
    return pattern_found, candle_details

async def check_green_candles_for_symbols(api: ExchangeAPI, symbols_config):
    session = await SessionManager.get_session()
    for symbol, config in symbols_config.items():
        if not config.get("enabled", True) or 'green_candles' not in config: continue
        green_config = config['green_candles']
        required_count = green_config.get('count', 0)
        volatility_thresholds = green_config.get('volatility_thresholds', [])
        if required_count <= 0 or not volatility_thresholds: continue
        try:
            candles = await api.fetch_klines(session, symbol, interval="1", limit=required_count + 3)
            if not candles: continue
            pattern_found, candle_details = analyze_green_candles_with_volatility(api, candles, required_count, volatility_thresholds)
            if pattern_found:
                msg = f"🟢 [{api.exchange_name.upper()}] {symbol}: Обнаружено {required_count} подряд зеленых свечей с подходящей волатильностью!\n\n"
                for i, detail in enumerate(candle_details):
                    msg += f"Свеча {i+1}: волатильность {detail['volatility']:.3%} (≤{detail['volatility_threshold']:.3%})\n"
                cache_key = f"{api.exchange_name}_green_vol_{symbol}_{required_count}"
                if get_cached_data(cache_key, _api_cache) is None:
                    await TelegramManager.send_message(TOKEN, CHAT_IDS, msg, use_markdown=False)
                    set_cache_data(cache_key, True, 300, _api_cache)
        except: pass

async def check_solo_volatility_for_symbols(api: ExchangeAPI, symbols_config):
    session = await SessionManager.get_session()
    for symbol, config in symbols_config.items():
        if not config.get("enabled", True) or 'volty_handels' not in config: continue
        threshold = config['volty_handels']
        if threshold <= 0: continue
        try:
            candles = await api.fetch_klines(session, symbol, interval="1", limit=5)
            if not candles or len(candles) < 4: continue
            parsed_candles = []
            for c in candles:
                parsed = api.parse_kline(c)
                if parsed[0] is not None:
                    parsed_candles.append((c, parsed))
            sorted_candles = sorted(parsed_candles, key=lambda x: x[1][0], reverse=True)
            c1_raw, p1 = sorted_candles[1]
            c2_raw, p2 = sorted_candles[2]
            c3_raw, p3 = sorted_candles[3]
            v1 = calculate_candle_volatility(c1_raw, api)
            v2 = calculate_candle_volatility(c2_raw, api)
            v3 = calculate_candle_volatility(c3_raw, api)
            if v1 == 0.0 or v2 == 0.0 or v3 == 0.0: continue
            if v1 <= threshold and v2 <= threshold and v3 <= threshold:
                import datetime
                t1 = datetime.datetime.fromtimestamp(p1[0] / 1000).strftime('%H:%M')
                t2 = datetime.datetime.fromtimestamp(p2[0] / 1000).strftime('%H:%M')
                t3 = datetime.datetime.fromtimestamp(p3[0] / 1000).strftime('%H:%M')
                msg = (f"📉 [{api.exchange_name.upper()}] {symbol}: Низкая волатильность на 3 свечах!\n\n"
                       f"Свеча 1 ({t1}): {v1:.3%}\n"
                       f"Свеча 2 ({t2}): {v2:.3%}\n"
                       f"Свеча 3 ({t3}): {v3:.3%}\n"
                       f"Порог: ≤{threshold:.3%}")
                cache_key = f"{api.exchange_name}_solo_vol_{symbol}_{p1[0]}_{p2[0]}_{p3[0]}"
                if get_cached_data(cache_key, _api_cache) is None:
                    await TelegramManager.send_message(TOKEN, CHAT_IDS, msg, use_markdown=False)
                    set_cache_data(cache_key, True, 120, _api_cache)
        except: pass

def parse_token_range(tokens_transactions):
    if not tokens_transactions or tokens_transactions == 0: return None
    try:
        if isinstance(tokens_transactions, str) and '-' in tokens_transactions:
            parts = tokens_transactions.split('-')
            if len(parts) == 2:
                mini, maxi = float(parts[0].strip()), float(parts[1].strip())
                if mini >= 0 and maxi >= mini: return (mini, maxi)
        elif isinstance(tokens_transactions, (int, float)) and tokens_transactions > 0:
            return (tokens_transactions * 0.95, tokens_transactions * 1.05)
    except: pass
    return None

async def check_token_transactions_for_symbols(api: ExchangeAPI, symbols_config):
    session = await SessionManager.get_session()
    symbols_to_check = {}
    for symbol, config in symbols_config.items():
        if not config.get("enabled", True) or 'tokens_transactions' not in config: continue
        tokens_range = parse_token_range(config['tokens_transactions'])
        if tokens_range: symbols_to_check[symbol] = tokens_range
    if not symbols_to_check: return
    all_trades = {}
    for symbol in symbols_to_check.keys():
        cache_key = f"{api.exchange_name}_trades_{symbol}_100"
        cached = get_cached_data(cache_key, _api_cache)
        if cached is not None:
            all_trades[symbol] = cached
        else:
            trades = await api.fetch_recent_trades(session, symbol, 100)
            all_trades[symbol] = trades
            set_cache_data(cache_key, trades, CACHE_TTL, _api_cache)

    for symbol, (min_t, max_t) in symbols_to_check.items():
        try:
            trades = all_trades.get(symbol, [])
            if len(trades) < 50: continue
            matching = []
            analyzed = 0
            current_time = time.time()
            ten_mins_ago = (current_time - 600) * 1000
            for t in trades:
                price, size, ts, side = api.parse_trade(t)
                if price is None: continue
                if ts < ten_mins_ago: continue
                if price > 0:
                    analyzed += 1
                    if min_t <= size <= max_t:
                        matching.append({'size': size, 'price': price, 'time': ts, 'side': side})
            if analyzed >= 50:
                m_count = len(matching)
                if m_count >= 40:
                    cache_key = f"{api.exchange_name}_tok_tx_{symbol}_{min_t}_{max_t}"
                    last_alert = get_cached_data(cache_key, _api_cache)
                    if last_alert is None or (current_time - last_alert) > 300:
                        matching.sort(key=lambda x: x['time'], reverse=True)
                        m_perc = (m_count / analyzed) * 100
                        msg = f"💰 [{api.exchange_name.upper()}] {symbol}: МАССОВЫЕ СДЕЛКИ В ДИАПАЗОНЕ!\n\n"
                        msg += f"📊 {m_count} из {analyzed} сделок ({m_perc:.1f}%) в диапазоне {min_t}-{max_t} токенов\n\n"
                        for i, tr in enumerate(matching[:5]):
                            t_fmt = time.strftime('%H:%M:%S', time.gmtime(tr['time']/1000))
                            e = "🟢" if tr['side'] == 'Buy' else "🔴" if tr['side'] == 'Sell' else "⚪"
                            msg += f"{e} {tr['size']:.2f} токенов по ${tr['price']:.6f} в {t_fmt}\n"
                        for chat_id in CHAT_IDS:
                            try:
                                if bot: await bot.send_message(chat_id=chat_id, text=msg)
                            except: pass
                        set_cache_data(cache_key, current_time, 300, _api_cache)
        except: pass

async def check_volume_rest_for_exchange(exchange_doc: Dict):
    exchange_name = exchange_doc.get("exchange", "bybit")
    try:
        api = get_exchange_api(exchange_name)
    except ValueError as e: return
    heartbeat_counter = 0
    while True:
        try:
            all_configs = get_all_exchanges_config_from_mongo()
            current_doc = next((d for d in all_configs if d.get("exchange", "bybit") == exchange_name), exchange_doc)
            watchlist = get_watchlist_for_exchange(current_doc)
            token_config = get_token_config_for_exchange(current_doc)
            logger.info(f"[{exchange_name.upper()}] Watchlist: {watchlist}")
            if not watchlist:
                await asyncio.sleep(60)
                continue
                
            session = await SessionManager.get_session()
            
            srv_time = await api.get_server_time(session)
            min_start = (int(srv_time) // 60 - 1) * 60
            min_start_ms = min_start * 1000
            min_end_ms = (min_start + 59) * 1000
            
            avail_cache_key = f"{api.exchange_name}_avail_symbols"
            avail_symbols = get_cached_data(avail_cache_key, _api_cache)
            if avail_symbols is None:
                avail_symbols = await api.get_available_symbols(session)
                if avail_symbols: 
                    set_cache_data(avail_cache_key, avail_symbols, 300, _api_cache)
                else: 
                    logger.warning(f"[{api.exchange_name.upper()}] get_available_symbols returned empty. Falling back to watchlist.")
                    avail_symbols = set(watchlist)
            
            filtered = [s for s in watchlist if s in avail_symbols][:30]
            if not filtered:
                logger.warning(f"[{api.exchange_name.upper()}] No symbols from watchlist found in available symbols.")
                await asyncio.sleep(60)
                continue

            await check_green_candles_for_symbols(api, token_config)
            await check_solo_volatility_for_symbols(api, token_config)
            await check_token_transactions_for_symbols(api, token_config)
            
            for symbol in filtered:
                try:
                    cfg = token_config.get(symbol)
                    if not cfg: continue
                    
                    cache_key = f"{api.exchange_name}_trades_{symbol}_1000"
                    trades = get_cached_data(cache_key, _api_cache)
                    if trades is None:
                        trades = await api.fetch_recent_trades(session, symbol, 1000)
                        set_cache_data(cache_key, trades, CACHE_TTL, _api_cache)
                    if not trades: continue
                        
                    min_vol = 0.0
                    v_count = 0
                    trade_ts_range = []
                    for t in trades:
                        p, s, ts, side = api.parse_trade(t)
                        if ts: trade_ts_range.append(ts)
                        if ts and p and s and min_start_ms <= ts <= min_end_ms:
                            min_vol += s * p
                            v_count += 1
                    
                    if trade_ts_range:
                        logger.debug(f"[{api.exchange_name.upper()}] {symbol} trade TS range: {min(trade_ts_range)} to {max(trade_ts_range)} (need {min_start_ms}-{min_end_ms})")
                    
                    candles = await api.fetch_klines(session, symbol, "1", 5)
                    volatility = None
                    if candles:
                        cand_ts = []
                        for c in candles:
                            ts, o, h, l, cl, _ = api.parse_kline(c)
                            if ts is not None: cand_ts.append(ts)
                            if ts is not None and min_start_ms - 30000 <= ts <= min_start_ms + 30000:
                                if l and h and l > 0: volatility = (h - l) / l
                                break
                        logger.debug(f"[{api.exchange_name.upper()}] {symbol} candle TS found: {cand_ts}")
  
                    if (min_vol > cfg["minute_volume"]) and (volatility is not None and volatility <= cfg["volatility"]):
                        msg = f"🚨 [{api.exchange_name.upper()}] АНОМАЛЬНАЯ АКТИВНОСТЬ!\n\n"
                        msg += f"{symbol}: объем за минуту {time.strftime('%Y-%m-%d %H:%M', time.gmtime(min_start))} = {min_vol:.2f} $ ({v_count} сделок)\n"
                        
                        volty_str = f"{volatility:.4%}" if volatility is not None else "N/A"
                        threshold_str = f"{cfg['volatility']:.4%}" if cfg.get('volatility') is not None else "N/A"
                        
                        msg += f"волатильность: {volty_str} [Пороги: объем>{cfg['minute_volume']}$, волат<={threshold_str}]"
                        logger.info(f"MATCH! Sending Telegram alert for {symbol} on {api.exchange_name}")
                        for chat_id in CHAT_IDS:
                            try:
                                if bot: await bot.send_message(chat_id=chat_id, text=msg)
                            except Exception as te:
                                logger.error(f"Telegram error: {te}")
                    else:
                        volty_val = f"{volatility:.4%}" if volatility is not None else "N/A"
                        logger.info(f"[{api.exchange_name.upper()}] {symbol}: Vol={min_vol:.2f}$ (need >{cfg['minute_volume']}$), Volty={volty_val} (need <={cfg['volatility']:.4%})")

                except Exception as e:
                    logger.error(f"Error processing {symbol} on {api.exchange_name}: {e}")
            
            logger.info(f"[HEARTBEAT] [{exchange_name.upper()}] Processed {len(filtered)} symbols via REST.")
            
            heartbeat_counter += 1
            if heartbeat_counter % 10 == 0:
                for d in [_api_cache, _config_cache]:
                    exp = [k for k, v in d.items() if v.is_expired()]
                    for k in exp: del d[k]
        except Exception as e:
            logger.error(f'Error in volume_tracker loop: {e}', exc_info=True)
        sleep_time = 60 - (int(time.time()) % 60)
        await asyncio.sleep(max(sleep_time, 1))

async def run_all_exchange_monitors():
    logger.info("Starting all exchange monitors manager...")
    while True:
        try:
            # Re-fetch configurations to pick up new exchanges
            all_exchange_configs = get_all_exchanges_config_from_mongo()
            if not all_exchange_configs:
                logger.warning("No exchange configurations found in MongoDB.")
                all_exchange_configs = [{'exchange': 'bybit'}]
                
            for config in all_exchange_configs:
                exchange_name = config.get("exchange", "bybit").lower().strip()
                # If monitor not active or was crashed/finished, start it
                if exchange_name not in _active_monitors or _active_monitors[exchange_name].done():
                    logger.info(f"Starting monitor task for {exchange_name.upper()}...")
                    task = asyncio.create_task(check_volume_rest_for_exchange(config))
                    _active_monitors[exchange_name] = task
        except Exception as e:
            logger.error(f"Error in monitor manager: {e}")
            
        await asyncio.sleep(60)

async def run_volume_tracker():
    await run_all_exchange_monitors()

if __name__ == "__main__":
    asyncio.run(run_volume_tracker())

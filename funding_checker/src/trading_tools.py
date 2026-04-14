import asyncio
import os
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

from utils.logger import setup_logger
from utils.http import SessionManager
from utils.exchanges import get_exchange_api
from utils.telegram import TelegramManager

# Load variables from .env file
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME_TRADING_TOOLS", "binance_alpha_volume")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME_TRADING_TOOLS", "binance_alpha")

# Telegram bot configuration
TOKEN = os.getenv("TRADING_TOOLS_TOKEN")
CHAT_IDS = os.getenv("TRADING_TOOLS_CHAT_IDS", "").split(",")

def format_trading_tools_symbol(symbol: str, exchange: str) -> str:
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

# Logging configuration
logger = setup_logger("trading_tools")

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

# Global caches
_api_cache: Dict[str, CacheEntry] = {}
_config_cache: Dict[str, CacheEntry] = {}

# Timeframe mapping
TIMEFRAME_MAPPING = {
    1: "1", 5: "5", 15: "15", 30: "30", 60: "60", 240: "240", 1440: "D", 10080: "W"
}

def get_cached_data(cache_key: str, cache_dict: Dict[str, CacheEntry]) -> Optional[Any]:
    if cache_key in cache_dict:
        entry = cache_dict[cache_key]
        if not entry.is_expired(): return entry.data
        else: del cache_dict[cache_key]
    return None

def set_cache_data(cache_key: str, data: Any, ttl: float, cache_dict: Dict[str, CacheEntry]):
    cache_dict[cache_key] = CacheEntry(data=data, timestamp=time.time(), ttl=ttl)

def get_trading_tools_config_from_mongo():
    cached = get_cached_data("trading_tools_config", _config_cache)
    if cached is not None: return cached
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        # Search for any document with global_tracking: True
        doc = collection.find_one({"global_tracking": True})
        client.close()
        
        result = []
        
        if doc:
            # Processing flash_crush_strategy
            if "flash_crush_strategy" in doc:
                strategies = doc["flash_crush_strategy"]
                for strategy in strategies:
                    if strategy.get("enabled", True):
                        strategy_config = {
                            "strategy_type": "flash_crush",
                            "symbol": strategy.get("symbol"),
                            "exchange": strategy.get("exchange", "bybit"),
                            "timeframe": strategy.get("timeframe", 1),
                            "min_volatility": strategy.get("min_volatility", 0.002),
                            "volume": strategy.get("volume", 30000),
                            "rsi": strategy.get("rsi", "off"),
                            "rsi_low": strategy.get("rsi_low", 30),
                            "rsi_high": strategy.get("rsi_high", 70),
                            "rsi_period": strategy.get("rsi_period", 14),
                            "macd": strategy.get("macd", "off"),
                            "macd_fast": strategy.get("macd_fast", 12),
                            "macd_slow": strategy.get("macd_slow", 26),
                            "macd_signal": strategy.get("macd_signal", 9),
                            "macd_signal_type": strategy.get("macd_signal_type", "crossover"),
                            "macd_divergence_periods": strategy.get("macd_divergence_periods", 5),
                            "macd_min_divergence_strength": strategy.get("macd_min_divergence_strength", 0.8),
                            "macd_histogram_threshold": strategy.get("macd_histogram_threshold", 0.001),
                            "macd_zero_cross_confirmation": strategy.get("macd_zero_cross_confirmation", True)
                        }
                        result.append(strategy_config)
            
        
        # Cache the result
        set_cache_data("trading_tools_config", result, CONFIG_CACHE_TTL, _config_cache)
        return result
    except Exception as e:
        logger.error(f"Error fetching trading tools config: {e}")
        return []

async def fetch_kline_data(session, symbol, interval="1", limit=10):
    """Fetch kline/candlestick data from Bybit API"""
    url = f"https://api.bybit.com/v5/market/kline?category=spot&symbol={symbol}&interval={interval}&limit={limit}"
    max_retries = 2
    
    for attempt in range(max_retries):
        try:
            async with session.get(url) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get('Retry-After', 10))
                    await asyncio.sleep(min(retry_after, 30))
                    continue
                elif response.status != 200:
                    logger.warning(f"Kline API error {response.status} for {symbol}")
                    return []
                
                data = await response.json()
                if 'result' not in data or 'list' not in data['result']:
                    logger.warning(f"Invalid kline response for {symbol}")
                    return []
                
                return data['result']['list']
                
        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}, attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
    
    return []

def calculate_candle_volatility(candle):
    """Calculate volatility for a single candle (high-low)/open"""
    try:
        open_price = float(candle[1])
        high_price = float(candle[2])
        low_price = float(candle[3])
        
        if open_price > 0:
            volatility = (high_price - low_price) / open_price
            return volatility
        return 0.0
    except (ValueError, IndexError):
        return 0.0

def calculate_rsi(candles, period=14):
    """Calculate RSI for the given candles with specified period"""
    try:
        if len(candles) < period + 1:
            return None
        
        # Sort candles by timestamp (oldest first for RSI calculation)
        sorted_candles = sorted(candles, key=lambda x: int(x[0]))
        
        # Extract closing prices
        closes = []
        for candle in sorted_candles:
            closes.append(float(candle[4]))  # Close price is at index 4
        
        # Calculate price changes
        deltas = []
        for i in range(1, len(closes)):
            deltas.append(closes[i] - closes[i-1])
        
        # Separate gains and losses
        gains = [delta if delta > 0 else 0 for delta in deltas]
        losses = [-delta if delta < 0 else 0 for delta in deltas]
        
        # Calculate initial averages
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        # Calculate RSI for remaining periods using smoothed averages
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        # Calculate RSI
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    except Exception as e:
        logger.error(f"Error calculating RSI: {e}")
        return None

def calculate_ema(prices, period):
    """Calculate Exponential Moving Average"""
    if len(prices) < period:
        return None
    
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period  # Initial SMA
    
    for i in range(period, len(prices)):
        ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
    
    return ema

def calculate_macd(candles, fast_period=12, slow_period=26, signal_period=9):
    """Calculate MACD indicator with crossover signal"""
    try:
        if len(candles) < slow_period + signal_period:
            return None, None, None, None
        
        # Sort candles by timestamp (oldest first)
        sorted_candles = sorted(candles, key=lambda x: int(x[0]))
        
        # Extract closing prices
        closes = []
        for candle in sorted_candles:
            closes.append(float(candle[4]))
        
        # Calculate EMAs
        fast_ema = []
        slow_ema = []
        
        # Calculate EMA values for each point
        for i in range(slow_period, len(closes)):
            fast_values = closes[i-fast_period+1:i+1]
            slow_values = closes[i-slow_period+1:i+1]
            
            fast_ema.append(calculate_ema(fast_values, fast_period))
            slow_ema.append(calculate_ema(slow_values, slow_period))
        
        # Calculate MACD Line (Fast EMA - Slow EMA)
        macd_line = []
        for i in range(len(fast_ema)):
            if fast_ema[i] is not None and slow_ema[i] is not None:
                macd_line.append(fast_ema[i] - slow_ema[i])
            else:
                macd_line.append(0)
        
        if len(macd_line) < signal_period:
            return None, None, None, None
        
        # Calculate Signal Line (EMA of MACD Line)
        signal_line = calculate_ema(macd_line, signal_period)
        
        # Get current and previous values for crossover detection
        current_macd = macd_line[-1]
        current_signal = signal_line
        
        # Check for crossover (need at least 2 points)
        crossover_signal = None
        if len(macd_line) >= 2:
            prev_macd = macd_line[-2]
            prev_signal_values = macd_line[:-1]
            prev_signal = calculate_ema(prev_signal_values, signal_period) if len(prev_signal_values) >= signal_period else current_signal
            
            # Detect crossover
            if prev_macd <= prev_signal and current_macd > current_signal:
                crossover_signal = "bullish"  # MACD crosses above Signal
            elif prev_macd >= prev_signal and current_macd < current_signal:
                crossover_signal = "bearish"  # MACD crosses below Signal
        
        return current_macd, current_signal, crossover_signal, len(macd_line)
        
    except Exception as e:
        logger.error(f"Error calculating MACD: {e}")
        return None, None, None, None

def calculate_macd_divergence(candles, macd_values, signal_values, divergence_periods=5, min_strength=0.8):
    """Calculate MACD divergence signals"""
    try:
        if len(candles) < divergence_periods * 2 or len(macd_values) < divergence_periods * 2:
            return None, None
        
        # Extract price data (highs and lows)
        prices_high = []
        prices_low = []
        for candle in candles:
            prices_high.append(float(candle[2]))  # High price
            prices_low.append(float(candle[3]))   # Low price
        
        # Look for divergence in the last N periods
        recent_periods = divergence_periods
        
        # Find recent highs and lows in price
        price_highs_recent = prices_high[-recent_periods:]
        price_lows_recent = prices_low[-recent_periods:]
        macd_recent = macd_values[-recent_periods:]
        
        # Find previous highs and lows in price (before recent period)
        price_highs_prev = prices_high[-recent_periods*2:-recent_periods]
        price_lows_prev = prices_low[-recent_periods*2:-recent_periods]
        macd_prev = macd_values[-recent_periods*2:-recent_periods]
        
        if not price_highs_prev or not price_lows_prev or not macd_prev:
            return None, None
        
        # Find peaks and troughs
        recent_price_high = max(price_highs_recent)
        recent_price_low = min(price_lows_recent)
        recent_macd_high = max(macd_recent)
        recent_macd_low = min(macd_recent)
        
        prev_price_high = max(price_highs_prev)
        prev_price_low = min(price_lows_prev)
        prev_macd_high = max(macd_prev)
        prev_macd_low = min(macd_prev)
        
        # Check for bullish divergence (price makes lower low, MACD makes higher low)
        bullish_divergence = False
        bullish_strength = 0
        if recent_price_low < prev_price_low and recent_macd_low > prev_macd_low:
            # Calculate divergence strength
            price_decline = (prev_price_low - recent_price_low) / prev_price_low
            macd_improvement = (recent_macd_low - prev_macd_low) / abs(prev_macd_low) if prev_macd_low != 0 else 0
            bullish_strength = min(price_decline + macd_improvement, 1.0)
            
            if bullish_strength >= min_strength:
                bullish_divergence = True
        
        # Check for bearish divergence (price makes higher high, MACD makes lower high)
        bearish_divergence = False
        bearish_strength = 0
        if recent_price_high > prev_price_high and recent_macd_high < prev_macd_high:
            # Calculate divergence strength
            price_increase = (recent_price_high - prev_price_high) / prev_price_high
            macd_decline = (prev_macd_high - recent_macd_high) / abs(prev_macd_high) if prev_macd_high != 0 else 0
            bearish_strength = min(price_increase + macd_decline, 1.0)
            
            if bearish_strength >= min_strength:
                bearish_divergence = True
        
        # Return divergence type and strength
        if bullish_divergence:
            return "bullish_divergence", bullish_strength
        elif bearish_divergence:
            return "bearish_divergence", bearish_strength
        else:
            return None, None
            
    except Exception as e:
        logger.error(f"Error calculating MACD divergence: {e}")
        return None, None

def calculate_macd_histogram(macd_line, signal_line, threshold=0.001):
    """Calculate MACD histogram and check if it crosses threshold"""
    try:
        if macd_line is None or signal_line is None:
            return None, None
        
        histogram = macd_line - signal_line
        
        # Check if histogram is significant (above threshold)
        histogram_signal = None
        if abs(histogram) >= threshold:
            if histogram > 0:
                histogram_signal = "bullish_histogram"
            else:
                histogram_signal = "bearish_histogram"
        
        return histogram, histogram_signal
        
    except Exception as e:
        logger.error(f"Error calculating MACD histogram: {e}")
        return None, None

def calculate_macd_extended(candles, fast_period=12, slow_period=26, signal_period=9, 
                          signal_type="crossover", divergence_periods=5, min_divergence_strength=0.8,
                          histogram_threshold=0.001, zero_cross_confirmation=True):
    """Extended MACD calculation with multiple signal types"""
    try:
        if len(candles) < slow_period + signal_period + divergence_periods:
            return None, None, None, None, None
        
        # Sort candles by timestamp (oldest first)
        sorted_candles = sorted(candles, key=lambda x: int(x[0]))
        
        # Extract closing prices
        closes = []
        for candle in sorted_candles:
            closes.append(float(candle[4]))
        
        # Calculate all MACD values for divergence analysis
        all_macd_values = []
        all_signal_values = []
        
        # Calculate MACD for sufficient history
        for i in range(slow_period, len(closes)):
            fast_values = closes[i-fast_period+1:i+1]
            slow_values = closes[i-slow_period+1:i+1]
            
            fast_ema = calculate_ema(fast_values, fast_period)
            slow_ema = calculate_ema(slow_values, slow_period)
            
            if fast_ema is not None and slow_ema is not None:
                macd_value = fast_ema - slow_ema
                all_macd_values.append(macd_value)
            else:
                all_macd_values.append(0)
        
        # Calculate signal line for all MACD values
        if len(all_macd_values) >= signal_period:
            for i in range(signal_period-1, len(all_macd_values)):
                signal_values = all_macd_values[i-signal_period+1:i+1]
                signal_ema = calculate_ema(signal_values, signal_period)
                all_signal_values.append(signal_ema if signal_ema is not None else 0)
        
        if not all_macd_values or not all_signal_values:
            return None, None, None, None, None
        
        # Get current values
        current_macd = all_macd_values[-1]
        current_signal = all_signal_values[-1]
        
        # Calculate histogram
        histogram, histogram_signal = calculate_macd_histogram(current_macd, current_signal, histogram_threshold)
        
        signal_result = None
        signal_strength = None
        
        # Handle different signal types
        if signal_type == "crossover":
            # Standard crossover logic
            if len(all_macd_values) >= 2 and len(all_signal_values) >= 2:
                prev_macd = all_macd_values[-2]
                prev_signal = all_signal_values[-2]
                
                if prev_macd <= prev_signal and current_macd > current_signal:
                    signal_result = "bullish_crossover"
                elif prev_macd >= prev_signal and current_macd < current_signal:
                    signal_result = "bearish_crossover"
        
        elif signal_type == "divergence":
            # Divergence analysis
            if len(all_macd_values) >= divergence_periods * 2:
                divergence_candles = sorted_candles[-(divergence_periods * 2):]
                divergence_macd = all_macd_values[-(divergence_periods * 2):]
                divergence_signal = all_signal_values[-(divergence_periods * 2):] if len(all_signal_values) >= divergence_periods * 2 else None
                
                signal_result, signal_strength = calculate_macd_divergence(
                    divergence_candles, divergence_macd, divergence_signal, 
                    divergence_periods, min_divergence_strength
                )
        
        elif signal_type == "zero_cross":
            # Zero line crossover
            if len(all_macd_values) >= 2:
                prev_macd = all_macd_values[-2]
                
                if prev_macd <= 0 and current_macd > 0:
                    signal_result = "bullish_zero_cross"
                elif prev_macd >= 0 and current_macd < 0:
                    signal_result = "bearish_zero_cross"
                
                # Apply confirmation if required
                if zero_cross_confirmation and signal_result:
                    if len(all_signal_values) >= 2:
                        prev_signal = all_signal_values[-2]
                        if not ((prev_macd <= prev_signal and current_macd > current_signal) or 
                               (prev_macd >= prev_signal and current_macd < current_signal)):
                            signal_result = None
        
        return current_macd, current_signal, signal_result, signal_strength, histogram
        
    except Exception as e:
        logger.error(f"Error calculating extended MACD: {e}")
        return None, None, None, None, None


async def check_trading_tools_strategies():
    """Main function to check trading tools strategy conditions"""
    while True:
        try:
            session = await SessionManager.get_session()
            strategies = get_trading_tools_config_from_mongo()
            
            if not strategies:
                logger.info("No trading tools strategies configured or global tracking disabled")
                await asyncio.sleep(60)
                continue
            
            logger.info(f"Checking {len(strategies)} trading tools strategies...")
            
            for strategy in strategies:
                symbol = "UNKNOWN"
                try:
                    strategy_type = strategy.get("strategy_type", "flash_crush")
                    symbol = strategy.get("symbol", "UNKNOWN")
                    exchange = strategy.get("exchange", "bybit")
                    timeframe = strategy["timeframe"]
                    min_volatility = strategy["min_volatility"]
                    
                    if timeframe not in TIMEFRAME_MAPPING:
                        logger.error(f"Unsupported timeframe {timeframe} for {symbol}")
                        continue
                    
                    api_interval = TIMEFRAME_MAPPING[timeframe]
                    api = get_exchange_api(exchange)
                    
                    if strategy_type == "flash_crush":
                        # Flash Crush Strategy
                        volume_threshold = strategy["volume"]
                        rsi_enabled = strategy["rsi"] == "on"
                        rsi_low = strategy["rsi_low"]
                        rsi_high = strategy["rsi_high"]
                        rsi_period = strategy["rsi_period"]
                        macd_enabled = strategy["macd"] == "on"
                        macd_fast = strategy["macd_fast"]
                        macd_slow = strategy["macd_slow"]
                        macd_signal = strategy["macd_signal"]
                        macd_signal_type = strategy["macd_signal_type"]
                        macd_divergence_periods = strategy["macd_divergence_periods"]
                        macd_min_divergence_strength = strategy["macd_min_divergence_strength"]
                        macd_histogram_threshold = strategy["macd_histogram_threshold"]
                        macd_zero_cross_confirmation = strategy["macd_zero_cross_confirmation"]
                        
                        # Required candles for analysis
                        required_candles = 3
                        if rsi_enabled:
                            required_candles = max(required_candles, rsi_period + 5)
                        if macd_enabled:
                            if macd_signal_type == "divergence":
                                required_candles = max(required_candles, macd_slow + macd_signal + macd_divergence_periods * 2 + 5)
                            else:
                                required_candles = max(required_candles, macd_slow + macd_signal + 5)
                        
                        formatted_symbol = format_trading_tools_symbol(symbol, exchange)
                        candles = await api.fetch_klines(session, formatted_symbol, api_interval, required_candles, category="linear")
                        
                        if not candles or len(candles) < 2:
                            logger.warning(f"Not enough candle data for {symbol} ({formatted_symbol}) on {exchange} {timeframe}m")
                            continue
                        
                        # Normalize to newest first for logic
                        sorted_candles = candles
                        if exchange.lower() == "binance":
                            sorted_candles = candles[::-1]
                            
                        last_completed_candle = sorted_candles[1]
                        ts, open_price, high_price, low_price, close_price = api.parse_kline(last_completed_candle)
                        
                        if open_price == 0: continue
                        candle_volatility = (high_price - low_price) / open_price
                        
                        rsi_value = None
                        if rsi_enabled:
                            # calculate_rsi logic (can use helper or local)
                            rsi_value = calculate_rsi(sorted_candles[1:], period=rsi_period)
                        
                        macd_line = None
                        macd_signal_line = None
                        macd_signal_result = None
                        macd_signal_strength = None
                        macd_histogram = None
                        if macd_enabled:
                            macd_line, macd_signal_line, macd_signal_result, macd_signal_strength, macd_histogram = calculate_macd_extended(
                                sorted_candles[1:], 
                                fast_period=macd_fast, 
                                slow_period=macd_slow, 
                                signal_period=macd_signal,
                                signal_type=macd_signal_type,
                                divergence_periods=macd_divergence_periods,
                                min_divergence_strength=macd_min_divergence_strength,
                                histogram_threshold=macd_histogram_threshold,
                                zero_cross_confirmation=macd_zero_cross_confirmation
                            )
                        
                        # Volume calculation
                        # Note: Different exchanges return turnover/volume in different indices. api.parse_kline returns common OHLC.
                        # We might need a generic Turnover method.
                        # For now, let's assume index 6 for turnover if it exists, or 5 for volume.
                        candle_turnover = float(last_completed_candle[6]) if len(last_completed_candle) > 6 else (float(last_completed_candle[5]) * close_price if len(last_completed_candle) > 5 else 0)
                        avg_price = (open_price + high_price + low_price + close_price) / 4
                        timeframe_volume = candle_turnover / avg_price if avg_price > 0 else 0
                        
                        candle_time = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M')
                        
                        rsi_info = f", RSI={rsi_value:.2f}" if rsi_value is not None else ""
                        macd_info = f", MACD={macd_line:.6f}" if macd_line is not None else ""
                        if macd_signal_result:
                            strength_info = f" (strength: {macd_signal_strength:.2f})" if macd_signal_strength else ""
                            macd_info += f", Signal={macd_signal_result}{strength_info}"
                        
                        logger.info(f"TRADING_TOOLS FLASH_CRUSH {symbol} ({exchange} {timeframe}m): vol={candle_volatility:.4%}, volume={timeframe_volume:,.2f}{rsi_info}{macd_info}")
                        
                        basic_conditions_met = candle_volatility >= min_volatility and timeframe_volume >= volume_threshold
                        rsi_condition_met = True
                        if rsi_enabled and rsi_value is not None:
                            rsi_condition_met = rsi_value <= rsi_low or rsi_value >= rsi_high
                        
                        macd_condition_met = True
                        if macd_enabled and macd_signal_result is not None:
                            macd_condition_met = macd_signal_result in ["bullish_crossover", "bearish_crossover", "bullish_divergence", "bearish_divergence", "bullish_zero_cross", "bearish_zero_cross"]
                        
                        if basic_conditions_met and rsi_condition_met and macd_condition_met:
                            cache_key = f"flash_crush_alert_{exchange}_{symbol}_{timeframe}_{ts}"
                            if get_cached_data(cache_key, _api_cache) is None:
                                price_change = ((close_price - open_price) / open_price) * 100
                                direction = "📈" if price_change > 0 else "📉"
                                
                                rsi_alert_info = ""
                                if rsi_enabled and rsi_value is not None:
                                    rsi_status = f"OVERSOLD (≤{rsi_low})" if rsi_value <= rsi_low else f"OVERBOUGHT (≥{rsi_high})" if rsi_value >= rsi_high else "NEUTRAL"
                                    rsi_alert_info = f"📊 RSI: {rsi_value:.2f} ({rsi_status})\n"
                                
                                macd_alert_info = ""
                                if macd_enabled and macd_line is not None and macd_signal_line is not None:
                                    signal_text = ""
                                    if macd_signal_result:
                                        signal_mapping = {"bullish_crossover": "🔥 BULLISH CROSSOVER", "bearish_crossover": "❄️ BEARISH CROSSOVER", "bullish_divergence": "🚀 BULLISH DIVERGENCE", "bearish_divergence": "📉 BEARISH DIVERGENCE", "bullish_zero_cross": "⬆️ BULLISH ZERO CROSS", "bearish_zero_cross": "⬇️ BEARISH ZERO CROSS"}
                                        signal_text = f" ({signal_mapping.get(macd_signal_result, macd_signal_result.upper())})"
                                        if macd_signal_strength: signal_text += f" [Strength: {macd_signal_strength:.1%}]"
                                    histogram_text = f" | Hist: {macd_histogram:.6f}" if macd_histogram is not None else ""
                                    macd_alert_info = f"📈 MACD: {macd_line:.6f} | Signal: {macd_signal_line:.6f}{histogram_text}{signal_text}\n"
                                
                                alert_msg = (f"⚡️ TRADING TOOLS: FLASH CRUSH TRIGGERED!\n\n"
                                           f"🎯 Symbol: {symbol} ({exchange.upper()})\n"
                                           f"⏰ Timeframe: {timeframe} минут\n"
                                           f"📊 Time: {candle_time}\n\n"
                                           f"💥 Volatility: {candle_volatility:.3%} (≥{min_volatility:.3%})\n"
                                           f"💰 Volume: {timeframe_volume:,.2f} tokens (≥{volume_threshold:,} tokens)\n"
                                           f"{rsi_alert_info}"
                                           f"{macd_alert_info}"
                                           f"\n{direction} Price: {open_price} → {close_price} ({price_change:+.2f}%)\n"
                                           f"📈 High: {high_price}\n"
                                           f"📉 Low: {low_price}")
                                
                                await TelegramManager.send_message(TOKEN, CHAT_IDS, alert_msg)
                                set_cache_data(cache_key, True, timeframe * 60, _api_cache)
                    

                                
                except Exception as e:
                    logger.error(f"Error processing strategy for {symbol}: {e}")
            
            # Cleanup caches
            expired_keys = [k for k, v in _api_cache.items() if v.is_expired()]
            for key in expired_keys: del _api_cache[key]
            expired_config_keys = [k for k, v in _config_cache.items() if v.is_expired()]
            for key in expired_config_keys: del _config_cache[key]
            
        except Exception as e:
            logger.error(f"Error in trading tools main loop: {e}")
        
        await asyncio.sleep(60)

async def run_trading_tools():
    """Main entry point for Trading Tools monitor"""
    try:
        logger.info("Starting Trading Tools Strategy Monitor...")
        await check_trading_tools_strategies()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Trading Tools Monitor shutdown complete.")

if __name__ == "__main__":
    asyncio.run(run_trading_tools())

import aiohttp
import asyncio
import json
import time
import logging
from typing import Dict, List, Optional, Tuple, Set, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class ExchangeAPI:
    """Base class for all exchange API implementations."""
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.ws_url = ""
        self.max_ws_subscriptions = 100

    async def get_available_symbols(self, session: aiohttp.ClientSession, category: str = "spot") -> Set[str]:
        raise NotImplementedError

    async def fetch_recent_trades(self, session: aiohttp.ClientSession, symbol: str, limit: int) -> List[Any]:
        raise NotImplementedError

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str, limit: int, category: str = "spot") -> List[Any]:
        raise NotImplementedError

    def parse_kline(self, kline: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
        """Returns (timestamp_ms, open, high, low, close, volume)."""
        raise NotImplementedError

    def parse_trade(self, trade: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
        """Returns (price, size, timestamp_ms, side)."""
        raise NotImplementedError

    async def get_server_time(self, session: aiohttp.ClientSession) -> float:
        return time.time()

    def get_ws_subscription_msg(self, symbols: List[str]) -> List[Any]:
        raise NotImplementedError

    def parse_ws_orderbook_msg(self, msg_data: str, watchlist: List[str]) -> Tuple[Optional[str], List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Returns (symbol, bids, asks)."""
        raise NotImplementedError

    # New methods for funding rates and futures prices
    async def fetch_funding_rate(self, session: aiohttp.ClientSession, symbol: str) -> Tuple[Optional[float], Optional[int]]:
        """Returns (funding_rate_in_percent, next_funding_time_ms)."""
        raise NotImplementedError

    async def fetch_ticker_price(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        """Returns last price."""
        raise NotImplementedError

class BybitAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("bybit")
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.max_ws_subscriptions = 500

    async def get_available_symbols(self, session, category="spot"):
        url = f"https://api.bybit.com/v5/market/instruments-info?category={category}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("result", {}).get("list", [])])
        except Exception as e:
            logger.error(f"Error in BybitAPI.get_available_symbols: {e}")
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.bybit.com/v5/market/recent-trade?category=spot&symbol={symbol}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('result', {}).get('list', [])
        except Exception: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        url = f"https://api.bybit.com/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('result', {}).get('list', [])
        except Exception: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try: return float(trade.get('price', 0)), float(trade.get('size', 0)), float(trade.get('time', 0)), trade.get('side', 'unknown')
        except: return None, None, None, None

    async def get_server_time(self, session):
        url = "https://api.bybit.com/v5/market/time"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("time", time.time() * 1000)) / 1000.0
        except: pass
        return time.time()

    def get_ws_subscription_msg(self, symbols):
        return [{"op": "subscribe", "args": [f"orderbook.1.{s}"]} for s in symbols]

    def parse_ws_orderbook_msg(self, msg_data, watchlist):
        try:
            data = json.loads(msg_data)
            if data.get("topic", "").startswith("orderbook.1."):
                symbol = data["topic"].split(".")[-1]
                if symbol not in watchlist: return None, [], []
                orderbook = data.get("data", {})
                bids = [(float(b[0]), float(b[1])) for b in orderbook.get("b", [])]
                asks = [(float(a[0]), float(a[1])) for a in orderbook.get("a", [])]
                return symbol, bids, asks
        except Exception: pass
        return None, [], []

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}USDT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    item = data["result"]["list"][0]
                    rate = float(item["fundingRate"]) * 100
                    next_time = int(item.get("nextFundingTime", 0))
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}USDT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["result"]["list"][0]["lastPrice"])
        except Exception: pass
        return None

class BinanceAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("binance")
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.max_ws_subscriptions = 100

    async def get_available_symbols(self, session, category="spot"):
        if category == "spot":
            url = "https://api.binance.com/api/v3/exchangeInfo"
        else:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if category == "spot":
                        return set([item["symbol"] for item in data.get("symbols", []) if item.get("status") == "TRADING" and item.get("isSpotTradingAllowed")])
                    else:
                        return set([item["symbol"] for item in data.get("symbols", []) if item.get("status") == "TRADING"])
        except Exception as e:
            logger.error(f"Error in BinanceAPI.get_available_symbols: {e}")
            return set()
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit={min(limit, 1000)}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "240": "4h", "D": "1d"}
        bin_interval = interval_map.get(str(interval), "1m")
        if category == "spot":
            url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={bin_interval}&limit={limit}"
        else:
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={bin_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Buy' if trade.get('isBuyerMaker', False) else 'Sell'
            return float(trade.get('price', 0)), float(trade.get('qty', 0)), float(trade.get('time', 0)), side
        except: return None, None, None, None

    async def get_server_time(self, session):
        url = "https://api.binance.com/api/v3/time"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("serverTime", time.time() * 1000)) / 1000.0
        except: pass
        return time.time()

    def get_ws_subscription_msg(self, symbols):
        streams = [f"{s.lower()}@depth5@100ms" for s in symbols]
        return [{"method": "SUBSCRIBE", "params": streams, "id": 1}]

    def parse_ws_orderbook_msg(self, msg_data, watchlist):
        try:
            data = json.loads(msg_data)
            if "stream" in data and "data" in data:
                symbol = data["stream"].split("@")[0].upper()
                if symbol not in watchlist: return None, [], []
                orderbook = data["data"]
                bids = [(float(b[0]), float(b[1])) for b in orderbook.get("bids", [])]
                asks = [(float(a[0]), float(a[1])) for a in orderbook.get("asks", [])]
                return symbol, bids, asks
        except Exception: pass
        return None, [], []

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}USDT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    rate = float(data["lastFundingRate"]) * 100
                    next_time = int(data.get("nextFundingTime", 0))
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}USDT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["price"])
        except Exception: pass
        return None

class OKXAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("okx")
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.max_ws_subscriptions = 100

    async def get_available_symbols(self, session, category="spot"):
        inst_type = "SPOT" if category == "spot" else "SWAP"
        url = f"https://www.okx.com/api/v5/public/instruments?instType={inst_type}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["instId"] for item in data.get("data", []) if item.get("state") == "live"])
        except Exception as e:
            logger.error(f"Error in OKXAPI.get_available_symbols: {e}")
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://www.okx.com/api/v5/market/trades?instId={symbol}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1H", "240": "4H", "D": "1D"}
        okx_interval = interval_map.get(str(interval), "1m")
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar={okx_interval}&limit={limit}"
        # Note: OKX uses same endpoint for spot and swap history
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Buy' if trade.get('side') == 'buy' else 'Sell'
            return float(trade.get('px', 0)), float(trade.get('sz', 0)), float(trade.get('ts', 0)), side
        except: return None, None, None, None

    async def get_server_time(self, session):
        url = "https://www.okx.com/api/v5/public/system-time"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    ts = data.get('data', [{}])[0].get('ts', time.time()*1000)
                    return float(ts) / 1000.0
        except: pass
        return time.time()

    def get_ws_subscription_msg(self, symbols):
        args = [{"channel": "books5", "instId": s} for s in symbols]
        return [{"op": "subscribe", "args": args}]

    def parse_ws_orderbook_msg(self, msg_data, watchlist):
        try:
            data = json.loads(msg_data)
            if "arg" in data and "data" in data and data.get("action") in ["snapshot", "update"]:
                symbol = data["arg"].get("instId")
                if symbol not in watchlist: return None, [], []
                orderbook_data = data["data"][0] if len(data["data"]) > 0 else {}
                bids = [(float(b[0]), float(b[1])) for b in orderbook_data.get("bids", [])]
                asks = [(float(a[0]), float(a[1])) for a in orderbook_data.get("asks", [])]
                return symbol, bids, asks
        except Exception: pass
        return None, [], []

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol}-USDT-SWAP"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    item = data["data"][0]
                    rate = float(item["fundingRate"]) * 100
                    next_time = int(item.get("nextFundingTime", 0))
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}-USDT-SWAP"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["data"][0]["last"])
        except Exception: pass
        return None

class BitmartAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("bitmart")
        self.ws_url = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
        self.max_ws_subscriptions = 50

    async def get_available_symbols(self, session, category="spot"):
        url = "https://api-cloud.bitmart.com/spot/v1/symbols/details"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("data", {}).get("symbols", []) if item.get("trade_status") == "trading"])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api-cloud.bitmart.com/spot/v1/trades?symbol={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', {}).get('trades', [])
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1", "5": "5", "15": "15", "30": "30", "60": "60", "D": "1440", "240": "240"}
        bitmart_interval = interval_map.get(str(interval), "1")
        current_time_s = int(time.time())
        from_time = current_time_s - (limit * int(bitmart_interval) * 60)
        url = f"https://api-cloud.bitmart.com/spot/v1/symbols/kline?symbol={symbol}&step={bitmart_interval}&from={from_time}&to={current_time_s}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', {}).get('klines', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline.get('timestamp', 0)) * 1000, float(kline.get('open', 0)), float(kline.get('high', 0)), float(kline.get('low', 0)), float(kline.get('close', 0)), float(kline.get('volume', 0))
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Buy' if trade.get('type') == 'buy' else 'Sell'
            return float(trade.get('price', 0)), float(trade.get('amount', 0)), float(trade.get('timestamp', 0)), side
        except: return None, None, None, None

    async def get_server_time(self, session):
        url = "https://api-cloud.bitmart.com/spot/v1/time"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("data", {}).get("server_time", time.time() * 1000)) / 1000.0
        except: pass
        return time.time()

    def get_ws_subscription_msg(self, symbols):
        return [{"op": "subscribe", "args": [f"spot/depth5:{s}"]} for s in symbols]

    def parse_ws_orderbook_msg(self, msg_data, watchlist):
        try:
            data = json.loads(msg_data)
            if data.get("table") == "spot/depth5" and "data" in data and len(data["data"]) > 0:
                item = data["data"][0]
                symbol = item.get("symbol")
                if symbol not in watchlist: return None, [], []
                bids = [(float(b[0]), float(b[1])) for b in item.get("bids", [])]
                asks = [(float(a[0]), float(a[1])) for a in item.get("asks", [])]
                return symbol, bids, asks
        except Exception: pass
        return None, [], []

    async def fetch_funding_rate(self, session, symbol):
        # Bitmart Futures V2 API
        url = f"https://api-cloud-v2.bitmart.com/contract/public/funding-rate?symbol={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == 1000:
                        item = data["data"]
                        rate = float(item["funding_rate"]) * 100
                        # Bitmart returns funding_time as timestamp ms
                        next_time = int(item.get("funding_time", 0))
                        return rate, next_time
        except Exception: pass
        return None, None

class BitgetAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("bitget")
        self.ws_url = "wss://ws.bitget.com/v2/ws/public"
        self.max_ws_subscriptions = 50

    async def get_available_symbols(self, session, category="spot"):
        if category == "spot":
            url = "https://api.bitget.com/api/v2/spot/public/symbols"
        else:
            url = "https://api.bitget.com/api/v2/mix/market/symbols?productType=USDT-FUTURES"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("data", []) if item.get("status") == "online"])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.bitget.com/api/v2/spot/market/fills?symbol={symbol}&limit={min(limit, 500)}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1min", "5": "5min", "15": "15min", "30": "30min", "60": "1h", "D": "1day", "240": "4h"}
        bitget_interval = interval_map.get(str(interval), "1min")
        if category == "spot":
            url = f"https://api.bitget.com/api/v2/spot/market/candles?symbol={symbol}&granularity={bitget_interval}&limit={limit}"
        else:
            url = f"https://api.bitget.com/api/v2/mix/market/candles?symbol={symbol}&granularity={bitget_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Buy' if trade.get('side') == 'buy' else 'Sell'
            return float(trade.get('price', 0)), float(trade.get('size', 0)), float(trade.get('ts', 0)), side
        except: return None, None, None, None

    async def get_server_time(self, session):
        url = "https://api.bitget.com/api/v2/public/time"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("data", {}).get("serverTime", time.time() * 1000)) / 1000.0
        except: pass
        return time.time()

    def get_ws_subscription_msg(self, symbols):
        args = [{"instType": "SP", "channel": "books5", "instId": s} for s in symbols]
        return [{"op": "subscribe", "args": args}]

    def parse_ws_orderbook_msg(self, msg_data, watchlist):
        try:
            data = json.loads(msg_data)
            if data.get("action") == "snapshot" and "arg" in data and "data" in data and len(data["data"]) > 0:
                symbol = data["arg"].get("instId")
                if symbol not in watchlist: return None, [], []
                orderbook_data = data["data"][0]
                bids = [(float(b[0]), float(b[1])) for b in orderbook_data.get("bids", [])]
                asks = [(float(a[0]), float(a[1])) for a in orderbook_data.get("asks", [])]
                return symbol, bids, asks
        except Exception: pass
        return None, [], []

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://api.bitget.com/api/mix/v1/market/ticker?symbol={symbol}USDT_UMCBL"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == "00000":
                        rate = float(data["data"]["fundingRate"]) * 100
                        next_time = int(data["data"].get("nextFundingTime", 0))
                        return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://api.bitget.com/api/mix/v1/market/ticker?symbol={symbol}USDT_UMCBL"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == "00000":
                        return float(data["data"]["lastPr"])
        except Exception: pass
        return None

class BingXAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("bingx")
        self.ws_url = "wss://open-api-ws.bingx.com/market"
        self.max_ws_subscriptions = 50

    async def get_available_symbols(self, session, category="spot"):
        if category == "spot":
            url = "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
        else:
            url = "https://open-api.bingx.com/openApi/swap/v2/quote/allSymbols"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("data", {}).get("symbols", []) if item.get("status") == 1])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://open-api.bingx.com/openApi/spot/v1/market/trades?symbol={symbol}&limit={min(limit, 100)}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "D": "1d", "240": "4h"}
        bingx_interval = interval_map.get(str(interval), "1m")
        if category == "spot":
            url = f"https://open-api.bingx.com/openApi/spot/v2/market/kline?symbol={symbol}&interval={bingx_interval}&limit={limit}"
        else:
            url = f"https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol={symbol}&interval={bingx_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Buy' if trade.get('isBuyerMaker') is False else 'Sell'
            return float(trade.get('price', 0)), float(trade.get('qty', 0)), float(trade.get('time', 0)), side
        except: return None, None, None, None

    async def get_server_time(self, session):
        url = "https://open-api.bingx.com/openApi/spot/v1/common/time"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("data", {}).get("serverTime", time.time() * 1000)) / 1000.0
        except: pass
        return time.time()

    def get_ws_subscription_msg(self, symbols):
        return [{"id": s, "reqType": "sub", "dataType": f"{s}@depth10"} for s in symbols]

    def parse_ws_orderbook_msg(self, msg_data, watchlist):
        try:
            data = json.loads(msg_data)
            datatype = data.get("dataType", "")
            if "@depth10" in datatype and "data" in data:
                symbol = datatype.split("@")[0]
                if symbol not in watchlist: return None, [], []
                orderbook_data = data["data"]
                bids = [(float(b[0]), float(b[1])) for b in orderbook_data.get("bids", [])]
                asks = [(float(a[0]), float(a[1])) for a in orderbook_data.get("asks", [])]
                return symbol, bids, asks
        except Exception: pass
        return None, [], []

    async def fetch_funding_rate(self, session, symbol):
        # BingX Swap V2 API
        url = f"https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex?symbol={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == 0:
                        item = data["data"]
                        rate = float(item["lastFundingRate"]) * 100
                        next_time = int(item.get("nextFundingTime", 0))
                        return rate, next_time
        except Exception: pass
        return None, None

class MEXCAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("mexc")

    async def get_available_symbols(self, session, category="spot"):
        url = "https://api.mexc.com/api/v3/exchangeInfo"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("symbols", []) if item.get("status") == "ENABLED"])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.mexc.com/api/v3/trades?symbol={symbol}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "D": "1d", "240": "4h"}
        mexc_interval = interval_map.get(str(interval), "1m")
        url = f"https://api.mexc.com/api/v3/klines?symbol={symbol}&interval={mexc_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Sell' if trade.get('isBuyerMaker') else 'Buy'
            return float(trade.get('price', 0)), float(trade.get('qty', 0)), float(trade.get('time', 0)), side
        except: return None, None, None, None

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://contract.mexc.com/api/v1/contract/funding_rate?symbol={symbol}_USDT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    item = data["data"][0]
                    rate = float(item["fundingRate"]) * 100
                    next_time = int(item.get("nextFundingTime", 0))
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://contract.mexc.com/api/v1/contract/ticker?symbol={symbol}_USDT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["data"]["lastPrice"])
        except Exception: pass
        return None

class KuCoinAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("kucoin")

    async def get_available_symbols(self, session, category="spot"):
        url = "https://api.kucoin.com/api/v1/symbols"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("data", []) if item.get("enableTrading")])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.kucoin.com/api/v1/market/histories?symbol={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1min", "5": "5min", "15": "15min", "30": "30min", "60": "1hour", "D": "1day", "240": "4hour"}
        ku_interval = interval_map.get(str(interval), "1min")
        url = f"https://api.kucoin.com/api/v1/market/candles?symbol={symbol}&type={ku_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: # KuCoin: [time, open, close, high, low, volume, amount]
            return float(kline[0]) * 1000, float(kline[1]), float(kline[3]), float(kline[4]), float(kline[2]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Buy' if trade.get('side') == 'buy' else 'Sell'
            return float(trade.get('price', 0)), float(trade.get('size', 0)), float(trade.get('time', 0)) / 1000000, side
        except: return None, None, None, None

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://api-futures.kucoin.com/api/v1/funding-rate/{symbol}USDTM/current"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    item = data["data"]
                    rate = float(item["value"]) * 100
                    next_time = int(item.get("timePoint", 0))
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}USDTM"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["data"]["price"])
        except Exception: pass
        return None

class GateAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("gate")

    async def get_available_symbols(self, session, category="spot"):
        url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["id"] for item in data if item.get("trade_status") == "tradable"])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.gateio.ws/api/v4/spot/trades?currency_pair={symbol}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "D": "1d", "240": "4h"}
        gate_interval = interval_map.get(str(interval), "1m")
        url = f"https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair={symbol}&interval={gate_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
        except: pass
        return []

    def parse_kline(self, kline):
        try: # Gate: [ts, volume, close, high, low, open]
            return float(kline[0]) * 1000, float(kline[5]), float(kline[3]), float(kline[4]), float(kline[2]), float(kline[1])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            return float(trade.get('price', 0)), float(trade.get('amount', 0)), float(trade.get('create_time', 0)) * 1000, trade.get('side').capitalize()
        except: return None, None, None, None

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    rate = float(data.get("funding_rate", 0)) * 100
                    next_time = int(data.get("funding_next_apply", 0)) * 1000
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        return float(data[0].get("last", 0))
        except Exception: pass
        return None

class HTXAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("htx")

    async def get_available_symbols(self, session, category="spot"):
        url = "https://api.huobi.pro/v1/common/symbols"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("data", []) if item.get("state") == "online"])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.huobi.pro/market/history/trade?symbol={symbol}&size={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    trades = []
                    for item in data.get('data', []):
                        trades.extend(item.get('data', []))
                    return trades
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1min", "5": "5min", "15": "15min", "30": "30min", "60": "60min", "D": "1day", "240": "4hour"}
        htx_interval = interval_map.get(str(interval), "1min")
        url = f"https://api.huobi.pro/market/history/kline?symbol={symbol}&period={htx_interval}&size={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline.get('id', 0)) * 1000, float(kline.get('open', 0)), float(kline.get('high', 0)), float(kline.get('low', 0)), float(kline.get('close', 0)), float(kline.get('vol', 0))
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            return float(trade.get('price', 0)), float(trade.get('amount', 0)), float(trade.get('ts', 0)), trade.get('side').capitalize()
        except: return None, None, None, None

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://api.huobi.pro/linear-swap-api/v1/swap_funding_rate?contract_code={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    item = data.get("data", {})
                    rate = float(item.get("funding_rate", 0)) * 100
                    next_time = int(item.get("funding_time", 0))
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://api.huobi.pro/linear-swap-api/v1/swap_ticker?contract_code={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    item = data.get("tick", {})
                    return float(item.get("close", 0))
        except Exception: pass
        return None

class HyperliquidAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("hyperliquid")
        self.spot_meta_cache = {}

    async def _get_spot_meta(self, session):
        if not self.spot_meta_cache:
            url = "https://api.hyperliquid.xyz/info"
            payload = {"type": "spotMeta"}
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for i, token in enumerate(data.get("tokens", [])):
                            name = token.get("name")
                            if name:
                                self.spot_meta_cache[name] = f"@{i}"
            except: pass
        return self.spot_meta_cache

    async def get_available_symbols(self, session, category="spot"):
        meta = await self._get_spot_meta(session)
        return set(meta.keys())

    async def fetch_recent_trades(self, session, symbol, limit):
        meta = await self._get_spot_meta(session)
        coin = meta.get(symbol)
        if not coin: return []
        url = "https://api.hyperliquid.xyz/info"
        payload = {"type": "recentTrades", "coin": coin}
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        meta = await self._get_spot_meta(session)
        coin = meta.get(symbol)
        if not coin: return []
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "D": "1d", "240": "4h"}
        hl_interval = interval_map.get(str(interval), "1m")
        url = "https://api.hyperliquid.xyz/info"
        payload = {"type": "candleSnapshot", "req": {"coin": coin, "interval": hl_interval}}
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    def parse_kline(self, kline):
        try: # HL: t (open time), T (close time), o, h, l, c, v
            return float(kline.get('t', 0)), float(kline.get('o', 0)), float(kline.get('h', 0)), float(kline.get('l', 0)), float(kline.get('c', 0)), float(kline.get('v', 0))
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try: # HL: px, sz, side (A/B), time, tid
            side = 'Buy' if trade.get('side') == 'B' else 'Sell'
            return float(trade.get('px', 0)), float(trade.get('sz', 0)), float(trade.get('time', 0)), side
        except: return None, None, None, None

    async def fetch_funding_rate(self, session, symbol):
        url = "https://api.hyperliquid.xyz/info"
        payload = {"type": "metaAndAssetCtx"}
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # data is [meta, assetCtxs]
                    meta = data[0]
                    asset_ctxs = data[1]
                    for i, m in enumerate(meta.get("universe", [])):
                        if m.get("name") == symbol:
                            ctx = asset_ctxs[i]
                            rate = float(ctx.get("funding", 0)) * 100
                            # HL doesn't provide exact next funding time in this endpoint, 
                            # but usually it's every hour.
                            return rate, int(time.time() * 1000) + 3600000
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = "https://api.hyperliquid.xyz/info"
        payload = {"type": "metaAndAssetCtx"}
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    meta = data[0]
                    asset_ctxs = data[1]
                    for i, m in enumerate(meta.get("universe", [])):
                        if m.get("name") == symbol:
                            return float(asset_ctxs[i].get("markPx", 0))
        except Exception: pass
        return None

class AsterdexAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("asterdex")

    async def get_available_symbols(self, session, category="spot"):
        url = "https://api.asterdex.com/fapi/v1/exchangeInfo"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("symbols", [])])
        except: pass
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.asterdex.com/fapi/v1/trades?symbol={symbol}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit, category="spot"):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "D": "1d", "240": "4h"}
        ast_interval = interval_map.get(str(interval), "1m")
        url = f"https://api.asterdex.com/fapi/v1/klines?symbol={symbol}&interval={ast_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4]), float(kline[5])
        except: return None, None, None, None, None, None

    def parse_trade(self, trade):
        try:
            side = 'Buy' if trade.get('isBuyerMaker', False) is False else 'Sell'
            return float(trade.get('price', 0)), float(trade.get('qty', 0)), float(trade.get('time', 0)), side
        except: return None, None, None, None

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://api.asterdex.com/fapi/v1/premiumIndex?symbol={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    rate = float(data.get("lastFundingRate", 0)) * 100
                    next_time = int(data.get("nextFundingTime", 0))
                    return rate, next_time
        except Exception: pass
        return None, None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://api.asterdex.com/fapi/v1/ticker/price?symbol={symbol}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("price", 0))
        except Exception: pass
        return None



def get_exchange_api(name: str) -> ExchangeAPI:
    name = name.lower().strip()
    if name == "bybit": return BybitAPI()
    if name == "binance": return BinanceAPI()
    if name == "okx": return OKXAPI()
    if name == "bitget": return BitgetAPI()
    if name == "bitmart": return BitmartAPI()
    if name == "bingx": return BingXAPI()
    if name == "mexc": return MEXCAPI()
    if name == "kucoin": return KuCoinAPI()
    if name == "gate": return GateAPI()
    if name == "htx": return HTXAPI()
    if name == "hyperliquid": return HyperliquidAPI()
    if name == "asterdex": return AsterdexAPI()
    return BybitAPI()

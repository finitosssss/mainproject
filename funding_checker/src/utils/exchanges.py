import aiohttp
import asyncio
import json
import time
import logging
from typing import Dict, List, Optional, Tuple, Set, Any

logger = logging.getLogger(__name__)

class ExchangeAPI:
    """Base class for all exchange API implementations."""
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.ws_url = ""
        self.max_ws_subscriptions = 100

    async def get_available_symbols(self, session: aiohttp.ClientSession) -> Set[str]:
        raise NotImplementedError

    async def fetch_recent_trades(self, session: aiohttp.ClientSession, symbol: str, limit: int) -> List[Any]:
        raise NotImplementedError

    async def fetch_klines(self, session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> List[Any]:
        raise NotImplementedError

    def parse_kline(self, kline: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
        """Returns (timestamp_ms, open, high, low, close)."""
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
    async def fetch_funding_rate(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        """Returns funding rate in percent (e.g., 0.01 for 0.01%)."""
        raise NotImplementedError

    async def fetch_ticker_price(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        """Returns last price."""
        raise NotImplementedError

class BybitAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("bybit")
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.max_ws_subscriptions = 500

    async def get_available_symbols(self, session):
        url = "https://api.bybit.com/v5/market/instruments-info?category=spot"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("result", {}).get("list", [])])
        except Exception: pass
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

    async def fetch_klines(self, session, symbol, interval, limit):
        url = f"https://api.bybit.com/v5/market/kline?category=spot&symbol={symbol}&interval={interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('result', {}).get('list', [])
        except Exception: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4])
        except: return None, None, None, None, None

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
                    return float(data["result"]["list"][0]["fundingRate"]) * 100
        except Exception: pass
        return None

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

    async def get_available_symbols(self, session):
        url = "https://api.binance.com/api/v3/exchangeInfo"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["symbol"] for item in data.get("symbols", []) if item.get("status") == "TRADING" and item.get("isSpotTradingAllowed")])
        except Exception: return set()
        return set()

    async def fetch_recent_trades(self, session, symbol, limit):
        url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit={min(limit, 1000)}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    async def fetch_klines(self, session, symbol, interval, limit):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "240": "4h", "D": "1d"}
        bin_interval = interval_map.get(str(interval), "1m")
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={bin_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200: return await resp.json()
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4])
        except: return None, None, None, None, None

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
                    return float(data["lastFundingRate"]) * 100
        except Exception: pass
        return None

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

    async def get_available_symbols(self, session):
        url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return set([item["instId"] for item in data.get("data", []) if item.get("state") == "live"])
        except: pass
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

    async def fetch_klines(self, session, symbol, interval, limit):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1H", "240": "4H", "D": "1D"}
        okx_interval = interval_map.get(str(interval), "1m")
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={symbol}&bar={okx_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4])
        except: return None, None, None, None, None

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
                    return float(data["data"][0]["fundingRate"]) * 100
        except Exception: pass
        return None

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

    async def get_available_symbols(self, session):
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

    async def fetch_klines(self, session, symbol, interval, limit):
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
        try: return float(kline.get('timestamp', 0)) * 1000, float(kline.get('open', 0)), float(kline.get('high', 0)), float(kline.get('low', 0)), float(kline.get('close', 0))
        except: return None, None, None, None, None

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

class BitgetAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("bitget")
        self.ws_url = "wss://ws.bitget.com/v2/ws/public"
        self.max_ws_subscriptions = 50

    async def get_available_symbols(self, session):
        url = "https://api.bitget.com/api/v2/spot/public/symbols"
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

    async def fetch_klines(self, session, symbol, interval, limit):
        interval_map = {"1": "1min", "5": "5min", "15": "15min", "30": "30min", "60": "1h", "D": "1day", "240": "4h"}
        bitget_interval = interval_map.get(str(interval), "1min")
        url = f"https://api.bitget.com/api/v2/spot/market/candles?symbol={symbol}&granularity={bitget_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4])
        except: return None, None, None, None, None

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
                        return float(data["data"]["fundingRate"]) * 100
        except Exception: pass
        return None

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

    async def get_available_symbols(self, session):
        url = "https://open-api.bingx.com/openApi/spot/v1/common/symbols"
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

    async def fetch_klines(self, session, symbol, interval, limit):
        interval_map = {"1": "1m", "5": "5m", "15": "15m", "30": "30m", "60": "1h", "D": "1d", "240": "4h"}
        bingx_interval = interval_map.get(str(interval), "1m")
        url = f"https://open-api.bingx.com/openApi/spot/v2/market/kline?symbol={symbol}&interval={bingx_interval}&limit={limit}"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
        except: pass
        return []

    def parse_kline(self, kline):
        try: return float(kline[0]), float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4])
        except: return None, None, None, None, None

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

class MEXCAPI(ExchangeAPI):
    def __init__(self):
        super().__init__("mexc")

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://contract.mexc.com/api/v1/contract/funding_rate?symbol={symbol}_USDT"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["data"][0]["fundingRate"]) * 100
        except Exception: pass
        return None

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

    async def fetch_funding_rate(self, session, symbol):
        url = f"https://api-futures.kucoin.com/api/v1/funding-rate/{symbol}USDTM/current"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["data"]["value"]) * 100
        except Exception: pass
        return None

    async def fetch_ticker_price(self, session, symbol):
        url = f"https://api-futures.kucoin.com/api/v1/ticker?symbol={symbol}USDTM"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["data"]["price"])
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
    return BybitAPI()

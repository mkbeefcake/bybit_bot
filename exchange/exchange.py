import threading
import os
import certifi
from exchange.bybit import ByBitWebSocketPublicStream, BybitWebSocket
from enum import Enum

# Get the cacert.pem path and set SSL_CERT_FILE dynamically for websocket communication
os.environ['SSL_CERT_FILE'] = certifi.where()


def set_kline_interval(interval: int):
    ByBitWebSocketPublicStream.KLINE_INTERVAL = interval
    pass

class ExchangeType(Enum):
    BYBIT = 1
    MEXC = 2

class ExchangeWrapper(ByBitWebSocketPublicStream):
    all_sockets = {}
    exchange = ExchangeType.BYBIT
    lock = threading.Lock()

    @classmethod
    def set_exchange_type(cls, exchange):
        try:
            cls.exchange = exchange
        except Exception as e:
            cls.exchange = ExchangeType.BYBIT

    @classmethod
    def get_session(cls, api_key: str, api_secret: str, symbols: list[str] = [], testnet: bool = False) -> 'BybitWebSocket':
        if not api_key:
            raise ValueError("API Key must be provided")

        with cls.lock:
            if (cls.exchange, api_key) in cls.all_sockets:
                websocket: BybitWebSocket = cls.all_sockets[(cls.exchange, api_key)]
                if symbols != []:
                    ByBitWebSocketPublicStream.set_symbols(list(set(ByBitWebSocketPublicStream.get_symbols() + symbols)))
                return websocket
            else:
                cls.all_sockets[(cls.exchange, api_key)] = BybitWebSocket(
                    api_key=api_key,
                    api_secret=api_secret,
                    symbols=symbols,
                    testnet=testnet
                )
                cls.all_sockets[(cls.exchange, api_key)].start()
                return cls.all_sockets[(cls.exchange, api_key)]

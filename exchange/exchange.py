import threading
import os
import certifi
from exchange.bybit import ByBitWebSocketPublicStream, BybitWebSocket

# Get the cacert.pem path and set SSL_CERT_FILE dynamically for websocket communication
os.environ['SSL_CERT_FILE'] = certifi.where()


def set_kline_interval(interval: int):
    ByBitWebSocketPublicStream.KLINE_INTERVAL = interval
    pass


class ExchangeWrapper(ByBitWebSocketPublicStream):
    all_sockets = {}
    lock = threading.Lock()

    @classmethod
    def get_session(cls, api_key, api_secret, symbols=[], testnet=False) -> 'BybitWebSocket':
        if not api_key:
            raise ValueError("API Key must be provided")

        with cls.lock:
            if api_key in cls.all_sockets:
                websocket: BybitWebSocket = cls.all_sockets[api_key]
                if symbols != []:
                    ByBitWebSocketPublicStream.set_symbols(list(set(ByBitWebSocketPublicStream.get_symbols() + symbols)))
                return websocket
            else:
                cls.all_sockets[api_key] = BybitWebSocket(
                    api_key=api_key,
                    api_secret=api_secret,
                    symbols=symbols,
                    testnet=testnet
                )
                cls.all_sockets[api_key].start()
                return cls.all_sockets[api_key]

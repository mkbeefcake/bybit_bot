import logging
import threading
from fixed_size_queue import FixedSizeQueue
from pybit.unified_trading import WebSocketTrading, WebSocket
from pybit.unified_trading import HTTP
import time
import os
import certifi
import json
import ccxt
# from datatypes import serialize_order_book_response

# Get the cacert.pem path and set SSL_CERT_FILE dynamically for websocket communication
os.environ['SSL_CERT_FILE'] = certifi.where()

BYBIT_ORDER_LEVEL_1 = 1
BYBIT_ORDER_LEVEL_50 = 50
BYBIT_ORDER_LEVEL_200 = 200
BYBIT_ORDER_LEVEL_500 = 500
MAX_SIZE = 20

def set_kline_interval(interval: int):
    ByBitWebSocketPublicStream.KLINE_INTERVAL = interval
    pass

class BybitWebSocketWrapper:
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


class ByBitWebsocketTradingOrder:
    def __init__(self, api_key, api_secret, testnet=False):
        self.trading = WebSocketTrading(testnet=testnet, 
                            api_key=api_key, 
                            api_secret=api_secret,
                            ping_interval=None,
                            trace_logging=False)
        
        self.place_order_event = threading.Event()
        self.amend_order_event = threading.Event()
        self.cancel_order_event = threading.Event()
        pass

    def handle_amend_order_message(self, message):
        logging.info(f"amend_order: {message['data']['orderId']}")
        pass

    # Trading : Update order
    def amend_order(self, category, symbol, order_id, qty):
        self.trading.amend_order(
            None,
            category=category,
            symbol=symbol,
            order_id=order_id,
            qty=qty
        )
        pass

    def handle_cancel_order_message(self, message):
        logging.info(f"cancel_order: {message['data']['orderId']}")
        pass

    # Trading : Cancel order
    def cancel_order(self, category, symbol, orderId):
        if orderId != '':
            self.trading.cancel_order(
                None,
                category=category,
                symbol=symbol,
                order_id=orderId
            )

    _order_id = ""
    def handle_place_order_message(self, message):
        logging.info(f"place_order: {message['data']['orderId']}")
        self._order_id = message['data']['orderId']
        self.place_order_event.set()
        pass

    # Trading : Create order
    def place_order(self, category, symbol, side, orderType, price, qty, timeInForce="PostOnly", reduceOnly = False, closeOnTrigger = False):
        self.trading.place_order(
            self.handle_place_order_message,
            category=category,
            symbol=symbol,
            side=side,
            orderType=orderType,
            price=price,
            qty=qty,
            timeInForce=timeInForce,
            reduceOnly=reduceOnly,
            closeOnTrigger=closeOnTrigger
        )
        self.place_order_event.wait()
        self.place_order_event.clear()
        order_id = self._order_id

        self._order_id = ""
        return order_id


class ByBitWebSocketPublicStream:
    KLINE_INTERVAL = -1

    ws = None
    # orderbook_queue = FixedSizeQueue(max_size=1)
    # trade_queue = FixedSizeQueue(max_size=MAX_SIZE)
    ticker_queue = FixedSizeQueue(max_size=MAX_SIZE)
    kline_queue = FixedSizeQueue(max_size=MAX_SIZE * 5)
    kline_last_sync_time = {}
    symbols = []
    testnet = False

    @classmethod
    def init(cls, symbols: list = [], testnet: bool = False):
        if cls.ws == None:
            cls.ws = WebSocket(testnet=testnet, 
                                channel_type="linear", 
                                ping_interval=None, 
                                trace_logging=False)

            cls.testnet = testnet
            cls.symbols = symbols
            cls.register_public_stream()

        else:
            cls.update_public_stream(symbols)

    @classmethod
    def get_symbols(cls):
        return cls.symbols
    
    @classmethod
    def set_symbols(cls, symbols):
        cls.update_public_stream(symbols)

    @classmethod
    def get_last_item(cls, queue : FixedSizeQueue, symbol=None):
        items = queue.get_all()
        if symbol == None:
            return items[-1] if items else None
        else:
            _items = [item for item in items if item['symbol'] == symbol]
            return _items[-1] if _items else None

    @classmethod
    def get_last_nth_item(cls, queue : FixedSizeQueue, topic=None, nth: int = 1):
        items = queue.get_all()
        if topic == None:
            return items[-1 * nth:] if items else None
        else:
            _items = [item for item in items if item['topic'] == topic]
            return _items[-1 * nth:] if _items else None

    # @classmethod
    # def get_last_orderbook(cls):
    #     return cls.get_last_item(cls.orderbook_queue)
    
    # @classmethod
    # def get_last_trade(cls):
    #     return cls.get_last_item(cls.trade_queue)

    @classmethod
    def get_last_ticker(cls, category, symbol):
        return cls.get_last_item(cls.ticker_queue, symbol=symbol)
    
    @classmethod
    def get_last_klines(cls, symbol, nth):
        topic=f"kline.{ByBitWebSocketPublicStream.KLINE_INTERVAL}.{symbol}"
        kline_data = cls.get_last_nth_item(cls.kline_queue, topic, nth)
        content = [kline['data'][0] for kline in kline_data]
        
        columns = ['open', 'high', 'low', 'close', 'volume']
        for item in content:
            for field in columns:
                item[field] = float(item[field])

        return content

    # @classmethod
    # # Handler Function : Trade
    # def handle_trade(cls, message):
    #     # logging.info(f"trade: {message['topic']} -- {message['ts']} -- {len(message['data'])}")
    #     cls.trade_queue.push(message)
    #     pass

    # @classmethod
    # # Handler Function : Orderbook
    # def handle_orderbook(cls, message):
    #     # logging.info(f"orderbook: {message['topic']} -- {message['ts']} -- {message['data']['s']}")
    #     cls.orderbook_queue.push(message)
    #     pass

    # Handler Function : Ticker
    @classmethod
    def handle_ticker(cls, message):
        # logging.info(f"ticker: {message['topic']} -- {message['ts']} -- {message['data']['symbol']}")
        cls.ticker_queue.push(message['data'])
        pass

    @classmethod
    def handle_kline(cls, message):
        # logging.info(f"kline: {message['topic']} -- {message['ts']} -- {len(message['data'])}")
        topic = message['topic']
        if topic in cls.kline_last_sync_time:
            ts = message['ts']
            if ts - cls.kline_last_sync_time[topic] > ByBitWebSocketPublicStream.KLINE_INTERVAL * 60 * 1000:
                logging.info(f"kline: {message['topic']} -- {message['data'][0]['open']} -- {message['data'][0]['close']} -- {message['data'][0]['high']} -- {message['data'][0]['low']}")
                cls.kline_queue.push(message)
                cls.kline_last_sync_time[topic] = message['ts']
            else:
                cls.kline_queue.update_last(message)
        else:
            logging.info(f"kline: {message['topic']} -- {message['data'][0]['open']} -- {message['data'][0]['close']} -- {message['data'][0]['high']} -- {message['data'][0]['low']}")
            cls.kline_last_sync_time[topic] = message['ts']
            cls.kline_queue.push(message)
        pass

    @classmethod
    def add_previous_2_klines(cls, symbol):
        # symbol = "BTC/USDT:USDT"
        exchange = ccxt.bybit({
            'enableRateLimit': True,
        })
        if cls.testnet == True:
            exchange.set_sandbox_mode(True)

        exchange.options['defaultType'] = 'future'
        # markets = exchange.load_markets()
        # if symbol not in markets:
        #     raise ValueError(f"{symbol} is not a valid market on ByBit spot.")

        timeframe = str(ByBitWebSocketPublicStream.KLINE_INTERVAL) + "m"
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=2)
        data = [{ 
            "topic": f"kline.{ByBitWebSocketPublicStream.KLINE_INTERVAL}.BTCUSDT",
            "data" : [{
                "timestamp": one[0],
                "open": one[1],
                "high": one[2],
                "low": one[3],
                "close": one[4],
                "volume": one[5]
            }],
            "ts": one[0],
            "type": "snapshot"
        } for one in ohlcv ]
        for one in data:
            cls.kline_queue.push(one)

    @classmethod
    def register_public_stream(cls):
        for symbol in cls.symbols:
            cls.add_previous_2_klines(symbol)

            # cls.ws.orderbook_stream(BYBIT_ORDER_LEVEL_50, symbol, cls.handle_orderbook)
            # cls.ws.trade_stream(symbol, cls.handle_trade)
            cls.ws.ticker_stream(symbol, cls.handle_ticker)
            cls.ws.kline_stream(ByBitWebSocketPublicStream.KLINE_INTERVAL, symbol, cls.handle_kline)

            logging.info(f"Symbol: {symbol} -- Registered -- ticker & kline stream")


    @classmethod
    def update_public_stream(cls, symbols):
        for symbol in symbols:
            if symbol not in cls.symbols:
                # cls.ws.orderbook_stream(BYBIT_ORDER_LEVEL_50, symbol, cls.handle_orderbook)
                # cls.ws.trade_stream(symbol, cls.handle_trade)
                cls.ws.ticker_stream(symbol, cls.handle_ticker)
                cls.ws.kline_stream(ByBitWebSocketPublicStream.KLINE_INTERVAL, symbol, cls.handle_kline)
    
                cls.symbols.append(symbol)
                logging.info(f"Symbol: {symbol} -- Updated -- ticker & kline stream")
            
class ByBitWebSocketPrivateStream:
    def __init__(self, api_key, api_secret, testnet=False):
        self.private_ws = WebSocket(testnet=testnet, 
                                    channel_type="private", 
                                    api_key=api_key, 
                                    api_secret=api_secret, 
                                    ping_interval=None,
                                    trace_logging=False)
        self.testnet = testnet
        self.api_key = api_key
        self.api_secret = api_secret

        self.position_lock = threading.Lock()
        self.wallet_lock = threading.Lock()
        self.order_lock = threading.Lock()

        self.open_positions = {}
        self.wallet_balance = {}
        self.open_orders = {}

        pass

    # Handler Function : Position
    def handle_position(self, message):
        logging.info(f"position: {message['topic']} -- {message['creationTime']} -- {message['id']}")
        
        for data in message['data']:
            with self.position_lock:
                category = data["category"]
                symbol = data["symbol"]
                if data['size'] == "0":
                    self.open_positions[(category, symbol)] = []
                else:
                    self.open_positions[(category, symbol)] = [data]

        pass

    # Handler Function : Order
    def handle_orders(self, message):
        logging.info(f"order: {message['topic']} -- {message['creationTime']} -- {message['id']}")

        for data in message['data']:
            with self.order_lock:
                category = data['category']
                symbol = data['symbol']
                
                if data['orderStatus'] == 'Filled' or data['orderStatus'] == 'Cancelled':
                    self.open_orders[(category, symbol)] = []
                else:
                    self.open_orders[(category, symbol)] = [data]

    # Handler Function : Wallet
    def handle_wallet(self, message):
        logging.info(f"wallet: {message['topic']} -- {message['creationTime']} -- {message['id']}")

        for data in message['data']:
            with self.wallet_lock:
                account_type = data["accountType"]
                self.wallet_balance[account_type] = [data]

    def register_private_stream(self):
        self.private_ws.order_stream(callback=self.handle_orders)
        self.private_ws.position_stream(callback=self.handle_position)
        self.private_ws.wallet_stream(callback=self.handle_wallet)
        logging.info(f"Registered -- order - position -- wallet stream")
        pass

    def get_wallet_balance(self, account_type, coin):
        if account_type in self.wallet_balance:
            with self.wallet_lock:
                return self.wallet_balance[account_type]
        else:
            session = HTTP(testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret)
            try:
                balance_info = session.get_wallet_balance(accountType=account_type, coin=coin)
                if balance_info['retCode'] == 0:
                    with self.wallet_lock:
                        if balance_info['result']['list']:
                            self.wallet_balance[account_type] = balance_info['result']['list']
                        else:
                            self.wallet_balance[account_type] = []

            except Exception as e:
                return []
            
        with self.wallet_lock:
            return self.wallet_balance[account_type]

    def get_positions(self, category, symbol):
        if (category, symbol) in self.open_positions:
            with self.position_lock:
                return self.open_positions[(category, symbol)]
        else:
            session = HTTP(testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret)            
            try:
                open_positions = session.get_positions(category=category, symbol=symbol)
                if open_positions['retCode'] == 0:
                    with self.position_lock:
                        if open_positions['result']['list']:
                            self.open_positions[(category, symbol)] = open_positions['result']['list']
                        else:
                            self.open_positions[(category, symbol)] = []
            except Exception as e:
                return []

        with self.position_lock:
            return self.open_positions[(category, symbol)]
    
    def get_open_orders(self, category, symbol):
        if (category, symbol) in self.open_orders:
            with self.order_lock:
                return self.open_orders[(category, symbol)]
        else:
            session = HTTP(testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret)
            try:
                open_orders = session.get_open_orders(category=category, symbol=symbol)
                if open_orders['retCode'] == 0:
                    with self.order_lock:
                        if open_orders['result']['list']:
                            self.open_orders[(category, symbol)] = open_orders['result']['list']
                        else:
                            self.open_orders[(category, symbol)] = []
            except Exception as e:
                return []

        with self.order_lock:
            return self.open_orders[(category, symbol)]

class ByBitRestConsumer:
    def __init__(self, api_key, api_secret, testnet=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        pass

    def set_leverage(self, category, symbol, buy_leverage, sell_leverage):
        logging.info(f"set_leverage: {category} -- {symbol} -- {buy_leverage} -- {sell_leverage}")

        session = HTTP(testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret)
        try:
            leverage_response = session.set_leverage(
                category=category,
                symbol=symbol, 
                buy_leverage=buy_leverage, 
                sell_leverage=sell_leverage
            )
            if leverage_response['retCode'] == 110043:
                logging.info(f"Leverage already set for account {self.api_key}.")
            else:
                logging.info(f"Leverage set response for account {self.api_key}: {leverage_response}")
        except Exception as e:
            logging.info(f"Error setting leverage for account {self.api_key}: {e}")

        pass


class BybitWebSocket(ByBitWebsocketTradingOrder, ByBitWebSocketPrivateStream, ByBitRestConsumer):
    def __init__(self, api_key, api_secret, symbols=[], testnet=False):
        ByBitWebsocketTradingOrder.__init__(self, api_key, api_secret, testnet)
        ByBitWebSocketPrivateStream.__init__(self, api_key, api_secret, testnet)
        ByBitRestConsumer.__init__(self, api_key, api_secret, testnet)

        ByBitWebSocketPublicStream.init(symbols, testnet)

        self.running = False       

    # main running thread
    def run(self):
        logging.info(f"Run() : Started..........")

        self.register_private_stream()

        # Run thread until it marked as running == False
        while self.running == True:
            time.sleep(1)

        logging.info("Run() : Stopped............")

    def start(self):
        self.ws_thread = threading.Thread(target=self.run)
        self.ws_thread.start()
        self.running = True
        pass

    def stop(self):
        self.running = False
        self.ws_thread.join()



# def main(account):
#     websocket = BybitWebSocket(api_key=account['api_key'], 
#                                api_secret=account['api_secret'], 
#                                testnet=True, 
#                                symbols=["BTCUSDT"])

#     websocket.start()
#     time.sleep(3)

#     websocket.get_positions("linear", "BTCUSDT")
#     # websocket.place_order(category="linear",
#     #                         symbol="BTCUSDT",
#     #                         side="Sell",
#     #                         orderType="Limit",
#     #                         price="90000",
#     #                         qty="0.01",
#     #                         timeInForce="PostOnly")
#     time.sleep(30)
#     websocket.stop()
#     websocket.get_last_orderbook()

# # Example usage
# account = {
#     'api_key': 'gExsrmBfeG8mHub03S',
#     'api_secret': '0moXJICRFwXRnnAPDTtk3xcUzRuugHor8PAf'
# }

# if __name__ == "__main__":
#     main(account)
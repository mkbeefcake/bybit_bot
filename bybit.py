import logging
import threading
from fixed_size_queue import FixedSizeQueue
from pybit.unified_trading import WebSocketTrading, WebSocket
from pybit.unified_trading import HTTP
import time
import os
import certifi
from datatypes import serialize_order_book_response


# Get the cacert.pem path and set SSL_CERT_FILE dynamically for websocket communication
os.environ['SSL_CERT_FILE'] = certifi.where()
print(certifi.where())

# Setup logging
logging.basicConfig(
    filename='websocket_bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BYBIT_ORDER_LEVEL_1 = 1
BYBIT_ORDER_LEVEL_50 = 50
BYBIT_ORDER_LEVEL_200 = 200
BYBIT_ORDER_LEVEL_500 = 500
MAX_SIZE = 20

class BybitWebSocketWrapper:
    all_sockets = {}
    lock = threading.Lock()

    @classmethod
    def get_session(cls, api_key, api_secret, symbols, testnet=False) -> 'BybitWebSocket':
        if not api_key:
            raise ValueError("API Key must be provided")

        with cls.lock:
            if api_key in cls.all_sockets:
                return cls.all_sockets[api_key]
            else:
                cls.all_sockets[api_key] = BybitWebSocket(
                    api_key=api_key,
                    api_secret=api_secret,
                    symbols=symbols,
                    testnet=testnet
                )
                return cls.all_sockets[api_key]


class ByBitWebsocketTradingOrder:
    def __init__(self, api_key, api_secret, symbols=[], testnet=False):
        self.trading = WebSocketTrading(testnet=testnet, 
                            api_key=api_key, 
                            api_secret=api_secret)
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

    # Trading : Cancel order
    def cancel_order(self, category, symbol, order_id):
        self.trading.cancel_order(
            None,
            category=category,
            symbol=symbol,
            order_id=order_id
        )

    # Trading : Create order
    def place_order(self, category, symbol, side, orderType, price, qty, timeInForce="PostOnly", reduceOnly = False):
        self.trading.place_order(
            None,
            category=category,
            symbol=symbol,
            side=side,
            orderType=orderType,
            price=price,
            qty=qty,
            timeInForce=timeInForce,
            reduceOnly=reduceOnly
        )
        pass


class ByBitWebSocketPublicStream:
    def __init__(self, api_key, api_secret, symbols=[], testnet=False):
        self.ws = WebSocket(testnet=testnet, 
                            channel_type="linear", 
                            api_key=api_key, 
                            api_secret=api_secret, 
                            trace_logging=False)

        self.orderbook_queue = FixedSizeQueue(max_size=1)
        self.trade_queue = FixedSizeQueue(max_size=MAX_SIZE)
        self.ticker_queue = FixedSizeQueue(max_size=MAX_SIZE)
        pass

    def get_last_item(self, queue):
        items = queue.pop_all()
        return items[-1] if items else None

    def get_last_orderbook(self):
        return self.get_last_item(self.orderbook_queue)
    
    def get_last_trade(self):
        return self.get_last_item(self.trade_queue)

    def get_last_ticker(self):
        return self.get_last_item(self.ticker_queue)

    # Handler Function : Trade
    def handle_trade(self, message):
        self.trade_queue.push(message)
        pass

    # Handler Function : Orderbook
    def handle_orderbook(self, message):
        self.orderbook_queue.push(message)
        pass

    # Handler Function : Ticker
    def handle_ticker(self, message):
        self.ticker_queue.push(message)
        pass

    def register_public_stream(self, symbols):
        for symbol in symbols:
            self.ws.orderbook_stream(BYBIT_ORDER_LEVEL_50, symbol, self.handle_orderbook)
            self.ws.trade_stream(symbol, self.handle_trade)
            self.ws.ticker_stream(symbol, self.handle_ticker)


class ByBitWebSocketPrivateStream:
    def __init__(self, api_key, api_secret, symbols=[], testnet=False):
        self.private_ws = WebSocket(testnet=testnet, 
                                    channel_type="private", 
                                    api_key=api_key, 
                                    api_secret=api_secret, 
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
        print(f"handle_position: {message}")
        
        for data in message.data:
            with self.position_lock:
                category = data["category"]
                symbol = data["symbol"]
                self.open_positions[(category, symbol)] = data

        pass

    # Handler Function : Order
    def handle_orders(self, message):
        print(f"handle_orders: {message}")

        for data in message.data:
            with self.order_lock:
                category = data['category']
                symbol = data['symbol']
                self.open_orders[(category, symbol)] = data

    # Handler Function : Wallet
    def handle_wallet(self, message):
        print(f"handle_wallet: {message}")

        for data in message.data:
            with self.wallet_lock:
                account_type = data["accountType"]
                self.wallet_balance[account_type] = data

    def register_private_stream(self):
        self.private_ws.order_stream(callback=self.handle_orders)
        self.private_ws.position_stream(callback=self.handle_position)
        self.private_ws.wallet_stream(callback=self.handle_wallet)
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
    def __init__(self, api_key, api_secret, symbols=[], testnet=False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        pass

    def set_leverage(self, category, symbol, buy_leverage, sell_leverage):
        session = HTTP(testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret)
        try:
            leverage_response = session.set_leverage(
                category=category,
                symbol=symbol, 
                buy_leverage=buy_leverage, 
                sell_leverage=sell_leverage
            )
            if leverage_response['retCode'] == 110043:
                logging.info(f"Leverage already set for account {account['api_key']}.")
            else:
                logging.info(f"Leverage set response for account {account['api_key']}: {leverage_response}")
        except Exception as e:
            logging.info(f"Error setting leverage for account {account['api_key']}: {e}")

        pass


class BybitWebSocket(ByBitWebsocketTradingOrder, ByBitWebSocketPublicStream, ByBitWebSocketPrivateStream, ByBitRestConsumer):
    def __init__(self, api_key, api_secret, symbols=[], testnet=False):
        ByBitWebsocketTradingOrder.__init__(self, api_key, api_secret, symbols, testnet)
        ByBitWebSocketPublicStream.__init__(self, api_key, api_secret, symbols, testnet)
        ByBitWebSocketPrivateStream.__init__(self, api_key, api_secret, symbols, testnet)
        ByBitRestConsumer.__init__(self, api_key, api_secret, symbols, testnet)
       
        self.running = False
        self.symbols = symbols    

    # main running thread
    def run(self):
        print(f"Run() : Started..........")

        self.register_private_stream()
        self.register_public_stream(self.symbols)

        # Run thread until it marked as running == False
        while self.running == True:
            time.sleep(1)

        print("Run() : Stopped............")

    def start(self):
        self.ws_thread = threading.Thread(target=self.run)
        self.ws_thread.start()
        self.running = True
        pass

    def stop(self):
        self.running = False
        self.ws_thread.join()



def main(account):
    websocket = BybitWebSocket(api_key=account['api_key'], 
                               api_secret=account['api_secret'], 
                               testnet=True, 
                               symbols=["BTCUSDT"])

    websocket.start()
    time.sleep(3)

    websocket.get_positions("linear", "BTCUSDT")
    # websocket.place_order(category="linear",
    #                         symbol="BTCUSDT",
    #                         side="Sell",
    #                         orderType="Limit",
    #                         price="90000",
    #                         qty="0.01",
    #                         timeInForce="PostOnly")
    time.sleep(30)
    websocket.stop()
    websocket.get_last_orderbook()

# Example usage
account = {
    'api_key': 'gExsrmBfeG8mHub03S',
    'api_secret': '0moXJICRFwXRnnAPDTtk3xcUzRuugHor8PAf'
}

if __name__ == "__main__":
    main(account)
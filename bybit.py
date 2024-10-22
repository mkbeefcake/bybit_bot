import logging
import threading
from fixed_size_queue import FixedSizeQueue
from pybit.unified_trading import WebSocketTrading, WebSocket
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
    def get_session(cls, api_key, api_secret, symbols, testnet=False):
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


class BybitWebSocket:
    def __init__(self, api_key, api_secret, symbols=[], testnet=False):
        self.ws = WebSocket(testnet=testnet, 
                            channel_type="linear", 
                            api_key=api_key, 
                            api_secret=api_secret, 
                            trace_logging=False)
        
        self.private_ws = WebSocket(testnet=testnet, 
                                    channel_type="private", 
                                    api_key=api_key, 
                                    api_secret=api_secret, 
                                    trace_logging=False)
        
        self.trading = WebSocketTrading(testnet=testnet, 
                            api_key=api_key, 
                            api_secret=api_secret)

        self.orderbook_queue = FixedSizeQueue(max_size=1)
        self.trade_queue = FixedSizeQueue(max_size=20)

        self.running = False
        self.symbols = symbols

    def get_last_orderbook(self):
        all = self.orderbook_queue.pop_all()
        if all == None:
            return None
        return all[-1]
    
    def get_last_trade(self):
        all = self.trade_queue.pop_all()
        if all == None:
            return None
        return all[-1]        
    
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
    def create_order(self, category, symbol, side, orderType, price, qty, timeInForce="PostOnly"):
        self.trading.place_order(
            None,
            category=category,
            symbol=symbol,
            side=side,
            orderType=orderType,
            price=price,
            qty=qty,
            timeInForce=timeInForce
        )
        pass

    # Handler Function : Trade
    def handle_trade(self, message):
        self.trade_queue.push(message)
        pass

    # Handler Function : Orderbook
    def handle_orderbook(self, message):
        self.orderbook_queue.push(message)
        pass

    # Handler Function : Position
    def handle_position(self, message):
        print(f"handle_position: {message}")
        pass

    # Handler Function : Order
    def handle_orders(self, message):
        print(f"handle_orders: {message}")

    # main running thread
    def run(self):
        print(f"Run() : Started..........")

        # get order information
        self.private_ws.order_stream(callback=self.handle_orders)
        self.private_ws.position_stream(callback=self.handle_position)

        for symbol in self.symbols:
            self.ws.orderbook_stream(BYBIT_ORDER_LEVEL_50, symbol, self.handle_orderbook)
            self.ws.trade_stream(symbol, self.handle_trade)

        # Run thread until it marked as running == False
        while self.running == True:
            time.sleep(1)

        print("Run() : Stopped............")

    def start(self):
        self.ws_thread = threading.Thread(target=self.run)
        self.ws_thread.start()
        self.running = True
        print("Start command !..................")
        pass

    def stop(self):
        self.running = False
        self.ws_thread.join()
        print("Stop command !..................")


    # def get_one(self):
    #     return self.orderbook_queue.pop()
    
    # def get_all(self):
    #     return self.orderbook_queue.pop_all()


def main(account):
    websocket = BybitWebSocket(api_key=account['api_key'], 
                               api_secret=account['api_secret'], 
                               testnet=True, 
                               symbols=["BTCUSDT"])

    websocket.start()
    time.sleep(3)

    # websocket.create_order(category="linear",
    #                         symbol="BTCUSDT",
    #                         side="Sell",
    #                         orderType="Limit",
    #                         price="90000",
    #                         qty="0.01",
    #                         timeInForce="PostOnly")
    time.sleep(30)
    websocket.stop()
    print(websocket.get_last_orderbook())

# Example usage
account = {
    'api_key': 'gExsrmBfeG8mHub03S',
    'api_secret': '0moXJICRFwXRnnAPDTtk3xcUzRuugHor8PAf'
}

if __name__ == "__main__":
    main(account)
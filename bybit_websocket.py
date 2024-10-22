import logging
import threading
from fixed_size_queue import FixedSizeQueue
from pybit.unified_trading import WebSocketTrading, WebSocket
import time
import os
import ssl
import certifi
import json

from dataclasses import dataclass
from typing import List, Tuple


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
MAX_SIZE = 5

@dataclass
class OrderBookResponse:
    topic: str
    type: str
    ts: int
    data: 'OrderBookData'
    cts: int

@dataclass
class OrderBookData:
    s: str  # Symbol
    b: List[Tuple[str, str]]  # Bid prices and quantities
    a: List[Tuple[str, str]]  # Ask prices and quantities
    u: int  # Update ID
    seq: int  # Sequence number

def serialize_order_book_response(message) -> OrderBookResponse:
    """Deserialize a JSON message into an OrderBookSnapshot instance."""
    try:
        order_book_json = message

        # Create OrderBookData instance
        order_book_data = OrderBookData(
            s=order_book_json['data']['s'],
            b=[[bid[0], bid[1]] for bid in order_book_json['data']['b']],
            a=[[ask[0], ask[1]] for ask in order_book_json['data']['a']],
            u=order_book_json['data']['u'],
            seq=order_book_json['data']['seq']
        )

        # Create OrderBookSnapshot instance
        order_book_snapshot = OrderBookResponse(
            topic=order_book_json['topic'],
            type=order_book_json['type'],
            ts=order_book_json['ts'],
            data=order_book_data,
            cts=order_book_json['cts']
        )

        return order_book_snapshot
    
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        return None
    except KeyError as e:
        print(f"Missing key in JSON data: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


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
        

        self.position_queue = FixedSizeQueue(max_size=1)
        self.running = False
        self.symbols = symbols

    # handle position function
    def handle_position(self, message):
        print(f"position message: {message}")
        pass

    # handle orderbook function
    def handle_orderbook(self, message):
        response = serialize_order_book_response(message)
        if response != None:
            self.position_queue.push(response)
        pass

    def get_last_orderbook(self):
        all = self.position_queue.pop_all()
        if all == None:
            return None
        return all[-1]
        

    # main running thread
    def run(self):
        print(f"Run() : Started..........")

        # Get position information
        # self.private_ws.position_stream(self.handle_position)
        
        # get order information
        for symbol in self.symbols:
            self.ws.orderbook_stream(BYBIT_ORDER_LEVEL_50, symbol, self.handle_orderbook)

        # Run thread until it marked as running == False
        while self.running == True:
            time.sleep(1)

        # close the websocket
        try:
            self.ws.exit()
            print("Websocket is closed")
        except Exception as e:
            print("Error closing websocket: ", e)
        pass

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
    #     return self.position_queue.pop()
    
    # def get_all(self):
    #     return self.position_queue.pop_all()


def main(account):
    websocket = BybitWebSocket(api_key=account['api_key'], 
                               api_secret=account['api_secret'], 
                               testnet=True, 
                               symbols=["BTCUSDT"])

    websocket.start()
    time.sleep(10)
    websocket.stop()
    websocket.get_last_orderbook()

# Example usage
account = {
    'api_key': 'gExsrmBfeG8mHub03S',
    'api_secret': '0moXJICRFwXRnnAPDTtk3xcUzRuugHor8PAf'
}

if __name__ == "__main__":
    main(account)
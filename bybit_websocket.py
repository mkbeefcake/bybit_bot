import logging
import threading
from fixed_size_queue import FixedSizeQueue
from pybit.unified_trading import WebSocket
import time
import os
import ssl
import certifi

# Get the cacert.pem path and set SSL_CERT_FILE dynamically for websocket communication
os.environ['SSL_CERT_FILE'] = certifi.where()
print(certifi.where())

# Setup logging
logging.basicConfig(
    filename='websocket_bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

MAX_SIZE = 5

class BybitWebSocket:
    BYBIT_ORDER_LEVEL_1 = 1
    BYBIT_ORDER_LEVEL_50 = 50
    BYBIT_ORDER_LEVEL_200 = 200
    BYBIT_ORDER_LEVEL_500 = 500

    def __init__(self, api_key, api_secret, symbols, testnet=False):
        self.ws = WebSocket(testnet=testnet, 
                            channel_type="linear", 
                            api_key=api_key, 
                            api_secret=api_secret, 
                            trace_logging=True)
        
        self.private_ws = WebSocket(testnet=testnet, 
                                    channel_type="private", 
                                    api_key=api_key, 
                                    api_secret=api_secret, 
                                    trace_logging=True)
        

        self.position_queue = FixedSizeQueue(max_size=MAX_SIZE)
        self.running = True
        self.symbols = symbols

    def handle_orderbook(self, message):
        print(f"orderbook message: {message}")
        pass

    def handle_position(self, message):
        print(f"position message: {message}")
        pass

    def run(self):
        print(f"Run() : Started..........")

        # Get position information
        self.private_ws.position_stream(self.handle_position)
        
        # get order information
        # for symbol in self.symbols:
        #     self.ws.orderbook_stream(BybitWebSocket.BYBIT_ORDER_LEVEL_50, symbol, self.handle_orderbook)

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

    def stop(self):
        self.running = False

    # def get_one(self):
    #     return self.position_queue.pop()
    
    # def get_all(self):
    #     return self.position_queue.pop_all()


def main(account):
    websocket = BybitWebSocket(api_key=account['api_key'], 
                               api_secret=account['api_secret'], 
                               testnet=True, 
                               symbols=["BTCUSDT"])

    # Start the WebSocket thread
    ws_thread = threading.Thread(target=websocket.run)
    ws_thread.start()

    time.sleep(20)

    websocket.stop()  # Stop the WebSocket connection gracefully
    ws_thread.join()  # Wait for WebSocket thread to finish

# Example usage
account = {
    'api_key': 'gExsrmBfeG8mHub03S',
    'api_secret': '0moXJICRFwXRnnAPDTtk3xcUzRuugHor8PAf'
}

if __name__ == "__main__":
    main(account)
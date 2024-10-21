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
    def __init__(self, api_key, api_secret, symbol, testnet=False):
        self.ws = WebSocket(testnet=testnet, channel_type="linear", api_key=api_key, api_secret=api_secret, trace_logging=True)
        self.position_queue = FixedSizeQueue(max_size=MAX_SIZE)
        self.running = True
        self.symbol = symbol

    def handle_orderbook(self, message):
        print(f"orderbook message: {message}")
        self.position_queue.push(message)

    def handle_position(self, message):
        print(f"position message: {message}")

    def run(self):
        print(f"Runner function: started")
        # self.ws.orderbook_stream(10, self.symbol, self.handle_orderbook)
        self.ws.position_stream(self.handle_position)
        while self.running == True:
            time.sleep(1)
        print("Runner function is closed")

    def stop(self):
        self.running = False
        try:
            self.ws.exit()
            print("Websocket is closed")
        except Exception as e:
            print("Error closing websocket: ", e)
        pass

    def get_one(self):
        return self.position_queue.pop()
    
    def get_all(self):
        return self.position_queue.pop_all()


def main(account):
    websocket = BybitWebSocket(api_key=account['api_key'], api_secret=account['api_secret'], testnet=True, symbol="BTCUSDT")

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
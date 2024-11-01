import logging
import threading
import os
import certifi
import threading
import time
import ccxt.pro as ccxtpro
import asyncio
from enum import Enum


# Get the cacert.pem path and set SSL_CERT_FILE dynamically for websocket communication
os.environ['SSL_CERT_FILE'] = certifi.where()

print('CCXT version', ccxtpro.__version__)
# print('Supported Exchanges: ', ccxtpro.exchanges)

class ExchangeType(Enum):
    BYBIT = 1
    MEXC = 2

class ExchangeFactory:
    exchange = ExchangeType.BYBIT
    lock = threading.Lock()
    interval = 5
    all_sockets = {}
    public_socket = None

    @classmethod
    def set_kline_interval(cls, interval: int):
        cls.interval = interval
        pass

    @classmethod
    def set_exchange_type(cls, exchange):
        try:
            cls.exchange = exchange
        except Exception as e:
            cls.exchange = ExchangeType.BYBIT

    @classmethod
    def get_session(cls, api_key: str, api_secret: str, symbols: list[str] = [], testnet: bool = False) -> 'ExchangeWebSocket':
        if not api_key:
            raise ValueError("API Key must be provided")

        with cls.lock:
            if api_key in cls.all_sockets:
                websocket: ExchangeWebSocket = cls.all_sockets[api_key]
                return websocket
            else:
                cls.all_sockets[api_key] = ExchangeWebSocket(
                    exchange_type=cls.exchange,
                    api_key=api_key,
                    api_secret=api_secret,
                    interval=cls.interval,
                    symbols=symbols,
                    testnet=testnet
                )
                return cls.all_sockets[api_key]

    @classmethod
    def get_public_session(cls, testnet: bool = False) -> 'ExchangePublicStream':
        with cls.lock:
            if cls.public_socket == None:
                cls.public_socket = ExchangePublicStream(
                    exchange_type=cls.exchange,
                    interval=cls.interval,
                    testnet=testnet)
                
            return cls.public_socket


class ExchangePublicStream:
    
    def __init__(self, exchange_type, interval, testnet=False):
        self.interval = interval

        if exchange_type == ExchangeType.MEXC:
            self.exchange = ccxtpro.mexc({
                'newUpdates' : False,
            })        
        else:
            self.exchange = ccxtpro.bybit({
                'newUpdates' : False,
            })
        
        if testnet == True:
            self.exchange.set_sandbox_mode(True)

    async def get_last_klines(self, symbol, nth, steps=-1):
        if steps == -1:
            steps = self.interval

        return await self.exchange.watch_ohlcv(symbol=symbol, limit=nth, timeframe=steps)
    
    async def get_last_ticker(self, symbol):
        return await self.exchange.watch_ticker(symbol=symbol)


class ExchangeWebSocket:
    def __init__(self, exchange_type, api_key, api_secret, interval, symbols=[], testnet=False):        
        self.running = True
        self.lock = threading.Lock()
        self.symbols = symbols
        self.testnet = testnet
        self.interval = interval
        
        if exchange_type == ExchangeType.MEXC:
            self.exchange = ccxtpro.mexc({
                'newUpdates' : False,
                'apiKey': api_key,
                'secret': api_secret
            })        
        else:
            self.exchange = ccxtpro.bybit({
                'newUpdates' : False,
                'apiKey': api_key,
                'secret': api_secret
            })

        if testnet == True:
            with self.lock:
                self.exchange.set_sandbox_mode(True)

        _ = asyncio.create_task(self.run())
        pass

    async def run(self):
        while self.running:
            for symbol in self.symbols:
                orderbook = await self.exchange.watch_order_book(symbol)
                print(orderbook['asks'][0], orderbook['bids'][0])

                trades = await self.exchange.watch_trades(symbol)
                print(self.exchange.iso8601(self.exchange.milliseconds()), trades)    

            await asyncio.sleep(1)

    async def stop(self):
        self.running = False
        with self.lock:
            await self.exchange.close()

    async def set_leverage(self, symbol, buy_leverage, sell_leverage):
        logging.info(f"set_leverage: {symbol} -- {buy_leverage} -- {sell_leverage}")
        with self.lock:
            try:                
                await asyncio.sleep(10)
                return await self.exchange.set_leverage(leverage=buy_leverage, symbol=symbol)
            except Exception as e:
                logging.info(f"Error: set_leverage() : {e}")
        

    async def get_open_orders(self, symbol):
        with self.lock:
            try:
                return await self.exchange.watch_orders(symbol=symbol)
            except Exception as e:
                logging.info(f"Error: get_open_orders() : {e}")


    async def get_positions(self, symbol):
        with self.lock:
            try:
                await asyncio.sleep(10)
                return await self.exchange.watch_positions(symbols=[symbol])
            except Exception as e:
                logging.info(f"Error: get_positions() : {e}")

    
    async def get_wallet_balance(self, account_type, coin):
        with self.lock:
            try:
                return await self.exchange.watch_balance()
            except Exception as e:
                logging.info(f"Error: watch_balance() : {e}")
            
        
    async def place_order(self, symbol, side, orderType, price, qty, timeInForce="PostOnly", reduceOnly = False, closeOnTrigger = False):
        order_params = {
            'symbol' : symbol,
            'type' : orderType,     # limit or market
            'side' : side,          # buy or sell
            'amount' : qty
        }

        params = {
            'timeInForce': timeInForce,
            'reduceOnly' : reduceOnly,
            "closeOnTrigger" : closeOnTrigger
        }

        if orderType.lower() == 'limit':
            order_params['price'] = price # add price for limit orders

        with self.lock:
            try:
                return await self.exchange.create_order_ws(**order_params, params=params)
            except Exception as e:
                logging.info(f"Error: place_order() : {e}")
            
    
    async def cancel_order(self, symbol, orderId):
        with self.lock:
            try:    
                return await self.exchange.cancel_order_ws(id=orderId, symbol=symbol)
            except Exception as e:
                logging.info(f"Error: cancel_order() : {e}")
            
    


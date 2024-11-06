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
    def get_public_session(cls, symbols = [], testnet: bool = False) -> 'ExchangePublicStream':
        with cls.lock:
            if cls.public_socket == None:
                cls.public_socket = ExchangePublicStream(
                    exchange_type=cls.exchange,
                    symbols=symbols,
                    interval=cls.interval,
                    testnet=testnet)
                
            return cls.public_socket


class ExchangePublicStream:
    LIMITED_ITEMS = 4

    def __init__(self, exchange_type, interval, symbols, testnet=False):
        self.interval = interval
        self.running = True
        self.symbols = symbols
        self.lock = asyncio.Lock()

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

        _ = asyncio.create_task(self.run())
        pass

    async def run(self):
        await self.exchange.load_markets()
        while self.running:
            # for symbol in self.symbols:
            #     candles = await self.exchange.watch_ohlcv(symbol, timeframe=f"{self.interval}m", limit=ExchangePublicStream.LIMITED_ITEMS)
            #     print(f">>> OHLCV >>> " + self.exchange.iso8601(self.exchange.milliseconds()), candles)

            #     ticker = await self.exchange.watch_ticker(symbol)
            #     print(f">>> TICKER >>> " + self.exchange.iso8601(self.exchange.milliseconds()), ticker)

            await asyncio.sleep(1)

    async def stop(self):
        self.running = False
        async with self.lock:
            await self.exchange.close()

    async def get_last_klines(self, symbol, nth, steps=-1):
        if steps == -1:
            steps = self.interval

        # return await self.exchange.watch_ohlcv(symbol=symbol, limit=nth, timeframe=f"{steps}m")
        return await self.exchange.fetch_ohlcv(symbol=symbol, limit=nth, timeframe=f"{steps}m")
    
    async def get_last_ticker(self, symbol):
        ticker = await self.exchange.watch_ticker(symbol=symbol)
        return ticker['info']


class ExchangeWebSocket:
    def __init__(self, exchange_type, api_key, api_secret, interval, symbols=[], testnet=False):        
        self.running = True
        self.lock = asyncio.Lock()
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
            self.exchange.set_sandbox_mode(True)

        _ = asyncio.create_task(self.run())
        pass

    async def run(self):
        await self.exchange.load_markets()
        while self.running:
            for symbol in self.symbols:
                try:
                    # trades = await self.exchange.fetch_my_trades()
                    # print(f">>> MY TRADES >>> " + self.exchange.iso8601(self.exchange.milliseconds()), trades)    
                    await asyncio.sleep(1)
                except Exception as e:
                    print(f">>> ERROR : MY TRADES >>> " + e)    

            await asyncio.sleep(1)

    async def stop(self):
        self.running = False
        async with self.lock:
            await self.exchange.close()

    async def set_leverage(self, symbol, buy_leverage, sell_leverage):
        logging.info(f"set_leverage: {symbol} -- {buy_leverage} -- {sell_leverage}")
        async with self.lock:
            return await self.exchange.set_leverage(leverage=buy_leverage, symbol=symbol)
        

    async def get_open_orders(self, symbol):
        async with self.lock:
            try:
                return await self.exchange.fetch_open_orders(symbol=symbol)
            except Exception as e:
                logging.info(f"Error: get_open_orders() : {e}")


    async def get_positions(self, symbol):
        async with self.lock:
            try:
                positions = await self.exchange.fetch_positions(symbols=[symbol])
                positions = [position['info'] for position in positions]
                return positions
            except Exception as e:
                logging.info(f"Error: get_positions() : {e}")
                return []

    
    async def get_wallet_balance(self, account_type, coin):
        async with self.lock:
            try:
                return await self.exchange.fetch_balance()
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

        async with self.lock:
            try:
                return await self.exchange.create_order(**order_params, params=params)
            except Exception as e:
                logging.info(f"Error: place_order() : {e}")
            
    
    async def cancel_order(self, symbol, orderId):
        async with self.lock:
            try:    
                return await self.exchange.cancel_order(id=orderId, symbol=symbol)
            except Exception as e:
                logging.info(f"Error: cancel_order() : {e}")
            
    


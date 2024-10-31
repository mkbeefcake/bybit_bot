import datetime
from datetime import timezone
import time
import json
import threading
from pybit.exceptions import InvalidRequestError
import math
import argparse
import logging
import os
import requests
import pandas as pd
import ccxt
from bybit import BybitWebSocketWrapper, BybitWebSocket, set_kline_interval, ByBitWebSocketPublicStream


m_valid_qty=0
m_side = "DEFAULT"
m_testnet = True
m_leverage = 10

# Setup logging
logging.basicConfig(
    filename='trading_bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info(f"Start!")


def get_utc_current_time():
    timestamp = datetime.datetime.now()
    timestamp = timestamp.astimezone(timezone.utc)    
    return timestamp

def get_current(step=15, howMany=1):
    # original code

    # symbol = "BTC/USDT:USDT"
    # exchange = ccxt.bybit({
    #     'enableRateLimit': True
    # })
    # exchange.options['defaultType'] = 'future'
    # markets = exchange.load_markets()

    # if symbol not in markets:
    #     raise ValueError(f"{symbol} is not a valid market on ByBit spot.")

    # timeframe = str(step) + "m"
    # ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=howMany)
    
    # Removed the websocket code
    ohlcv = ByBitWebSocketPublicStream.get_last_klines("BTCUSDT", nth=howMany)

    columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    data = pd.DataFrame(ohlcv, columns=columns)
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    
    return data

def get_diff(step,howMany):

    data = get_current(step,howMany)
    data['high_low_diff'] = data['high'] - data['low']
    logging.info(f"inside get_diff data is {data}")
    int_row=data.iloc[-2]
    logging.info(f"inside get_diff market irregularities is {int_row}")
    return int_row['high_low_diff']


GET_ALL_ACCOUNTS_URL = 'https://cryptopredictor.ai/get_all_accounts.php'
UPDATE_ACCOUNT_URL = 'https://cryptopredictor.ai/update_all_accounts.php'

def fetch_all_accounts():
    try:
        # Sending a POST request with an empty payload to get all accounts
        response = requests.post(GET_ALL_ACCOUNTS_URL, data={})
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        if data and isinstance(data, list):
            #logging.info(f"returns data as {data} in fetch_all_accounts")
            for account in data:
                # Ensure the necessary keys are present
                if 'api_key' not in account or 'api_secret' not in account:
                    logging.error(f"Account missing api_key or api_secret: {account}")
                    continue
            return data
        else:
            logging.error("Failed to fetch accounts: Unexpected response format")
            return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch accounts: {e}")
        return []

def update_account(api_key, has_open_trades, has_open_orders):
    payload = {
        "api_key": api_key,
        "has_open_trades": has_open_trades,
        "has_open_orders": has_open_orders
    }
    
    try:
        response = requests.post(UPDATE_ACCOUNT_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        if data['status'] == 'success':
            #logging.info(f"Successfully updated account {api_key}")
            xxx=1
        else:
            logging.error(f"Failed to update account {api_key}: {data['message']}")
    except requests.exceptions.RequestException as e:
        logging.exception(f"Failed to update account {api_key}: {e}")

def updates_sync():
    while True:
        accounts = fetch_all_accounts()

        for account in accounts:
            api_key = account.get('api_key')
            api_secret = account.get('api_secret')

            if not api_key or not api_secret:
                logging.error(f"Skipping account due to missing api_key or api_secret: {account}")
                continue

            # FOR DEBUG ONLY MY TEST ACCOUNT
            # if api_key != "gExsrmBfeG8mHub03S":
            #     continue

            # Assuming you have the following methods already implemented:
            has_o_t = int(has_open_trades(account) == 'True')  # Convert "True"/"False" to 1/0
            has_o_o = int(has_open_orders(account) == 'True')  # Convert "True"/"False" to 1/0
            #logging.info(f"account {api_key} has open_trades: {has_o_t}")
            #logging.info(f"account {api_key} has open_orders: {has_o_o}")
            
            # Update the account in the database
            update_account(api_key, has_o_t, has_o_o)

        # Sleep for 2 seconds before the next iteration
        time.sleep(3)


def fetch_qty(account, symbols=["BTCUSDT"]):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=symbols)

    try:
        # Fetch open positions first to get the qty
        for symbol in symbols:
            open_positions = session.get_positions(category="linear", symbol=symbol)
            logging.info(f"open_positions is {open_positions}")

            for position in open_positions:
                if position['size'] != '0':
                    qty = float(position['size'])  # Extract and return the size as qty
                    return qty

        # If no open positions found, check open orders
        #for symbol in symbols:
        
        #    open_orders = session.get_open_orders(category="linear", symbol=symbol)
        #    logging.info(f"open_orders is {open_orders}")

        #    if open_orders['result']['list']:
        #        for order in open_orders['result']['list']:
        #            qty = float(order['qty'])  # Extract and return the size as qty
        #            return qty

        # If neither positions nor orders have any qty, return 0
        return 0.0

    except Exception as e:
        logging.error(f"Error fetching qty for account {account['api_key']}: {e}")
        return 0.0


def get_account_status(api_key):
    url = f"https://cryptopredictor.ai/get_account_status.php?api_key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        account_data = response.json()
        if account_data['status'] == 'success':
            #logging.info(f"account_data is: {account_data}")
            return account_data
        else:
            logging.error(f"Failed to retrieve account status: {account_data['message']}")
            return "NOREC"
    except requests.exceptions.RequestException as e:
        logging.error(f"Request to get account status failed: {e}")
        return None

def sync_account(api_key, api_secret, has_open_trades, has_open_orders, order_id=None, trade_id=None, timestamp=None, source="unknown", closeby_order=False, qty=0, side='None'):
    """
    Sync account information with the remote database.
    The PHP script will handle whether to insert or update based on the api_key.
    """
    url = "https://cryptopredictor.ai/bot_insert_account.php"
    
    # Ensure order_id and trade_id are strings, even if empty
    if order_id is None:
        order_id = ''
    if trade_id is None:
        trade_id = ''

    # Ensure timestamp is set to current time if not provided
    if timestamp is None:
        timestamp = get_utc_current_time()
    elif isinstance(timestamp, str):
        # If the timestamp is a string, convert it to a datetime object
        timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

    data = {
        'api_key': api_key,
        'api_secret': api_secret,
        'has_open_trades': has_open_trades,
        'has_open_orders': has_open_orders,
        'order_id': order_id,
        'trade_id': trade_id,
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'updated_at': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'source': source,
        'closeby_order': closeby_order,
        'qty': qty,
        'side': side
    }
    
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()  # Will raise an error for bad responses
        logging.info(f"Account sync successful: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to sync account: {e}")



def sync_account_prediction(api_key, prediction_price, prediction_time, source):
    url = "https://cryptopredictor.ai/bot_insert_prediction.php"
    
    timestamp = get_utc_current_time()
    data = {
        'api_key': api_key,
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'source': source,
        'Prediction': prediction_price,  # This is already a string
        'PredictionTime': prediction_time.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string format here
    }

    # Log the exact data being sent
    logging.info(f"Data being sent to {url}: {data}")

    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        logging.info(f"Account sync successful: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to sync account: {e}")




def validate_positive_int(value):
    """Ensure the provided value is a positive integer."""
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is an invalid positive int value")
    return ivalue

def validate_positive_float(value):
    """Ensure the provided value is a positive float."""
    fvalue = float(value)
    if fvalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is an invalid positive float value")
    return fvalue

def get_max_quantity(account, symbol="BTCUSDT", entry_limit=2000):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=[symbol])
    print(f"entry_limit is {entry_limit}")
    # Get the available USDT balance
    balance_info = session.get_wallet_balance(account_type="UNIFIED", coin="USDT")
    available_balance = float(balance_info[0]['coin'][0]['walletBalance'])
    
    # Assuming 99% of the available balance can be used
    available_balance = available_balance * 0.95
    
    # Fetch leverage for the symbol
    leverage_info = session.get_positions(category="linear", symbol=symbol)
    try:
        leverage = float(leverage_info[0]['leverage'])
    except Exception as e:
        leverage = m_leverage

    # Calculate the maximum position size considering leverage
    max_position_value = available_balance * leverage
    
    # Calculate the maximum quantity based on the entry limit
    max_qty = max_position_value / entry_limit
    
    # Fetch minimum quantity and precision settings from exchange info
    # Assuming you have access to this data; otherwise, use static values as before
    min_qty = 0.001  # Replace with actual minimum order quantity if available
    precision = 0.001  # Replace with actual precision (step size) if available

    # Round the quantity to the nearest allowed precision
    max_qty = math.floor(max_qty / precision) * precision
    
    # Ensure the quantity is at least the minimum allowed quantity
    if max_qty < min_qty:
        max_qty = min_qty

    # Round the final quantity to avoid floating-point errors
    max_qty = round(max_qty, 2)
    
    return max_qty


def has_open_orders(account, symbols=["BTCUSDT"]):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=symbols)
    try:
        for symbol in symbols:
            open_orders = session.get_open_orders(category="linear", symbol=symbol)
            if len(open_orders) != 0:
                return "True"
        return "False"
    except Exception as e:
        logging.error(f"Error checking open trades for account {account['api_key']}: {e}")
        return "False"

def get_open_order_ids(account, symbols=["BTCUSDT"]):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=symbols)
    try:
        order_ids = []

        for symbol in symbols:
            open_orders = session.get_open_orders(category="linear", symbol=symbol)
            for order in open_orders:
                order_ids.append(order['orderId'])
        

        if order_ids:
            account['order_id'] = order_ids[0]  # Store the first order ID as a string
        else:
            account['order_id'] = ""  # No open orders
        logging.info(f"open_orders is {open_orders}")
        return "True" if order_ids else "False"
    
    except Exception as e:
        logging.error(f"Error checking open orders for account {account['api_key']}: {e}")
        return "False"

def get_trade_ids(account, symbols=["BTCUSDT"]):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=symbols)
    try:
        trade_ids = []

        for symbol in symbols:
            open_positions = session.get_positions(category="linear", symbol=symbol)
            logging.info(f"open_positions for {symbol}: {open_positions}")
            
            for position in open_positions:
                if position['size'] != '0':
                    trade_ids.append(position['positionIdx'])
        if 'trade_id' not in account:
            account['trade_id']=[]
        
        account['trade_id'].extend(trade_ids)
        return "True" if trade_ids else "False"
        

    except Exception as e:
        logging.error(f"Error retrieving trade IDs for account {account['api_key']}: {e}")
        return "False"


def has_open_trades(account, symbols=["BTCUSDT"]):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=symbols)
    try:
        for symbol in symbols:
            open_positions = session.get_positions(category="linear", symbol=symbol)
            #print(f"open_positions returns {open_positions}")
            for position in open_positions:
                if position['size'] != '0':
                    return "True"
        
            return "False"
    except Exception as e:
        logging.error(f"Error checking open trades for account {account['api_key']}: {e}")
        return "False"

def clean_order_id(order_id):
    """Remove any unnecessary characters from the order_id string."""
    if isinstance(order_id, str):
        return order_id.replace('\"', '').replace('\\', '')
    return order_id

def place_close_by_order(account,prediction,side,qty):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=["BTCUSDT"])

    logging.info("entered place_close_by_order")
    logging.info(f"inside place_close_by_order we have close_by_price = {prediction}")
    logging.info(f"inside place_close_by_order we have side = {side} and qty = {qty}")
    try:
        close_order_response = session.place_order(
            category="linear",
            symbol="BTCUSDT",
            side="Sell" if side == "Buy" else "Buy",  # Opposite of initial side #m_side
            orderType="Limit",
            price=prediction,  # Target close price
            qty=str(round(float(qty), 3)),  # Same quantity as the initial trade
            timeInForce="PostOnly",  # replace "GTC" to "PostOnly"
            reduceOnly=False, # True to False
            closeOnTrigger=True
        )
        logging.info(f"Close By order placed at {prediction} or {qty} with response {close_order_response}")
        x=True
    except InvalidRequestError as e:
        x=False
        logging.error(f"Error placing close-by order: {e}")
    return x

def cancel_pending_order(account, order_id):
    session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=["BTCUSDT"])
    cleaned_order_id = clean_order_id(order_id)  # Ensure we clean the order ID

    try:
        cancel_response = session.cancel_order(
            category="linear",
            symbol="BTCUSDT",
            orderId=cleaned_order_id
        )
        logging.info(f"Cancel order response: {cancel_response}")
        #account['has_open_orders'] = "False"
        #account['has_open_trades'] = "False"
        return True
    except InvalidRequestError as e:
        logging.error(f"Error cancelling order: {e}")
        return False


def monitorTrades(account_manager):
    logging.info("Monitoring Accounts")
    for account in account_manager.accounts:
        api_key = account['api_key']
        logging.info(f"API Key: {api_key} - Has Open Trades: {has_open_trades(account)}")
        logging.info(f"API key: {api_key} - Has Open Orders: {has_open_orders(account)}")    

        logging.info(f"API key: {api_key} - Has OrderIDs: {account['order_id']}")

        logging.info(f"API key: {api_key} - Has TradeIDs: {account['trade_id']}")
        account['timestamp'] = get_utc_current_time()

def getEntryLimit(account, symbol="BTCUSDT"):
    ticker_data = ByBitWebSocketPublicStream.get_last_ticker(category="linear", symbol=symbol)
    last_price = float(ticker_data['lastPrice'])
    return last_price

def GetPredictions(step, account, currency="BTC", exchange="BYBIT", type="FUTURES"):
    url = "https://cryptopredictor.ai/bybit_api.php"
    if step == 60:
        step = '1H'
    if step == 1440:
        step == '24H'

    payload = {
        "currency": currency,
        "frequency": f"{step}M",
        "exchange": exchange,
        "type": type
    }
    logging.info(f"Payload for predictions: {payload}")

    for attempt in range(3):
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                prediction_data = response.json()
                prediction_time_str = prediction_data.get('Time')
                prediction_price = float(prediction_data.get('Price', "CallAgain"))
                now = get_utc_current_time()

                prediction_time = datetime.datetime.strptime(prediction_time_str, '%H:%M:%S').time()
                current_time = now.replace(second=0, microsecond=0).time()

                today = datetime.datetime.today()
                prediction_time = datetime.datetime.combine(today, prediction_time)
                current_time = datetime.datetime.combine(today, current_time)

                logging.info(f"Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logging.info(f"Prediction Time: {prediction_time.strftime('%Y-%m-%d %H:%M:%S')}")

                time_difference = (prediction_time - current_time).total_seconds() / 60
                logging.info(f"Time difference is {time_difference}")
                if abs(time_difference) < 15:
                    logging.info("Entering sync_account_prediction")
                    logging.info(f"prediction price is: {prediction_price}")
                    logging.info(f"prediction time is: {prediction_time}")
                    sync_account_prediction(
                        account['api_key'],
                        prediction_price,  # Keep as float
                        prediction_time,   # Pass as datetime object
                        "execute_trade_logic"
                    )
                    return prediction_price
                else:
                    logging.warning(f"Prediction time is not in {step} minutes interval, retrying... (Attempt {attempt + 1})")
                    time.sleep(2)
            else:
                logging.error(f"Request failed with status code: {response.status_code}")
                return "CallAgain"

        except Exception as e:
            logging.error(f"An error occurred while getting predictions: {e}")
            return "CallAgain"

    logging.error("Failed to get valid prediction after 3 attempts. Aborting.")
    return "CallAgain"


class AccountManager:
    def __init__(self, config_file):
        self.accounts = self.load_accounts(config_file)

    def load_accounts(self, config_file):
        if not os.path.exists(config_file):
            logging.error(f"Config file not found: {config_file}")
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            data = json.load(f)
        accounts = []
        logging.info(f"Loaded accounts data: {data}")


        for account in data['accounts']:
        # Initialize missing keys with default values
            account.setdefault('has_open_trades', 0)
            account.setdefault('has_open_orders', 0)
            account.setdefault('order_id', '')
            account.setdefault('trade_id', '')
            account.setdefault('timestamp', get_utc_current_time().strftime('%Y-%m-%d %H:%M:%S'))
            account.setdefault('updated_at', get_utc_current_time().strftime('%Y-%m-%d %H:%M:%S'))
            account.setdefault('closeby_order', 'False')
            account.setdefault('qty', '0')
            account.setdefault('side', 'None')

        for account in data['accounts']:

            r_account=get_account_status(account['api_key'])

            if r_account == 'NOREC' :
                logging.info("1.Entered NOREC")
                accounts.append({
                    'api_key': account['api_key'],
                    'api_secret': account['api_secret'],
                    'has_open_trades': has_open_trades(account),
                    'has_open_orders': has_open_orders(account),
                    'order_id' : '',
                    'trade_id': '',
                    'timestamp': get_utc_current_time(),
                    'updated_at': get_utc_current_time().strftime('%Y-%m-%d %H:%M:%S'),
                    'closeby_order':'False',
                    'qty':'0',
                    'side':'None'
                })
                get_open_order_ids(account)
                get_trade_ids(account)
                
                sync_account(
                    api_key=account['api_key'],
                    api_secret=account['api_secret'],
                    has_open_trades=has_open_trades(account),
                    has_open_orders=has_open_orders(account),
                    order_id=account['order_id'],
                    trade_id=account['trade_id'],
                    timestamp=get_utc_current_time(),
                    source="load_accounts",
                    closeby_order=account['closeby_order'],
                    qty=account['qty'],
                    side=account['side']
                )
                logging.info(f"2. IN NOREC account is {account}")
                
            elif r_account != 'NOREC' :
                logging.info(f"3. IN !NOT NOREC")
                
                account['has_open_trades'] = has_open_trades(account)
                account['has_open_orders'] = has_open_orders(account)
                account['order_id'] = r_account['order_id']
                account['trade_id'] = r_account['trade_id']
                account['timestamp'] = r_account['timestamp']
                account['updated_at'] = r_account['updated_at']
                account['closeby_order'] = r_account['closeby_order']
                account['qty'] = r_account['qty']
                account['side'] = r_account['side']
                
                accounts.append(account)  # APPEND the account to the list
                
                logging.info(f"4. IN !NOT NOREC accounts is {account}")



            
        logging.info(f"Accounts loaded: {accounts}")
        return accounts

    def find_next_available_account(self):
        logging.info("second entry")
        for account in self.accounts:
            logging.info(f"Checking account {account['api_key']} for open trades/orders")
            if has_open_trades(account) == "False" and has_open_orders(account) == "False":
                logging.info(f"Found available account: {account['api_key']}")
                return account
        logging.info("All accounts have open trades or are pending. Returning None.")
        return None

    def update_account_status(self, account, trade_opened):
        account_s=get_account_status(account['api_key'])
        logging.info(f"Updated account status for {account['api_key']} to has_open_trades={trade_opened}")

        account['has_open_trades']=account_s['has_open_trades']
        account['has_open_orders']=account_s['has_open_orders']        

        account['order_id'] = account_s['order_id']
        account['trade_id'] = account_s['trade_id']
        account['timestamp'] = account_s['timestamp']
        account['source'] = "update_account_status"
        account['closeby_order'] = account_s['closeby_order']

        

class TradeExecutor:
    def __init__(self, account_manager,leverage):
        self.leverage = str(leverage)
        self.account_manager = account_manager
        self.set_leverage_for_all_accounts()
        

    def set_leverage_for_all_accounts(self):
        logging.info("Setting leverage for all accounts.")
        for account in self.account_manager.accounts:
            if has_open_trades(account) == "False" and has_open_orders(account) == "False":
                session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=["BTCUSDT"])
                try:
                    leverage_response = session.set_leverage(
                        category="linear",
                        symbol="BTCUSDT", 
                        buy_leverage=self.leverage, 
                        sell_leverage=self.leverage
                    )
                    if leverage_response['retCode'] == 110043:
                        logging.info(f"Leverage already set for account {account['api_key']}.")
                    else:
                        logging.info(f"Leverage set response for account {account['api_key']}: {leverage_response}")
                except Exception as e:
                    logging.info(f"Error setting leverage for account {account['api_key']}: {e}")

    def execute_trade_logic(self, account, n_minutes, leverage, spread):
        res = GetPredictions(n_minutes, account, currency="BTC", exchange="BYBIT", type="FUTURES")
        
        if res != "CallAgain":
            logging.info(f"Prediction result received: {res}")
            res = float(res)
            account['close_by_price'] = str(round(res, 2))  # Store close_by_price in account
            entry_limit = getEntryLimit(account, symbol="BTCUSDT")
            account['side'] = "DEFAULT"
            
            spread = float(spread)
            logging.info(f"Entry limit is: {entry_limit}")
            calculated_spread = abs(res - entry_limit)
            market_irregular=get_diff(n_minutes,2)
            logging.info(f"market irregular is {market_irregular}")
            if calculated_spread >= spread and market_irregular <= 300 :

                
                logging.info(f"Entering trade with account {account['api_key']}")
                session : BybitWebSocket = BybitWebSocketWrapper.get_session(testnet=m_testnet, api_key=account['api_key'], api_secret=account['api_secret'], symbols=["BTCUSDT"])
                if(res - entry_limit > 0):
                    #account['side'] = "Buy"
                    side = "Buy"
                    logging.info(f"Going LONG")
                if(res - entry_limit < 0):
                    #account['side'] = "Sell"
                    side = "Sell"
                    
                    logging.info(f"Going SHORT")
                try:
                    valid_qty = get_max_quantity(account, symbol="BTCUSDT", entry_limit=entry_limit)
                    
                    order_id = session.place_order(
                        category="linear",
                        symbol="BTCUSDT",
                        side=side,  
                        orderType="Limit",  
                        price=str(entry_limit),
                        qty=str(round(valid_qty, 3)),  
                        timeInForce="PostOnly"      # GTC to PostOnly
                    )
                    print(f"Linear/BTCUSDT : {side}, {entry_limit}, {round(valid_qty, 3)}")
                    logging.info(f"Order placed at {entry_limit} with response {order_id}")                    
                    
                    time.sleep(2)
                    ret_account=get_account_status(account['api_key'])        

                    # Sync the account immediately after placing the order
                    sync_account(
                        api_key=account['api_key'],
                        api_secret=account['api_secret'],
                        has_open_trades=has_open_trades(account),
                        has_open_orders=has_open_orders(account),
                        order_id=order_id,
                        trade_id=ret_account['trade_id'],   ##maybe another method
                        timestamp=get_utc_current_time(),
                        source="execute_trade_logic",
                        closeby_order='False',
                        qty=str(round(valid_qty, 3)),
                        side=side
                        )
                    f_ret_account=get_account_status(account['api_key'])

                    account['order_id'] = f_ret_account['order_id']
                    account['has_open_orders'] = has_open_orders(account)
                    account['has_open_trades'] = has_open_trades(account)
                    account['timestamp'] = f_ret_account['timestamp']
                    account['closeby_order']=f_ret_account['closeby_order']
                    account['side'] = f_ret_account['side']
                    account['qty'] = f_ret_account['qty']




                    # Instead of waiting in a loop, let the monitor handle the logic
                    return True

                except InvalidRequestError as e:
                    if "ab not enough for new order" in str(e):
                        logging.error("Not enough balance to place the order.")
                    else:
                        logging.error(f"Error placing order: {e}")
                return True
            else:
                logging.info("No trade entered due to insufficient spread.")
                account['has_open_trades'] = 'False'
                account['closeby_order']='False'
                return False
        else:
            logging.info("Prediction call returned 'CallAgain'. No trade executed.")
            account['has_open_trades'] = 'False'
            return False

def calculate_time_difference(timestamp_db):
    """
    Calculate the difference in minutes between the current time and a timestamp from the database.

    Parameters:
    - timestamp_db (str): The timestamp string from the database in the format 'YYYY-MM-DD HH:MM:SS'.

    Returns:
    - float: The difference in minutes between the current time and the database timestamp.
    """

    # Convert the database timestamp string to a datetime object
    time_db = datetime.datetime.strptime(timestamp_db, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

    # Get the current time
    time_now = get_utc_current_time()

    # Calculate the difference in minutes
    time_difference = (time_now - time_db).total_seconds()

    return time_difference

def monitor_and_manage_trades(account_manager):
    while True:
        logging.info("MONITOR and MANAGE TRADES POINT")
        try:
            
            for account in account_manager.accounts:
                if 'updated_at' not in account:
                    logging.warning(f"'updated_at' missing for account {account['api_key']}. Initializing it.")
                    account['updated_at'] = get_utc_current_time().strftime('%Y-%m-%d %H:%M:%S')

                api_key = account['api_key']
                account_status = get_account_status(api_key)
                #logging.info(f"account_status is {account_status}")
                if True:
                    #logging.info("ENTERS AT WHERE True")
                    has_open_trades = account_status['has_open_trades']
                    has_open_orders = account_status['has_open_orders']
                    updated_at = datetime.datetime.strptime(account_status['timestamp'], '%Y-%m-%d %H:%M:%S')
                    
                    #logging.info(f"has_open_trades returns {has_open_trades} with api_key {api_key}")
                    
                    #logging.info(f"has_open_orders returns {has_open_orders} with api_key {api_key}")                    


                    x = calculate_time_difference(account_status['timestamp'])  
                    if has_open_trades == 1 and has_open_orders == 0 and account_status['closeby_order'] == "False" and x < 61:
                        qty=fetch_qty(account)
                        logging.info(f"INTRA and time diff is {x}")
                        if place_close_by_order(account,account_status['Prediction'],account_status['side'],qty) is True:
                            time.sleep(3)
                            logging.info("enters place_close_by_order")
                            #account['closeby_order'] = "True"
                            
                            sync_account(
                                account['api_key'],
                                account['api_secret'],
                                has_open_trades(account),
                                has_open_orders(account),
                                order_id=account['order_id'],
                                trade_id=account['trade_id'],
                                timestamp=get_utc_current_time(),
                                source="monitor_and_manage_trades",
                                closeby_order="True",
                                qty=qty,
                                side=account_status['side']
                            )
                            f3_ret_account = get_account_status(account['api_key'])
                            account['has_open_trades']=f3_ret_account['has_open_trades']
                            account['has_open_orders']=f3_ret_account['has_open_orders']
                            account['order_id'] = f3_ret_account['order_id']
                            account['trade_id'] = f3_ret_account['trade_id']
                            account['timestamp'] = f3_ret_account['timestamp']
                            account['closeby_order'] = f3_ret_account['closeby_order']
                            account['qty'] = f3_ret_account['qty']
                            account['side'] = f3_ret_account['side']                    

                    if has_open_trades == 0 and has_open_orders == 1 and account_status['closeby_order'] == "False" and x>61:
                        logging.info(f"INTRA222 and time_diff is {x}")
                        time_diff = (get_utc_current_time() - updated_at.replace(tzinfo=timezone.utc))
                        
                        time_diff_seconds = time_diff.total_seconds()
                        logging.info(f"cancel order .. updates_at is {updated_at} and time_diff={time_diff} and time_diff_seconds={time_diff_seconds}")
                        if x > 61:
                            
                            if cancel_pending_order(account,account_status['order_id']) is True:
                                logging.info("cancel_pending_order")
                                time.sleep(3)
                                sync_account(
                                    api_key=account['api_key'],
                                    api_secret=account['api_secret'],
                                    has_open_trades=has_open_trades(account),
                                    has_open_orders=has_open_orders(account),
                                    order_id='',
                                    trade_id='',
                                    timestamp=get_utc_current_time(),
                                    source="monitor_and_manage_trades",
                                    closeby_order='False',
                                    qty=0,
                                    side='None'
                                )
                                f2_ret_account=get_account_status(account['api_key'])
                                account['has_open_trades']=f2_ret_account['has_open_trades']
                                account['has_open_orders']=f2_ret_account['has_open_orders']
                                account['order_id'] = f2_ret_account['order_id']
                                account['trade_id'] = f2_ret_account['trade_id']
                                account['timestamp'] = f2_ret_account['timestamp']
                                account['closeby_order'] = f2_ret_account['closeby_order']
                                account['qty'] = f2_ret_account['qty']
                                account['side'] = f2_ret_account['side']


                                

                            

                            
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error in monitor_and_manage_trades: {e}")


def main(leverage, howMany, n_minutes, spread, config_path):
    # update kline interval for Bybit
    set_kline_interval(n_minutes)
    m_leverage = leverage

    account_manager = AccountManager(config_path)

    # Start monitor_and_manage_trades in a separate thread
    trade_executor = TradeExecutor(account_manager, leverage)
    
    monitoring_thread = threading.Thread(target=monitor_and_manage_trades, args=(account_manager,), daemon=True)
    monitoring_thread.start()

    updates_sync_thread = threading.Thread(target=updates_sync, daemon=True)
    updates_sync_thread.start()

    def process_trades():
        for _ in range(howMany):
            logging.info("TEMP INTRO")
            account = account_manager.find_next_available_account()
            if account is None:
                logging.info("All accounts have open trades or are pending, waiting until the next interval.")
                continue

            # Step 1: Execute trade logic
            trade_opened = trade_executor.execute_trade_logic(account, n_minutes, leverage, spread)
            if trade_opened:
                # Step 2: Monitor and manage the trade
                time.sleep(2)  # Adjust the sleep time as necessary to allow for order placement processing

            # Step 3: Update account status after monitoring and managing trades
            account_manager.update_account_status(account, trade_opened)

    while True:
        logging.info(f"Beginning of main loop with howMany={howMany}")
        now = get_utc_current_time()
        current_minute = now.minute
        current_second = now.second                            

        next_n_minute_mark = (current_minute // n_minutes + 1) * n_minutes
        time_to_wait = (next_n_minute_mark - current_minute) * 60 - current_second
        if time_to_wait == 0:
            time_to_wait = n_minutes * 60
        logging.info(f"Sleeping for {time_to_wait} seconds")
        time.sleep(time_to_wait)

        process_trades()

        # Final monitoring pass to ensure all trades are managed
        #monitorTrades(account_manager)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cryptopredictor.ai Trading bot parameters")
    parser.add_argument('--leverage', type=validate_positive_int, required=True, help='Leverage to be used.')
    parser.add_argument('--howMany', type=validate_positive_int, required=True, help='Number of accounts to check simultaneously.')
    parser.add_argument('--n_minutes', type=validate_positive_int, required=True, help='Time interval in minutes to check predictions.')
    parser.add_argument('--spread', type=validate_positive_float, required=True, help='Minimum spread required to enter a trade.')
    parser.add_argument('--config_path', type=str, required=True, help='Path to the config.json file.')

    args = parser.parse_args()
    main(args.leverage, args.howMany, args.n_minutes, args.spread, args.config_path)
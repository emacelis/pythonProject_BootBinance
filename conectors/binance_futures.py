import hmac
import logging
import time

import json
import requests
import hmac
import hashlib
import websocket
import threading
from urllib.parse import urlencode

logger = logging.getLogger()


class BinanceFutureClient:
    def __init__(self, public_key, secret_key, testnet):
        if testnet:
            self.base_url = "https://testnet.binancefuture.com/"
            self.wss_url = "wss://stream.binancefuture.com/ws"
        else:
            self.base_url = "https://dapi.binance.com/"
            self.wss_url = "wss://fstream.binance.com/ws"

        self.publicKey = public_key
        self.secretKey = secret_key

        self.headers = {'X-MBX-APIKEY' : self.publicKey}

        self.prices = dict()
        self.id = 1
        self.ws = None

        #t=threading.Thread(target=self.start_ws())
        #t.start()



        logger.info("Binance futures inicialized")

    def generate_signature(self,data):
        return hmac.new(self.secretKey.encode(),urlencode(data).encode(),hashlib.sha256).hexdigest()


    def make_request(self, method, endpoint, data):
        if method == "GET":
            response = requests.get(self.base_url+endpoint, params=data, headers= self.headers)
        elif method == "POST":
            response = requests.post(self.base_url+endpoint, params=data, headers= self.headers)
        elif method == "DELETE":
            response = requests.delete(self.base_url+endpoint, params=data, headers= self.headers)
        else:
            raise ValueError()

        if response.status_code==200:
            return response.json()
        else:
            logger.error("Error while making %s request to %s : %s (error code %s)",
                         method,endpoint,response.json(), response.status_code)
            return None


    def get_contracts(self):
        exchange_info = self.make_request("GET", "/dapi/v1/exchangeInfo", None)
        contracts = dict()
        if exchange_info is not None:
            for contract_data in exchange_info['symbols']:
                contracts[contract_data['pair']] = contract_data
        return contracts

    def get_historical_candles(self, symbol, interval):
        data = dict()
        data['symbol'] = symbol
        data['interval'] = interval
        data['limit'] = 1000

        row_candles = self.make_request("GET", "/dapi/v1/klines", data)
        candles = []

        if row_candles is not None:
            for c in row_candles:
                candles.append([c[0], float(c[1]),float(c[2]), float(c[3]), float(c[4]), float(c[5])])

        return candles


    def get_bid_asl(self,symbol):
        data = dict()
        data['symbol']=symbol
        ob_data = self.make_request("GET", "/dapi/v1/ticker/bookTicker", data)

        if ob_data is not None:
            if symbol not in self.prices:
                self.prices[symbol]={'bid': float(ob_data['bidPrice']), 'ask': float(ob_data['askPrice'])}
            else:
                self.prices[symbol]['bid']=float(ob_data['bidPrice'])
                self.prices[symbol]['ask']=float(ob_data['askPrice'])

        return self.prices[symbol]

    def get_balance(self):
        data = dict()
        data['timestamp'] = int(time.time()*1000)
        data['signature'] = self.generate_signature(data)

        balances = dict()

        account_data = self.make_request("GET","/dapi/v1/positionSide/dual" ,data)

        if account_data is not None:
             for a in account_data['dualSidePosition']:
                 balances[a['dualSidePosition']]=a

        return balances

    def place_older(self,symbol,side,quantity,order_type,price = None, tif=None):
        data = dict()
        data['symbol'] = symbol
        data['side'] = side
        data['quantity'] = quantity
        data['type'] = order_type

        if price is not None:
            data['price']=price

        if tif is not None:
            data['timeInForce'] = tif

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self.generate_signature(data)

        order_status= self.make_request("POST","/dapi/v1/order" , data)

        return order_status

    def cancel_order(self, symbol,order_id):
        data = dict()
        data['orderId'] = order_id
        data['symbol'] = symbol

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self.generate_signature(data)

        order_status = self.make_request("DELETE","/dapi/v1/order" ,data)

        return order_status

    def get_order_status(self,symbol,order_id):
        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['symbol'] = symbol
        data['orderId'] = order_id
        data['signature'] = self.generate_signature(data)

        order_status = self.make_request("GET","/dapi/v1/order" ,data)

        return order_status

    def start_ws(self):
        self.ws=websocket.WebSocketApp(self.wss_url,on_open=self.on_open(None), on_close=self.on_close(None), on_error=self.on_error(None,None),
                                  on_message=self.on_message(None))
        self.ws.run_forever()

    def on_open(self,ws):
        logger.info("Binance conection")
        #self.subscribe_channel("BTCUSDT")

    def on_close(self,ws):
        logger.info("Binance Websoket conection close")

    def on_error(self,ws,msg):
        logger.info("Binance Websoket conection error")

    def on_message(self,msg):
        print(msg)
        #data = json.loads(msg)

    def subscribe_channel(self,symbol):
        data = dict()
        data['method'] = "SUBSCRIBE"
        data['params'] = []
        data['params'].append(symbol.lower()+"@bookTiker")
        data['id'] = self.id

        print(data, type(data))
        print(json.dumps(data),type(json.dumps(data)))

        self.ws.send(json(data))
        self.ws.send
        self.id +=1

import ta
from binance.client import Client
from sqlitedict import SqliteDict

import pandas as pd
import san as san
import pyodbc

from IPython.display import HTML
import base64
import datetime
from datetime import datetime, date, time, timedelta
from ta import add_all_ta_features
import numpy as np
import time
from time import strftime
from ta.utils import dropna

# Client key Sandpy
san.ApiConfig.api_key = '5ef3hgjjnaeeseix_6oen65ggxjihvlwz'
# Client key BINANCE
client = Client("CUTCYCgUn74CBouyK0Afc719XEeMFkeuDPz63naE6EEEoB1zMapnmcO5aa7UgtNR",
                "BkWYp0i419ffR31wiOiiMFdOmVbV28cjbNbRXN4De4j44i5gdKsw2LTRAuYjDZay")


def getData(symbol, interval, lookback):

    symbol_solana = "SOLUSDT"
    symbol_bitcoin = "BTCUSDT"
    symbol_polkadot = "DOTUSDT"
    symbol_terra_luna = "LUNAUSDT"
    symbol_polygon_matic = "MATICUSDT"
    symbol_sand = "SANDUSDT"
    symbol_miota = "IOTAUSDT"
    symbol_avalanche = "AVAXUSDT"
    symbol_binancecoin = "BNBUSDT"
    symbol_USDT = "USDTTUSD"
    symbol_ONE_ECOMI = "ONEUSDT"
    symbol_CAKE_SOAP = "CAKEUSDT"
    symbol_xrp = "XRPUSDT"
    symbol_ravecoin = "RVNUSDT"
    symbol_mana = "MANAUSDT"
    symbol_cardano = "ADAUSDT"
    symbol_litecoin = "LTCUSDT"
    symbol_aave = "AAVEUSDT"
    symbol_dogecoin = "DOGEUSDT"
    symbol_ether = "ETHUSDT"
    symbol_shiba = "SHIBUSDT"


    row_candles = pd.DataFrame(client.get_historical_klines(symbol, interval, lookback + ' day ago UTC'))
    row_candles = row_candles.iloc[:, :6]
    row_candles.columns = ['Time_' + symbol, 'Open', 'High', 'Low', 'Close', 'Volume']
    row_candles.set_index('Time_' + symbol)
    row_candles.index = pd.to_datetime(row_candles.index, unit='ms')
    row_candles = row_candles.astype(float)

    row_candles['stoch_' + symbol] = ta.momentum.stoch(row_candles.High, row_candles.Low, row_candles.Close, window=14,
                                                       smooth_window=3)
    row_candles['D_' + symbol] = row_candles['stoch_' + symbol].rolling(3).mean()
    row_candles['rsi_' + symbol] = ta.momentum.rsi(row_candles.Close, window=14)
    # row_candles['stoch_'+symbol] = ta.momentum.stoch(row_candles.High, row_candles.Low, row_candles.Close)
    row_candles['macd_diff_' + symbol] = ta.trend.macd_diff(row_candles.Close) + 70
    row_candles['macd_' + symbol] = ta.trend.macd(row_candles.Close)

    # seconds passed since epoch
    rsi_compra = False
    #if row_candles['Time_' + symbol] is not None:
        #i=0
        #for seconds in row_candles['Time_' + symbol]:
            #epoch_time = seconds / 1000
            #time_formatted = datetime.fromtimestamp(epoch_time).strftime("%a, %d %b %Y %T %z")
            #row_candles['Time_' + symbol].array.T = time_formatted
            #row_candles['Time_' + symbol] = row_candles.insert(row_candles['Time_' + symbol].array.T.size.denominator,2,time_formatted)
            #i=i+1



    current_utc = datetime.utcnow().date()

    if symbol == symbol_mana:
        burn_rate = "burn_rate/decentraland"
        df_dev_activity = "dev_activity/decentraland"
        df_daily_active_deposits = "daily_active_deposits/decentraland"
        df_github_activity = "github_activity/decentraland"
        df_network_growth = "network_growth/decentraland"
        df_social_dominance = "social_dominance/decentraland"
        df_transaction_volume = "transaction_volume/decentraland"
    elif symbol == symbol_xrp:
        burn_rate = "burn_rate/ripple"
        df_dev_activity = "dev_activity/ripple"
        df_daily_active_deposits = "daily_active_deposits/ripple"
        df_github_activity = "github_activity/ripple"
        df_network_growth = "network_growth/ripple"
        df_social_dominance = "social_dominance/ripple"
        df_transaction_volume = "transaction_volume/ripple"
        # ------------------------
    elif symbol == symbol_solana:
        burn_rate = "burn_rate/solana"
        df_dev_activity = "dev_activity/solana"
        df_daily_active_deposits = "daily_active_deposits/solana"
        df_github_activity = "github_activity/solana"
        df_network_growth = "network_growth/solana"
        df_social_dominance = "social_dominance/solana"
        df_transaction_volume = "transaction_volume/solana"
    elif symbol == symbol_bitcoin:
        burn_rate = "burn_rate/bitcoin"
        df_dev_activity = "dev_activity/bitcoin"
        df_daily_active_deposits = "daily_active_deposits/bitcoin"
        df_github_activity = "github_activity/bitcoin"
        df_network_growth = "network_growth/bitcoin"
        df_social_dominance = "social_dominance/bitcoin"
        df_transaction_volume = "transaction_volume/bitcoin"
        # ------------------------
    elif symbol == symbol_shiba:
        burn_rate = "burn_rate/shiba"
        df_dev_activity = "dev_activity/shiba"
        df_daily_active_deposits = "daily_active_deposits/shiba"
        df_github_activity = "github_activity/shiba"
        df_network_growth = "network_growth/shiba"
        df_social_dominance = "social_dominance/shiba"
        df_transaction_volume = "transaction_volume/shiba"
        # ------------------------
    elif symbol == symbol_ravecoin:
        burn_rate = "burn_rate/ravencoin"
        df_dev_activity = "dev_activity/ravencoin"
        df_daily_active_deposits = "daily_active_deposits/ravencoin"
        df_github_activity = "github_activity/ravencoin"
        df_network_growth = "network_growth/ravencoin"
        df_social_dominance = "social_dominance/ravencoin"
        df_transaction_volume = "transaction_volume/ravencoin"
    elif symbol == symbol_polkadot:
        burn_rate = "burn_rate/polkadot"
        df_dev_activity = "dev_activity/polkadot"
        df_daily_active_deposits = "daily_active_deposits/polkadot"
        df_github_activity = "github_activity/polkadot"
        df_network_growth = "network_growth/polkadot"
        df_social_dominance = "social_dominance/polkadot"
        df_transaction_volume = "transaction_volume/polkadot"
    elif symbol == symbol_cardano:
        burn_rate = "burn_rate/cardano"
        df_dev_activity = "dev_activity/cardano"
        df_daily_active_deposits = "daily_active_deposits/cardano"
        df_github_activity = "github_activity/cardano"
        df_network_growth = "network_growth/cardano"
        df_social_dominance = "social_dominance/cardano"
        df_transaction_volume = "transaction_volume/cardano"
    elif symbol == symbol_dogecoin:
        burn_rate = "burn_rate/dogecoin"
        df_dev_activity = "dev_activity/dogecoin"
        df_daily_active_deposits = "daily_active_deposits/dogecoin"
        df_github_activity = "github_activity/dogecoin"
        df_network_growth = "network_growth/dogecoin"
        df_social_dominance = "social_dominance/dogecoin"
        df_transaction_volume = "transaction_volume/dogecoin"
    elif symbol == symbol_litecoin:
        burn_rate = "burn_rate/litecoin"
        df_dev_activity = "dev_activity/litecoin"
        df_daily_active_deposits = "daily_active_deposits/litecoin"
        df_github_activity = "github_activity/litecoin"
        df_network_growth = "network_growth/litecoin"
        df_social_dominance = "social_dominance/litecoin"
        df_transaction_volume = "transaction_volume/litecoin"
    elif symbol == symbol_aave:
        burn_rate = "burn_rate/aave"
        df_dev_activity = "dev_activity/aave"
        df_daily_active_deposits = "daily_active_deposits/aave"
        df_github_activity = "github_activity/aave"
        df_network_growth = "network_growth/aave"
        df_social_dominance = "social_dominance/aave"
        df_transaction_volume = "transaction_volume/aave"
    elif symbol == symbol_ether:
        burn_rate = "burn_rate/ethereum"
        df_dev_activity = "dev_activity/ethereum"
        df_daily_active_deposits = "daily_active_deposits/ethereum"
        df_github_activity = "github_activity/ethereum"
        df_network_growth = "network_growth/ethereum"
        df_social_dominance = "social_dominance/ethereum"
        df_transaction_volume = "transaction_volume/ethereum"
    elif symbol == symbol_terra_luna:
        burn_rate = "burn_rate/luna"
        df_dev_activity = "dev_activity/luna"
        df_daily_active_deposits = "daily_active_deposits/luna"
        df_github_activity = "github_activity/luna"
        df_network_growth = "network_growth/luna"
        df_social_dominance = "social_dominance/luna"
        df_transaction_volume = "transaction_volume/luna"
    elif symbol == symbol_polygon_matic:
        burn_rate = "burn_rate/polygon"
        df_dev_activity = "dev_activity/polygon"
        df_daily_active_deposits = "daily_active_deposits/polygon"
        df_github_activity = "github_activity/polygon"
        df_network_growth = "network_growth/polygon"
        df_social_dominance = "social_dominance/polygon"
        df_transaction_volume = "transaction_volume/polygon"
    elif symbol == symbol_sand:
        burn_rate = "burn_rate/the_sandbox"
        df_dev_activity = "dev_activity/the_sandbox"
        df_daily_active_deposits = "daily_active_deposits/the_sandbox"
        df_github_activity = "github_activity/the_sandbox"
        df_network_growth = "network_growth/the_sandbox"
        df_social_dominance = "social_dominance/the_sandbox"
        df_transaction_volume = "transaction_volume/the_sandbox"
    elif symbol == symbol_miota:
        burn_rate = "burn_rate/iota"
        df_dev_activity = "dev_activity/iota"
        df_daily_active_deposits = "daily_active_deposits/iota"
        df_github_activity = "github_activity/iota"
        df_network_growth = "network_growth/iota"
        df_social_dominance = "social_dominance/iota"
        df_transaction_volume = "transaction_volume/iota"
    elif symbol == symbol_avalanche:
        burn_rate = "burn_rate/avalanche"
        df_dev_activity = "dev_activity/avalanche"
        df_daily_active_deposits = "daily_active_deposits/avalanche"
        df_github_activity = "github_activity/avalanche"
        df_network_growth = "network_growth/avalanche"
        df_social_dominance = "social_dominance/avalanche"
        df_transaction_volume = "transaction_volume/avalanche"
    elif symbol == symbol_binancecoin:
        burn_rate = "burn_rate/binance_usd"
        df_dev_activity = "dev_activity/binance_usd"
        df_daily_active_deposits = "daily_active_deposits/binance_usd"
        df_github_activity = "github_activity/binance_usd"
        df_network_growth = "network_growth/binance_usd"
        df_social_dominance = "social_dominance/binance_usd"
        df_transaction_volume = "transaction_volume/binance_usd"
    elif symbol == symbol_USDT:
        burn_rate = "burn_rate/tusd"
        df_dev_activity = "dev_activity/tusd"
        df_daily_active_deposits = "daily_active_deposits/tusd"
        df_github_activity = "github_activity/tusd"
        df_network_growth = "network_growth/tusd"
        df_social_dominance = "social_dominance/tusd"
        df_transaction_volume = "transaction_volume/tusd"
    elif symbol == symbol_ONE_ECOMI:
        burn_rate = "burn_rate/harmony"
        df_dev_activity = "dev_activity/harmony"
        df_daily_active_deposits = "daily_active_deposits/harmony"
        df_github_activity = "github_activity/harmony"
        df_network_growth = "network_growth/harmony"
        df_social_dominance = "social_dominance/harmony"
        df_transaction_volume = "transaction_volume/harmony"
    elif symbol == symbol_CAKE_SOAP:
        burn_rate = "burn_rate/pancakeswap"
        df_dev_activity = "dev_activity/pancakeswap"
        df_daily_active_deposits = "daily_active_deposits/pancakeswap"
        df_github_activity = "github_activity/pancakeswap"
        df_network_growth = "network_growth/pancakeswap"
        df_social_dominance = "social_dominance/pancakeswap"
        df_transaction_volume = "transaction_volume/pancakeswap"

    # Peticion a API SANDBASE------------------------MONEDAS-----------------------
    df_burn_rate = san.get(
        burn_rate,
        to_date=str(current_utc),
        from_date="2021-08-25",
        interval="3d",
    )
    df_dev_activity = san.get(
        df_dev_activity,
        to_date=str(current_utc),
        from_date="2021-09-24",
        interval="3d",
    )
    df_daily_active_deposits = san.get(
        df_daily_active_deposits,
        to_date=str(current_utc),
        from_date="2021-08-25",
        interval="3d",
    )
    df_github_activity = san.get(
        df_github_activity,
        to_date=str(current_utc),
        from_date="2021-09-24",
        interval="3d",
    )
    df_network_growth = san.get(
        df_network_growth,
        to_date=str(current_utc),
        from_date="2021-08-25",
        interval="3d",
    )
    df_social_dominance = san.get(
        df_social_dominance,
        to_date=str(current_utc),
        from_date="2021-08-25",
        interval="3d",
    )
    df_transaction_volume = san.get(
        df_transaction_volume,
        to_date=str(current_utc),
        from_date="2021-08-25",
        interval="3d",
    )

    len_row_df_burn_rate_mana_mana = len(df_burn_rate)
    len_row_df_dev_activity_mana = len(df_dev_activity)
    len_row_df_daily_active_deposits_mana = len(df_daily_active_deposits)
    len_row_df_github_activity_mana = len(df_github_activity)
    len_row_df_df_network_growth_mana = len(df_network_growth)
    len_row_df_social_dominance_mana = len(df_social_dominance)
    len_row_df_transaction_volume_mana = len(df_transaction_volume)

    #A)
    # iota only have dev_activity,github_activity,df_social_dominance
    # avax_avalanche only have dev_activity,github_activity,df_social_dominance
    # one harmony only have dev_activity,github_activity,df_social_dominance
    # pancake Swap only have dev_activity,github_activity,df_social_dominance
    # solana only have dev_activity,github_activity,social_dominance
    #B)
    # polygon dosn't have aniting ----------ERROR
    # the_sandbox dosn't have aniting ----------ERROR
    # binance usd dosn't have aniting ----------ERROR
    # Ture USD  usd dosn't have aniting ----------ERROR
    # A)
    if symbol == symbol_avalanche or symbol== symbol_ONE_ECOMI or symbol== symbol_miota \
            or symbol== symbol_CAKE_SOAP  or symbol== symbol_solana :
        row_candles.insert(11, 'dev_activity_' + symbol, df_dev_activity['activity'].array.T)
        row_candles.insert(12, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
        row_candles.insert(13, 'social_dominance_' + symbol, df_social_dominance['dominance'].array.T)
    #B)
    elif symbol == symbol_USDT or symbol == symbol_polygon_matic or symbol == symbol_sand \
            or symbol == symbol_binancecoin or symbol == symbol_shiba:
        row_candles.dropna(inplace=True)
    elif symbol == symbol_polkadot:
        row_candles.insert(11, 'dev_activity_' + symbol, df_dev_activity['activity'].array.T)
        row_candles.insert(12, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
    elif symbol == symbol_terra_luna:
        row_candles.insert(11, 'dev_activity_' + symbol, df_dev_activity['activity'].array.T)
        row_candles.insert(12, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
        row_candles.insert(13, 'network_growth_' + symbol, df_network_growth['value'].array.T)
    elif symbol == symbol_bitcoin:
        row_candles.insert(11, 'burnRate_' + symbol, df_burn_rate['burnRate'].array.T)
        row_candles.insert(12, 'dev_activity_' + symbol, df_dev_activity['activity'].array.T)
        row_candles.insert(13, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
        row_candles.insert(14, 'social_dominance_' + symbol, df_social_dominance['dominance'].array.T)
        row_candles.insert(15, 'transaction_volume_' + symbol, df_transaction_volume['value'].array.T)
    elif symbol == symbol_xrp:
        row_candles.insert(11, 'burnRate_' + symbol, df_burn_rate['burnRate'].array.T)
        row_candles.insert(12, 'dev_activity_' + symbol, df_dev_activity['activity'].array.T)
        row_candles.insert(13, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
        row_candles.insert(14, 'social_dominance_' + symbol, df_social_dominance['dominance'].array.T)
        row_candles.insert(15, 'transaction_volume_' + symbol, df_transaction_volume['value'].array.T)
        row_candles.insert(16, 'network_growth_' + symbol, df_network_growth['value'].array.T)
    elif symbol == symbol_ravecoin:
        row_candles.insert(11, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
        row_candles.insert(12, 'social_dominance_' + symbol, df_social_dominance['dominance'].array.T)
        row_candles.insert(13, 'network_growth_' + symbol, df_network_growth['value'].array.T)
    elif symbol == symbol_mana or symbol == symbol_aave or symbol == symbol_ether:
        row_candles.insert(11, 'burnRate_' + symbol, df_burn_rate['burnRate'].array.T)
        row_candles.insert(12, 'dev_activity_' + symbol, df_dev_activity['activity'].array.T)
        row_candles.insert(13, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
        row_candles.insert(14, 'social_dominance_' + symbol, df_social_dominance['dominance'].array.T)
        row_candles.insert(15, 'transaction_volume_' + symbol, df_transaction_volume['value'].array.T)
        row_candles.insert(16, 'daily_active_deposits_' + symbol, df_daily_active_deposits['activeDeposits'].array.T)
        row_candles.insert(17, 'network_growth_' + symbol, df_network_growth['value'].array.T)
    elif symbol == symbol_cardano or symbol == symbol_litecoin or symbol == symbol_dogecoin:
        row_candles.insert(11, 'burnRate_' + symbol, df_burn_rate['burnRate'].array.T)
        row_candles.insert(12, 'dev_activity_' + symbol, df_dev_activity['activity'].array.T)
        row_candles.insert(13, 'github_activity_' + symbol, df_github_activity['activity'].array.T)
        row_candles.insert(14, 'social_dominance_' + symbol, df_social_dominance['dominance'].array.T)
        row_candles.insert(15, 'transaction_volume_' + symbol, df_transaction_volume['value'].array.T)


    row_candles.dropna(inplace=True)
    # sqldb(row_candles)

    return row_candles



def rsi_indicator(row_candles):
    rsi_compra = False
    if row_candles is not None:
        for c in row_candles.rsi:
            # Se añadio +1 a c porque el parametro obtenido se le restaban 3 puntos en la grafica de binance.
            if c + 1 <= 30:
                rsi_compra = True
            elif c + 1 >= 70:
                rsi_compra = False
            elif c + 1 <= 70 or c >= 30:
                rsi_compra = False

    return rsi_compra


def san_indicator():
    aviable_metrica = san.available_metrics()
    return aviable_metrica


def create_download_link(title="Download CSV file", filename="data.csv", df=None):
    csv = df.to_csv()
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload, title=title, filename=filename)
    return HTML(html)


def sqldb(row_candles):
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=RON\SQLEXPRESS;'
                          'Database=test_database;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()

    cursor.execute('''
    		CREATE TABLE row_candles (
    			%K int,
    			%D int,
    			rsi int,
    			stoch int,
    			macd_diff int,
    			macd int,
    			)
                   ''')

    conn.commit()

    cursor.execute('INSERT INTO row_candles (K,D,rsi,stoch,macd_diff,macd)'
                   ' VALUES(' + row_candles.K + ',' + row_candles.D + ',' + row_candles.rsi + '' + row_candles.stoch + ',' + row_candles.macd_diff + ',' + row_candles.macd + '')

    conn.commit()


def stoch_indicator(row_candles):
    stoch_compra = False
    if row_candles is not None:
        for c in row_candles.stoch:
            if c <= 20:
                stoch_compra = True
            elif c >= 80:
                stoch_compra = False
            elif c <= 80 or c >= 20:
                stoch_compra = False

    return stoch_compra


def save(key, row_candles, cache_file="cache.sqlite3"):
    try:
        with SqliteDict() as mydict:
            mydict['K'] = row_candles.K  # Using dict[key] to store
            mydict['D'] = row_candles.D  # Using dict[key] to store
            mydict['stoch'] = row_candles.stoch  # Using dict[key] to store
            mydict['macd_diff'] = row_candles.macd_diff  # Using dict[key] to store
            mydict['macd'] = row_candles.macd  # Using dict[key] to store
            mydict.commit()  # Need to commit() to actually flush the data
    except Exception as ex:
        print("Error during storing data (Possibly unsupported):", ex)


def load(key, cache_file="cache.sqlite3"):
    try:
        with SqliteDict(cache_file) as mydict:
            value = mydict[key]  # No need to use commit(), since we are only loading data!
        return value
    except Exception as ex:
        print("Error during loading data:", ex)


# SQLFUNTION------------------------------------------------------------------///
import sqlite3;
import pandas as pd;

con = None


def getConnection():
    databaseFile = "./test.db"
    global con
    if con == None:
        con = sqlite3.connect(databaseFile)
    return con


def createTable(con):
    try:
        c = con.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS row_candles
                 (K, D, stoch, macd_diff, macd)""")
    except Exception as e:
        pass


def insert(con, row_candles):
    c = con.cursor()
    c.execute("""INSERT INTO row_candles (K, D, stoch)
          values(""" + row_candles.K + """,""" + row_candles.D + """, """ + row_candles.stoch + """)""")

    # c.execute('INSERT INTO row_candles (K,D,rsi,stoch,macd_diff,macd)'
    #             ' VALUES(' + row_candles.K + ',' + row_candles.D + ',' + row_candles.rsi + '' + row_candles.stoch + ',' + row_candles.macd_diff + ',' + row_candles.macd + '')


def queryExec(row_candles):
    con = getConnection()
    createTable(con)
    insert(con, row_candles)
    # r = con.execute("""SELECT * FROM Movie""")
    result = pd.read_sql_query("select * from row_candles;", con)
    return result

# r = queryExec(row_candles)
# print(r)

import logging

import  tkinter as tk

#from conectors.bitmex import get_contracts
import youtubeRSI.tables_data

from ta import add_all_ta_features
from ta.utils import dropna
import san as san
from conectors.binance_futures import BinanceFutureClient

san.ApiConfig.api_key = '5ef3hgjjnaeeseix_6oen65ggxjihvlwz'


logger = logging.getLogger()

logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s :: %(message)s')
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('info.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)


logger.info("This is logged in all cases")

if __name__ == '__main__':
    logger.info("This is looged only if we execute the main.py file")

    binance =BinanceFutureClient("34da009981933365e04450a289832d85816904fc92779fbc13b3d63b40439b87","04c56306d98a1906bd3dd9d5a293c9e6ef8222da1b85447e8ab84c76c60ef1ea",True)

    #table_mana = youtubeRSI.tables_data.getData('MANAUSDT','3d','150')
    table_xrp = youtubeRSI.tables_data.getData('SANDUSDT','3d','150')

    print(table_xrp)
    #print(table_mana)

    #root = tk.Tk()
    #root.mainloop()

    #savedataRSI = youtubeRSI.youtubetest.save("prueba01",minanceyoutube)
    #savedataRSI = youtubeRSI.youtubetest.load("prueba01")
    #print(minanceyoutube)
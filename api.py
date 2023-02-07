
import requests
import datetime
import time

import pandas as pd

from tools import DateManager

# def timestamp_to_datetime(timestamp:int) -> str:
#     return datetime.datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d") # %H:%M:%S

# def datetime_to_timestamp(datetime:str) -> int:
#     return int(mktime(pd.to_datetime(datetime).timetuple()))

class CryptoCompare:

    def __init__(self, api_key):
        self.__api_key = api_key
        self.__params = {"api_key":self.__api_key}

        self.__urls = {}
        self.__urls["rate_limit"] = "https://min-api.cryptocompare.com/stats/rate/limit"
        self.__urls["coin_list"] = "https://min-api.cryptocompare.com/data/blockchain/list"
        self.__urls["hist_ohlcv"] = "https://min-api.cryptocompare.com/data/v2/histoday?"
        self.__urls["hist_blockchain"] = "https://min-api.cryptocompare.com/data/blockchain/histo/day?"
        self.__urls["social_data"] = "https://min-api.cryptocompare.com/data/social/coin/histo/day?"

        try:
            self.__coin_list = pd.read_csv("symbol_id.csv", index_col=0)
        except:
            tmp = self.get_coin_list()
            tmp.to_csv("symbol_id.csv")
            self.__coin_list = tmp
        
        self.sleep_time = 0.05

    @property
    def api_key(self):
        return self.__api_key

    @property
    def params(self):
        return self.__params

    @property
    def urls(self):
        return self.__urls

    @property
    def coin_list(self):
        return self.__coin_list

    def __clear_params(self):
        self.__params = {"api_key":self.__api_key}
    
    def __get_something_simple(self, something_url):
        self.__params = {"api_key":self.__api_key}
        time.sleep(self.sleep_time)
        res = requests.get(url=something_url, params=self.__params).json()
        return pd.DataFrame(res["Data"])

    def __get_something_daily_symbol(self, something_url, symbol, start, end):
        self.__clear_params()
        self.__params["fsym"] = symbol
        self.__params["tsym"] = "USD"
        self.__params["limit"] = "2000"
        self.__params["toTs"] = DateManager.str_to_timestamp(end)
        start = pd.to_datetime(start)

        result_df = None
        while True:
            time.sleep(self.sleep_time)
            res = requests.get(url=something_url, params=self.__params).json()
            if res["Response"] != "Success":
                raise Exception("Request Failed!")
            
            tmp_df = pd.DataFrame(res["Data"]["Data"])
            tmp_df.index = pd.to_datetime(DateManager.timestamp_to_datetime(tmp_df["time"]))

            if type(result_df) == type(None):
                result_df = tmp_df.copy()
            else:
                result_df = pd.concat([tmp_df, result_df], axis=0)
            
            if result_df.index[0] <= start:
                break
            
            self.__params["toTs"] = result_df["time"].iloc[0]

        return result_df.loc[start.strftime("%Y-%m-%d"):end]

    def __get_something_daily_coinid(self, something_url, symbol, start, end):
        self.__clear_params()
        self.__params["coinId"] = symbol
        self.__params["limit"] = "2000"
        self.__params["toTs"] = DateManager.str_to_timestamp(end)
        start = pd.to_datetime(start)

        result_df = None
        while True:
            time.sleep(self.sleep_time)
            res = requests.get(url=something_url, params=self.__params).json()
            if res["Response"] != "Success":
                raise Exception("Request Failed!")
            
            tmp_df = pd.DataFrame(res["Data"]["Data"])
            tmp_df.index = pd.to_datetime(DateManager.timestamp_to_datetime(tmp_df["time"]))

            if type(result_df) == type(None):
                result_df = tmp_df.copy()
            else:
                result_df = pd.concat([tmp_df, result_df], axis=0)
            
            if result_df.index[0] <= start:
                break
            
            self.__params["toTs"] = result_df["time"].iloc[0]

        return result_df.loc[start.strftime("%Y-%m-%d"):end]

    def symbol_to_id(self, symbol):
        return self.__coin_list[self.__coin_list["symbol"]==symbol]["id"][0]

    def get_rate_limit(self):
        return self.__get_something_simple(something_url=self.__urls["rate_limit"])

    def get_coin_list(self):
        result_df = self.__get_something_simple(something_url=self.__urls["coin_list"]).T
        result_df["data_available_date"] = pd.to_datetime(DateManager.timestamp_to_datetime(result_df["data_available_from"]))
        return result_df

    def get_daily_ohlcv(self, symbol, start, end):
        return self.__get_something_daily_symbol(something_url=self.__urls["hist_ohlcv"], symbol=symbol, start=start, end=end)

    def get_daily_blockchain(self, symbol, start, end):
        return self.__get_something_daily_symbol(something_url=self.__urls["hist_blockchain"], symbol=symbol, start=start, end=end)
    
    def get_daily_social(self, symbol, start, end):
        self.__params["coinID"] = self.symbol_to_id(symbol)
        self.__params["limit"] = "2000"
        self.__params["toTs"] = DateManager.str_to_timestamp(end)
        start = pd.to_datetime(start)

        result_df = None
        while True:
            time.sleep(self.sleep_time)
            res = requests.get(url=self.__urls["social_data"], params=self.__params).json()
            if res["Response"] != "Success":
                raise Exception("Request Failed!")
            
            tmp_df = pd.DataFrame(res["Data"])
            tmp_df.index = pd.to_datetime(DateManager.timestamp_to_datetime(tmp_df["time"]))

            if type(result_df) == type(None):
                result_df = tmp_df.copy()
            else:
                result_df = pd.concat([tmp_df, result_df], axis=0)
            
            if result_df.index[0] <= start:
                break
            
            self.__params["toTs"] = result_df["time"].iloc[0]

        return result_df.loc[start.strftime("%Y-%m-%d"):end]
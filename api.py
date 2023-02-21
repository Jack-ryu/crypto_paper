
import argparse
import io
import requests
import time

import numpy as np
import pandas as pd
from tqdm import tqdm
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from oauth2client import file, client, tools

from tools import Singleton, DateManager


class CryptoCompare(metaclass=Singleton):

    def __init__(self, api_key):
        self.__api_key = api_key
        self.__params = {"api_key":self.__api_key}

        self.__urls = {}
        self.__urls["rate_limit"] = "https://min-api.cryptocompare.com/stats/rate/limit"
        self.__urls["blockchain_coin_list"] = "https://min-api.cryptocompare.com/data/blockchain/list"
        self.__urls["hist_ohlcv"] = "https://min-api.cryptocompare.com/data/v2/histoday?"
        self.__urls["hist_blockchain"] = "https://min-api.cryptocompare.com/data/blockchain/histo/day?"
        self.__urls["social_data"] = "https://min-api.cryptocompare.com/data/social/coin/histo/day?"
        self.__urls["all_coin_list"] = "https://min-api.cryptocompare.com/data/all/coinlist"

        self.sleep_time = 0.05
        
        self.all_coin_list = self.get_all_coin_list()

    @property
    def api_key(self):
        return self.__api_key

    @property
    def params(self):
        return self.__params

    @property
    def urls(self):
        return self.__urls

    def __clear_params(self):
        self.__params = {"api_key":self.__api_key}
    
    def __get_something_simple(self, something_url):
        self.__params = {"api_key":self.__api_key}
        time.sleep(self.sleep_time)
        res = requests.get(url=something_url, params=self.__params).json()
        return pd.DataFrame(res["Data"])

    def __get_something_daily_symbol(self, something_url, symbol, start, end):
        start = pd.to_datetime(start)

        self.__clear_params()
        self.__params["fsym"] = symbol
        self.__params["tsym"] = "USD"
        self.__params["limit"] = "2000"
        self.__params["toTs"] = DateManager.str_to_timestamp(end)

        result_df = None
        before_df = None
        while True:
            time.sleep(self.sleep_time)
            res = requests.get(url=something_url, params=self.__params).json()
            if res["Response"] != "Success":
                raise Exception("Request Failed!")
            
            tmp_df = pd.DataFrame(res["Data"]["Data"])
            tmp_df.index = pd.to_datetime(DateManager.timestamp_to_datetime(tmp_df["time"]))

            if type(result_df) == type(None):
                result_df = tmp_df.copy()
                before_df = tmp_df.copy()
            else:
                if np.array_equal(before_df.values, tmp_df.values):
                    break
                else:
                    result_df = pd.concat([tmp_df, result_df], axis=0)
                    before_df = tmp_df.copy()
                
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
        return self.all_coin_list[self.all_coin_list["Symbol"]==symbol]["Id"][0]

    def get_rate_limit(self):
        return self.__get_something_simple(something_url=self.__urls["rate_limit"])

    def get_blockchain_coin_list(self):
        result_df = self.__get_something_simple(something_url=self.__urls["blockchain_coin_list"]).T
        result_df["data_available_date"] = pd.to_datetime(DateManager.timestamp_to_datetime(result_df["data_available_from"]))
        return result_df
    
    def get_all_coin_list(self):
        result_df = self.__get_something_simple(something_url=self.__urls["all_coin_list"]).T
        return result_df

    def get_daily_ohlcv(self, symbol, start, end):
        return self.__get_something_daily_symbol(something_url=self.__urls["hist_ohlcv"], symbol=symbol, start=start, end=end)

    def get_daily_blockchain(self, symbol, start, end):
        return self.__get_something_daily_symbol(something_url=self.__urls["hist_blockchain"], symbol=symbol, start=start, end=end)
    
    def get_daily_social(self, symbol, start, end, isId=False):
        if isId:
            self.__params["coinID"] = symbol
        else:
            self.__params["coinID"] = self.symbol_to_id(symbol)
        self.__params["limit"] = "2000"
        self.__params["toTs"] = DateManager.str_to_timestamp(end)
        start = pd.to_datetime(start)

        result_df = None
        before_df = None
        while True:
            time.sleep(self.sleep_time)
            res = requests.get(url=self.__urls["social_data"], params=self.__params).json()
            if res["Response"] != "Success":
                raise Exception("Request Failed!")
            
            tmp_df = pd.DataFrame(res["Data"])
            tmp_df.index = pd.to_datetime(DateManager.timestamp_to_datetime(tmp_df["time"]))

            if type(result_df) == type(None):
                result_df = tmp_df.copy()
                before_df = tmp_df.copy()
            else:
                if np.array_equal(before_df.values, tmp_df.values):
                    break
                else:
                    result_df = pd.concat([tmp_df, result_df], axis=0)
                    before_df = tmp_df.copy()
            
            if result_df.index[0] <= start:
                break
            
            self.__params["toTs"] = result_df["time"].iloc[0]

        return result_df.loc[start.strftime("%Y-%m-%d"):end]


class GoogleDrive(metaclass=Singleton):

    def __init__(self, json_loc):
        """Google Drive API.

        Args:
            json_loc (str): local location of json files e.g.) "C:\\Users\\Users\\Desktop\\Data\\"
        
        """
        self.json_loc = json_loc
    
        # api 연결 및 사전정보 입력
        SCOPES = [
            'https://www.googleapis.com/auth/drive.metadata', 
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
        ]
        store = file.Storage(json_loc+"storage.json")
        creds = store.get()
            
        # 권한 인증 창. 제일 처음만 창이 띄워짐
        try :
            try:
                flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
            except:
                flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args(args=[])
        except ImportError:
            flags = None
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(json_loc+"client_secret_drive.json", SCOPES)
            creds = tools.run_flow(flow, store, flags) if flags else tools.run_flow(flow, store)
            
        self.service = build('drive','v3', credentials=creds)

    def create_folder(self, folder_name, parent="CryptoPaperResearch"):
        """Create a New Folder.

        Args:
            folder_name (str): name of new drive folder
            parents (str): name of parent drive folder

        Note:
            new folder is created under parent folder

        """
        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder"
        }
        if parent is None:
            pass
        else:
            folder_metadata["parents"] = [self.list_folders()[parent]]

        self.service.files().create(body=folder_metadata, fields='id').execute()
        print(f"Folder {folder_name} is created under {parent}")

    def list_folders(self, parent=None):
        """List Folders.

        Args:
            parent (str): name of parent drive folder, default None
        
        Return:
            dictionary of folders {name:id}
        
        """
        query = "mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent is not None:
            query = f"'{self.list_folders()[parent]}' in parents" + query
        results = self.service.files().list(q=query, pageSize=1000, fields="nextPageToken, files(name, id)").execute()
        items = results.get("files", [])
        dict_items = {}
        for item in items:
            dict_items[item["name"]] = item["id"]
        return dict_items

    def list_files(self, folder_name):
        """List Files.

        Args:
            folder_name (str): name of parent drive folder
        
        Return:
            dictionary of files {name:id}
        
        """
        query = f"'{self.list_folders()[folder_name]}' in parents and trashed=false"
        results = self.service.files().list(q=query, pageSize=1000, fields="nextPageToken, files(name, id)").execute()
        items = results.get('files', [])
        dict_items = {}
        for item in items:
            dict_items[item["name"]] = item["id"]
        return dict_items

    def upload(self, folder_name, file_loc, file_name):
        """Upload File.

        Args:
            folder_name (str): name of drive folder
            file_loc (str): lcoal location of file e.g.) "C:\\Users\\Users\\Desktop\\Data\\"
            file_name (str): name of file e.g.) "test.csv"
        
        """
        # parents: 업로드할 구글 드라이브 위치의 url 마지막 ID
        file_metadata = {
            "name": file_name,
            "parents": [self.list_folders()[folder_name]]
        }
        # 파일 업로드
        media = MediaFileUpload(file_loc+file_name, resumable=True)
        self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"{file_name} is uploaded on {folder_name}")

    def download(self, folder_name, file_loc, file_name):
        """Download File.

        Args:
            folder_name (str): name of drive folder
            file_loc (str): local location of file e.g.) "C:\\Users\\Users\\Desktop\\Data\\"
            file_name (str): name of file e.g.) "test.csv"
        
        """
        fh = io.FileIO(file_loc+file_name, "wb")
        request = self.service.files().get_media(fileId=self.list_files(folder_name)[file_name])

        MediaIoBaseDownload(fh, request).next_chunk()
        print(f"{file_name} is downloaded from {folder_name}")

    def multi_upload(self, folder_name, file_loc, file_name_list):
        """Upload Multiple Files.

        Args:
            folder_name (str): name of drive folder
            file_loc (str): lcoal location of file e.g.) "C:\\Users\\Users\\Desktop\\Data\\"
            file_name_list (list[str]): list with name of files e.g.) ["test1.csv","test2.csv"]
        
        """
        parent_folder_id = self.list_folders()[folder_name]
        for file_name in tqdm(file_name_list, desc="Uploading Files..."):
            file_metadata = {
                "name": file_name,
                "parents": [parent_folder_id]
            }
            media = MediaFileUpload(file_loc+file_name, resumable=True)
            self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    def multi_download(self, folder_name, file_loc, file_name_list):
        """Download Multiple Files.

        Args:
            folder_name (str): name of drive folder
            file_loc (str): local location of file e.g.) "C:\\Users\\Users\\Desktop\\Data\\"
            file_name_list (list[str]): list with name of files e.g.) ["test1.csv","test2.csv"]
        
        """
        file_id_dict = self.list_files(folder_name)
        for file_name in tqdm(file_name_list, desc="Downloading Files..."):
            fh = io.FileIO(file_loc+file_name, "wb")
            request = self.service.files().get_media(fileId=file_id_dict[file_name])
            MediaIoBaseDownload(fh, request).next_chunk()
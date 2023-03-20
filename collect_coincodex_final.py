
import subprocess
import time
from datetime import datetime

import requests
import pandas as pd
from tqdm import tqdm

import pymongo

client = pymongo.MongoClient("mongodb://erion:Aje3v3$v312840@152.70.90.103:27017/admin")
db = client["FDB_CRYPTO"]
collection = db["HIST_SNAPSHOT_FINAL"]

date_list = pd.date_range(start="2010-07-17", end="2023-03-10", freq="D").strftime("%Y-%m-%d").tolist()

header = {
    'authority': 'coincodex.com',
    'method': 'GET',
    'scheme': 'https',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'ko-KR,ko;q=0.9',
    'cache-control': 'max-age=0',
    'cookie': '',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
}

ssids = ["SKKU","SKKU_GUEST"] # 여기에 사용하는 Wifi 이름 넣기

for date in tqdm(date_list):
    cnt = 0
    header["path"] = f'/api/coincodex/get_historical_snapshot/{date}%2000:00/0/15000'
    url = f"https://coincodex.com/api/coincodex/get_historical_snapshot/{date}%2000:00/0/15000"

    documents = collection.find({"timestamp":datetime.strptime(date, "%Y-%m-%d")})
    docs = [doc for doc in documents]
    if len(docs) != 0:
        continue

    while True:
        cnt += 1
        try:
            time.sleep(5)
            res = requests.get(headers=header, url=url).json()["coins"]
            break
        except:
            print("except occured")
            if cnt % 2 == 0:
                subprocess.run(['netsh', 'wlan', 'connect', ssids[0]])
            else:
                subprocess.run(['netsh', 'wlan', 'connect', ssids[1]])
            time.sleep(5)

    data = {"timestamp": datetime.strptime(date, "%Y-%m-%d")}
    for coin in res:
        data[coin["symbol"]] = coin

    while True:
        try:
            collection.insert_one(data)
            break
        except:
            print("MongoDB insert error... Retry...")
            time.sleep(5)

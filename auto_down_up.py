import os
import time
import json

import pandas as pd
from tqdm import tqdm

from api import CoinGecko, GoogleDrive

google_api = GoogleDrive("C:\\Users\\Byeong-Guk Kang\\Desktop\\PaperWriting\\code\\json_setting\\")
gecko_api = CoinGecko()

coin_list = gecko_api.get_coin_list()

tmp = google_api.list_files("CoinGecko2")
tmp = sorted(tmp)
already_exist = sorted(google_api.list_files("CoinGecko2"))

log = {}
for coin_dict in tqdm(coin_list):
    coin_id = coin_dict["id"]
    coin_name = coin_dict["name"]

    coin_name = coin_name.replace("/","").replace("*","")

    if f"{coin_name}.csv" in already_exist:
        continue

    try:
        tmp = gecko_api.get_coin_price_cap(
            coin_id
        )
    except:
        try:
            time.sleep(30)
            tmp = gecko_api.get_coin_price_cap(
                coin_id
            )
        except:
            log[coin_name] = f"{coin_name} download failed"
    tmp.to_csv(f"C:\\Users\\Byeong-Guk Kang\\Desktop\\PaperWriting\\code\\data\\gecko_none\\{coin_name}.csv")

    try:
        google_api.upload(
            "CoinGecko2",
            "C:\\Users\\Byeong-Guk Kang\\Desktop\\PaperWriting\\code\\data\\gecko_none\\",
            f"{coin_name}.csv"
        )
        log[coin_name] = "success"
    except:
        try:
            time.sleep(30)
            google_api.upload(
                "CoinGecko2",
                "C:\\Users\\Byeong-Guk Kang\\Desktop\\PaperWriting\\code\\data\\gecko_none\\",
                f"{coin_name}.csv"
            )
            log[coin_name] = "success"
        except:
            log[coin_name] = f"{coin_name} drive upload failed"

with open('log.json','w') as f:
  json.dump(log, f, ensure_ascii=False, indent=4)
f.close()

print(len(log))

exit()
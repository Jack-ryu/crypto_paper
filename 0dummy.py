import os

import pandas as pd
from tqdm import tqdm

# raw_data를 불러와서 메모리에 올립니다
lst = os.listdir("coingecko")
coin_price_dict = {}
coin_marketcap_dict = {}
coin_volume_dict = {}
error_list = []

for file in tqdm(lst):
    try:
        df = pd.read_csv("coingecko\\{}".format(file), index_col=0)
        coin_price_dict[file] = df[["prices"]]
        coin_marketcap_dict[file] = df[["market_caps"]]
        coin_volume_dict[file] = df[["total_volumes"]]

    except:
        error_list.append(file)
        print("{} Open Error!!".format(file))
        continue
print("Done... Error file is saved in error_list")

# 데이터를 하나의 DataFrame으로 합쳐줍니다

# find the longest timeframe index
longest_index_price = max([df.index for df in coin_price_dict.values()], key=len)
longest_index_cap = max([df.index for df in coin_marketcap_dict.values()], key=len)
longest_index_vol = max([df.index for df in coin_volume_dict.values()], key=len)

# create a close dataframe with the longest timeframe index and columns "key1", "key2", "key3"
price_df = pd.DataFrame(index=longest_index_price, columns=coin_price_dict.keys())
market_cap_df = pd.DataFrame(index=longest_index_cap, columns=coin_marketcap_dict.keys())
volume_df = pd.DataFrame(index=longest_index_vol, columns=coin_volume_dict.keys())

for key, df in tqdm(coin_volume_dict.items()):
    df = df.astype("float64")
    volume_df[key] = df.groupby(df.index).last().reindex(longest_index_vol)

price_df.to_csv("gecko_volume.csv")
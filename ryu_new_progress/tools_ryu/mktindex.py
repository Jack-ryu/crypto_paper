import numpy as np 
import pandas as pd 

class CrossSectional:
    
    def __init__(self, data:pd.DataFrame, vender:str):
        # Initialize the Data
        '''
        data : pivot 필요한 데이터,
        vender : [coinmarketcap, binance]
        '''
           
        if vender == "coinmarketcap":
            v,p,m,col = "vol","close","mktcap","coin_id"
        elif vender == "binance":
            v,p,m,col = "tvol","prc","mcap","permno"
           
        self.vol = pd.pivot_table(data=data,
                             values=v,
                             index=data.index, 
                             columns=col)
        self.price = pd.pivot_table(data=data,
                               values=p, 
                               index=data.index, 
                               columns=col).replace({0:np.nan})
        self.mktcap = pd.pivot_table(data=data,
                                values=m, 
                                index=data.index, 
                                columns=col)
        self.daily_rtn = self.price.pct_change(fill_method=None)
    
    
    def make_market_index(self, mktcap_value=None, vol_value=None):
        '''
        return value weighted market index(Series)
        mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
        vol_value : vol_value 이하는 스크리닝 (MA30)

        -> Return : 마켓 수익률, 코인 개수 
        '''
        # vol만 조건을 거는 경우는 없을 것으로 가정하고 코드 구현
        if mktcap_value != None:
            mktcap_mask = (self.mktcap.rolling(window=30).mean() > mktcap_value) \
                                           .replace({True:1, False:np.nan})
            if vol_value != None:
                vol_mask = (self.vol.rolling(window=30).mean() > vol_value) \
                                         .replace({True:1, False:np.nan})
                mask = mktcap_mask * vol_mask
            else: # mkt만 조건이 있고, vol은 조건이 없는 경우
                mask = mktcap_mask
        
        else: # 아무 조건이 안 걸리는 경우 (전부 1로 채운 mask 생성)
            mask = pd.DataFrame(1, index=self.mktcap.index, columns=self.mktcap.columns)
        
        mktcap_screened = self.mktcap * mask
        # mktcap, vol 스크리닝을 했기 때문에, 코인이 1개라도 포함되는 시작일을 찾아야 한다(mkt index 계산을 위해서)
        start_idx = (mktcap_screened.isna().sum(1) < mktcap_screened.shape[1]) \
                                    .replace(False,np.nan).dropna().index[0]
                                    
        weight = mktcap_screened.loc[start_idx:].apply(lambda x: x / np.nansum(x), axis=1)
        mkt_rtn = self.daily_rtn.loc[start_idx:] * weight.shift(1)
        time_series_coin_num = mkt_rtn.count(axis=1)
        mkt_index = mkt_rtn.sum(1) 
        
        return mkt_index, time_series_coin_num # 수익이 담긴 pd.Series


    def weekly_momentum_w(self, group_num:int):
        '''
        group_num : 몇 개의 그룹으로 나눌 지 
        '''
        
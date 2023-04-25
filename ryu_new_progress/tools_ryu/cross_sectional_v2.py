import numpy as np 
import pandas as pd 
from tqdm import tqdm

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
        self.__group_initialized = False
        
    def __make_mask(self, mktcap_df, vol_df): 
        # 마스크를 생성해서 리턴합니다
        # vol만 조건을 거는 경우는 없을 것으로 가정하고 코드 구현
        if self.mktcap_value != None:
            mktcap_mask = (mktcap_df.rolling(window=30).mean() > self.mktcap_value) \
                                 .replace({True:1, False:np.nan})
            if self.vol_value != None:
                vol_mask = (vol_df.rolling(window=30).mean() > self.vol_value) \
                                         .replace({True:1, False:np.nan})
                mask = mktcap_mask * vol_mask
            else: # mkt만 조건이 있고, vol은 조건이 없는 경우
                mask = mktcap_mask
        
        else: # 아무 조건이 안 걸리는 경우 (전부 1로 채운 mask 생성)
            mask = pd.DataFrame(1, index= mktcap_df.index, columns=mktcap_df.columns) 
        
        return mask
    
    def __make_weekly_momentum_mask(self):
        '''
        Weekly Momentum을 기준으로 그룹을 나눈 후, 그룹의 마스크를 반환합니다
        '''

        mask = self.__make_mask(self.mktcap,self.vol)
        mktcap_screened = self.mktcap * mask
        start_idx = (mktcap_screened.isna().sum(1) < mktcap_screened.shape[1]) \
                                    .replace(False,np.nan).dropna().index[0]
        mktcap_screened = mktcap_screened[start_idx:]
        
        # 주간 데이터를 생성합니다
        self.weekly_mktcap = mktcap_screened.resample("W-"+self.day_of_week).last()
        self.weekly_rtn = self.price[start_idx:].pct_change(7,fill_method=None).resample("W-"+self.day_of_week).last()
        self.weekly_mask = mask[start_idx:].resample("W-"+self.day_of_week).last() 
        
        weekly_rtn_masked = self.weekly_rtn * self.weekly_mask # 그룹을 나눌때 사용함 
        
        # 언제부터 시작하는 지 (최소 q*n개의 코인이 필요)
        cnt = weekly_rtn_masked.count(1)
        more100 = cnt.loc[cnt >= (self.group_num * self.number_of_coin_group)] # 여기서 start date가 나온다
        self.strategy_start = more100.index[0] 
        
        # rank 계산
        rank = weekly_rtn_masked[self.strategy_start:].rank(axis=1, method="first")
        coin_count = rank.count(axis=1)  
        rank_thresh = coin_count.apply(lambda x: [i for i in range(0,x, x//self.group_num)])
        
        group_mask_dict = {}
        
        # rank 기반으로 그룹을 나눈다
        for i in tqdm(range(1, self.group_num+1)):
            
            if i == 1: # 처음
                thresh = rank_thresh.apply(lambda x: x[i])
                group_mask = rank.apply(lambda x: x <= thresh, axis=0).replace({True:1, False:np.nan})
            elif i == self.group_num: # 마지막
                thresh = rank_thresh.apply(lambda x: x[i-1])
                group_mask = rank.apply(lambda x: thresh < x, axis=0).replace({True:1, False:np.nan})
            else:
                thresh = rank_thresh.apply(lambda x: x[i])
                thresh_1 = rank_thresh.apply(lambda x: x[i-1]) # 뒤에거를 가져와야함
                group_mask = rank.apply(lambda x: (thresh_1 < x) & (x <= thresh), axis=0).replace({True:1, False:np.nan})
                
            group_mask_dict[f"G{i}"] = group_mask
        
        return group_mask_dict


    def __simulate_strategy(self, group_weight:pd.DataFrame):
        '''
        전략의 수익을 평가합니다
        '''
        strategy_rtn = {}
        pf_value = 1
        
        for t in group_weight.index:
            dollar_value = group_weight.loc[t] * pf_value    # 포트폴리오가 담을 각 코인의 달러가치
            # 여기까지가 t기 close에 momentum을 계산하고, 몇 개의 코인을 살지 결정한 것이다
            
            ## t+1기 부터 t+7기 close까지 수익을 계산해야 한다
            t_1, t_7 = t + pd.Timedelta(days=1), t + pd.Timedelta(days=7)
            for date in pd.date_range(t_1, t_7):
                
                if date > self.daily_rtn.index[-1]: # 우리가 가진 데이터의 기간 밖이면 break
                    break
                
                dollar_value = dollar_value * (1+self.daily_rtn.loc[date]) #코인의 dollar value 변화를 추적
                pf_value = dollar_value.sum()
                strategy_rtn[str(date.strftime("%Y-%m-%d"))] = pf_value
        # 저장
        pf_result = pd.Series(strategy_rtn)
        pf_result.index = pd.to_datetime(pf_result.index)
        pf_result[pf_result.index[0] - pd.Timedelta(days=1)] = 1 # 투자 시작일 포트폴리오 가치를 1로 셋팅
        pf_result = pf_result.sort_index().pct_change().fillna(0)
        
        return pf_result


    def make_market_index(self, mktcap_value=None, vol_value=None):
        '''
        return value weighted market index(Series)
        mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
        vol_value : vol_value 이하는 스크리닝 (MA30)

        -> Return : 마켓 수익률, 코인 개수 
        '''
        # vol만 조건을 거는 경우는 없을 것으로 가정하고 코드 구현
        self.mktcap_value = mktcap_value
        self.vol_value = vol_value
        
        mask = self.__make_mask(self.mktcap, self.vol)
        mktcap_screened = self.mktcap * mask
        
        # mktcap, vol 스크리닝을 했기 때문에, 코인이 1개라도 포함되는 시작일을 찾아야 한다(mkt index 계산을 위해서)
        start_idx = (mktcap_screened.isna().sum(1) < mktcap_screened.shape[1]) \
                                    .replace(False,np.nan).dropna().index[0]
                                    
        weight = mktcap_screened.loc[start_idx:].apply(lambda x: x / np.nansum(x), axis=1)
        mkt_rtn = self.daily_rtn.loc[start_idx:] * weight.shift(1)
        time_series_coin_num = mkt_rtn.count(axis=1)
        mkt_index = mkt_rtn.sum(1) 
        
        return mkt_index, time_series_coin_num # 수익이 담긴 pd.Series


    def weekly_momentum_value_weighted(self, group_num:int, day_of_week:str, number_of_coin_group:int, mktcap_value=None, vol_value=None):
        '''
        group_num : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
        vol_value : vol_value 이하는 스크리닝 (MA30)

        주간 리벨런싱을 진행합니다 / Value Weighted로 투자 비중을 생성합니다
        Return -> final_value, group_coin_count
        '''
        self.group_num = group_num
        self.day_of_week = day_of_week
        self.number_of_coin_group = number_of_coin_group
        self.mktcap_value = mktcap_value
        self.vol_value = vol_value
        
        group_coin_count = {}
        final_value = {}
               
        group_mask_dict = self.__make_weekly_momentum_mask()
                
        for key, mask in tqdm(group_mask_dict.items()):    
            # 그룹의 value weighted weight 생성
            group_weight = (self.weekly_mktcap[self.strategy_start:] * mask).apply(lambda x: x/np.nansum(x), axis=1)
            group_coin_count[key] = group_weight.count(1)
            
            # 여기서부터 투자 성과를 측정합니다
            final_value[key] = self.__simulate_strategy(group_weight=group_weight)
            
        # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 롱숏)
        long_short = final_value[key] - final_value["G1"]
        final_value["Long-Short"] = long_short
        
        return final_value, group_coin_count
    
    
    def weekly_momentum_momentum_weighted(self, group_num:int, day_of_week:str, number_of_coin_group:int, mktcap_value=None, vol_value=None):
        '''
        group_num : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
        vol_value : vol_value 이하는 스크리닝 (MA30)

        주간 리벨런싱을 진행합니다 / Momentum 순으로 투자 비중을 생성합니다 (pct_rank)
        Return -> final_value, group_coin_count
        '''
        self.group_num = group_num
        self.day_of_week = day_of_week
        self.number_of_coin_group = number_of_coin_group
        self.mktcap_value = mktcap_value
        self.vol_value = vol_value
        
        group_coin_count = {}
        final_value = {}
               
        group_mask_dict = self.__make_weekly_momentum_mask() # value weighted나 이거나 group mask는 동일하다
                
        for key, mask in tqdm(group_mask_dict.items()):    
            # 그룹의 value weighted weight 생성
            group_weekly_rtn = (self.weekly_rtn[self.strategy_start:] * mask)
            group_weight = group_weekly_rtn.rank(axis=1, method="first") \
                                           .apply(lambda x: x / np.nansum(x), axis=1)
            group_coin_count[key] = group_weight.count(1)
            
            # G1은 숏칠때 weight를 다르게 줘야함 (ascending=False)
            if key =="G1":
                group_weight_short = group_weekly_rtn.rank(axis=1, method="first", ascending=False) \
                                                     .apply(lambda x: x / np.nansum(x), axis=1)
                g1_short_strategy = self.__simulate_strategy(group_weight=group_weight_short)
                final_value["G1_short"] = -g1_short_strategy
                                                     
            # 여기서부터 투자 성과를 측정합니다
            final_value[key] = self.__simulate_strategy(group_weight=group_weight)
            
        # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 ascending=False로 계산한 숏 수익이 들어가야함)
        long_short = final_value[key] + g1_short_strategy
        final_value["Long-Short"] = long_short
        
        return final_value, group_coin_count
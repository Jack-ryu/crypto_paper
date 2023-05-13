import numpy as np 
import pandas as pd 
from tqdm import tqdm

###########################v########################
#   Weekly weight 뱃도록 수정하고,
# 벡테스팅 다시한번 살펴보기 / 포트폴리오 리턴 뽑아서, 그 포트폴리오 두개로 다시 일주일 리벨런싱하는 전략도 고민해보자
##############################################################

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
        for i in range(1, self.group_num+1):
            
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
        pf_value = 1
        pf_dict = {}

        weight = group_weight.iloc[0] # 시작 weight를 지정해준다(첫 weight에서 투자 시작, 장마감 직전에 포트폴리오 구성)
        rebalancing_idx = group_weight.index

        for idx, row in self.daily_rtn.loc[self.strategy_start:].iterrows(): # Daily로 반복문을 돌린다
            # 수익률 평가가 리밸런싱보다 선행해야함
            dollar_value = weight * pf_value
            dollar_value = dollar_value * (1+np.nan_to_num(row)) # update the dollar value
            weight = dollar_value / dollar_value.sum()   # update the weight (여기서 문제가 있겠군 Long-Short일 경우...)
            pf_value = dollar_value.sum() # update the pf value

            if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
                weight = group_weight.loc[idx]
                target_dollar_value = np.nan_to_num(pf_value * weight)
                dollar_fee = np.nansum(np.abs(target_dollar_value - np.nan_to_num(dollar_value)) * self.fee_rate)
                pf_value = pf_value - dollar_fee # fee 차감
                
            pf_dict[idx] = pf_value
        # 결과를 pct로 정렬
        pf_result = pd.Series(pf_dict)
        idx = pf_result.index[0] - pd.Timedelta(days=1)
        pf_result[idx] = 1
        pf_result.sort_index(inplace=True)
        pf_result = pf_result.pct_change().fillna(0)
        
        return pf_result


    def __simulate_strategy_long_short(self, long_weight_df:pd.DataFrame, short_weight_df:pd.DataFrame):
        '''
        전략의 수익을 평가합니다 (long-short)
        '''
        pf_value = 1
        pf_dict = {}

        long_weight = long_weight_df.iloc[0]  # 시작 weight를 지정해준다(첫 weight에서 투자 시작, 장마감 직전에 포트폴리오 구성)
        short_weight = short_weight_df.iloc[0]
        rebalancing_idx = long_weight_df.index

        for idx, rtn in self.daily_rtn.loc[self.strategy_start:].iterrows(): # Daily로 반복문을 돌린다
            # 수익률 평가가 리밸런싱보다 선행해야함
            dollar_value_of_sell = short_weight * pf_value
            dollar_value_of_buy = long_weight * pf_value # dollar_value_of_sell - dollar_value_of_buy=0 성립
            
            short_rtn = (short_weight * np.nan_to_num(-rtn)) + 1
            long_rtn = (long_weight * np.nan_to_num(rtn)) + 1
        
            # Update each dollar value
            dollar_value_of_sell_update = dollar_value_of_sell * short_rtn
            dollar_value_of_sell_update = np.where(dollar_value_of_sell_update > 0, dollar_value_of_sell_update, 0) # pf 가치가 음수로 안내려가도록 청산 시 0이 된다
            dollar_value_of_buy_update = dollar_value_of_buy * long_rtn
            
            # Update the portfolio value of every days
            dollar_gain_sell = np.nansum(dollar_value_of_sell_update - dollar_value_of_sell)
            dollar_gain_buy = np.nansum(dollar_value_of_buy_update - dollar_value_of_buy)
            pf_value = pf_value + dollar_gain_buy #+ dollar_gain_sell

            # Update the weight vectors
            short_weight = dollar_value_of_sell_update / np.nansum(dollar_value_of_sell_update)
            long_weight = dollar_value_of_buy_update / np.nansum(dollar_value_of_buy_update)
            
            # 변수 다시 원래대로
            dollar_value_of_sell = dollar_value_of_sell_update
            dollar_value_of_buy = dollar_value_of_buy_update
            
            pf_dict[idx] = pf_value
            
            if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
                long_weight = long_weight_df.loc[idx]   # target weight 
                short_weight = short_weight_df.loc[idx]
                
                target_dollar_value_of_sell = short_weight * pf_value # Cash inflow
                target_dollar_value_of_buy = long_weight * pf_value # Cash outflow  : dollar_value_of_sell+dollar_value_of_buy=0 성립
                
                dv_delta_sell = np.abs(np.nan_to_num(target_dollar_value_of_sell) - np.nan_to_num(dollar_value_of_sell))
                dv_delta_buy = np.abs(np.nan_to_num(target_dollar_value_of_buy) - np.nan_to_num(dollar_value_of_buy))
                
                fee = np.nansum(dv_delta_buy) * self.fee_rate#(np.nansum(dv_delta_sell) + np.nansum(dv_delta_buy)) * self.fee_rate
                pf_value = pf_value - fee
            
        # 결과를 pct로 정렬
        pf_result = pd.Series(pf_dict)
        idx = pf_result.index[0] - pd.Timedelta(days=1)
        pf_result[idx] = 1
        pf_result.sort_index(inplace=True)
        pf_result = pf_result.pct_change().fillna(0)
        
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
        mkt_rtn = (self.daily_rtn.loc[start_idx:] * weight.shift(1))
        time_series_coin_num = mkt_rtn.count(axis=1)
        mkt_index = mkt_rtn.sum(1) 
        
        return mkt_index, time_series_coin_num # 수익이 담긴 pd.Series


    def weekly_momentum_value_weighted(self, group_num:int, day_of_week:str, number_of_coin_group:int, mktcap_value=None, vol_value=None, fee_rate=0):
        '''
        group_num : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
        vol_value : vol_value 이하는 스크리닝 (MA30)
        fee_rate : 거래비용

        주간 리벨런싱을 진행합니다 / Value Weighted로 투자 비중을 생성합니다
        Return -> final_value, group_coin_count
        '''
        self.group_num = group_num
        self.day_of_week = day_of_week
        self.number_of_coin_group = number_of_coin_group
        self.mktcap_value = mktcap_value
        self.vol_value = vol_value
        self.fee_rate = fee_rate
        
        group_coin_count = {}
        final_value = {}
               
        group_mask_dict = self.__make_weekly_momentum_mask()
        group_weight_list = []
                
        for key, mask in group_mask_dict.items():    
            # 그룹의 value weighted weight 생성
            group_weight = (self.weekly_mktcap[self.strategy_start:] * mask).apply(lambda x: x/np.nansum(x), axis=1)
            group_weight_list.append(group_weight)   # v5 추가
            group_coin_count[key] = group_weight.count(1)
            
            # 투자 성과를 측정합니다
            #final_value["Long_" + str(key)] = self.__simulate_strategy(group_weight=group_weight)
            
        # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 롱숏) v5 추가
        #final_value["Long-Short"] = self.__simulate_strategy_long_short(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0])
        
        return group_weight_list
    
    
    #def weekly_momentum_momentum_weighted(self, group_num:int, day_of_week:str, number_of_coin_group:int, mktcap_value=None, vol_value=None, fee_rate=0):
    #    '''
    #    group_num : 몇 개의 그룹으로 나눌 지
    #    day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
    #    number_of_coin_group : 그룹당 최소 필요한 코인 수
    #    mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
    #    vol_value : vol_value 이하는 스크리닝 (MA30)
    #    fee_rate : 거래 비용
    #    
    #    주간 리벨런싱을 진행합니다 / Momentum 순으로 투자 비중을 생성합니다 (pct_rank)
    #    Return -> final_value, group_coin_count
    #    '''
    #    self.group_num = group_num
    #    self.day_of_week = day_of_week
    #    self.number_of_coin_group = number_of_coin_group
    #    self.mktcap_value = mktcap_value
    #    self.vol_value = vol_value
    #    self.fee_rate = fee_rate
    #    
    #    group_coin_count = {}
    #    final_value = {}
    #           
    #    group_mask_dict = self.__make_weekly_momentum_mask() # value weighted나 이거나 group mask는 동일하다
    #            
    #    for key, mask in tqdm(group_mask_dict.items()):    
    #        # 그룹의 value weighted weight 생성
    #        group_weekly_rtn = (self.weekly_rtn[self.strategy_start:] * mask)
    #        group_weight = group_weekly_rtn.rank(axis=1, method="first") \
    #                                       .apply(lambda x: x / np.nansum(x), axis=1)
    #        group_coin_count[key] = group_weight.count(1)
    #        
    #        # G1은 숏칠때 weight를 다르게 줘야함 (ascending=False)
    #        if key =="G1":
    #            group_weight_short = group_weekly_rtn.rank(axis=1, method="first", ascending=False) \
    #                                                 .apply(lambda x: x / np.nansum(x), axis=1)
    #            g1_short_strategy = self.__simulate_strategy(group_weight=group_weight_short)
    #            final_value["G1_short"] = -g1_short_strategy
    #                                                 
    #        # 투자 성과를 측정합니다
    #        final_value[key] = self.__simulate_strategy(group_weight=group_weight)
    #        
    #    # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 ascending=False로 계산한 숏 수익이 들어가야함)
    #    long_short = final_value[key] + g1_short_strategy
    #    final_value["Long-Short"] = long_short
    #    
    #    return final_value, group_coin_count
    

    def weekly_momentum_value_weighted_capped(self, group_num:int, day_of_week:str, number_of_coin_group:int, mktcap_value=None, vol_value=None, fee_rate=0, num_cap=5):
        '''
        group_num : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
        vol_value : vol_value 이하는 스크리닝 (MA30)
        fee_rate : 거래비용
        num_cap : cap을 씌울 코인 개수 (Ex:5일 경우 그룹별로 Marketcap 상위 5개의 코인의 weight를 가장 낮은 weight로 맞춥니다)

        주간 리벨런싱을 진행합니다 / Value Weighted로 투자 비중을 생성합니다
        Return -> final_value, group_coin_count
        '''
        self.group_num = group_num
        self.day_of_week = day_of_week
        self.number_of_coin_group = number_of_coin_group
        self.mktcap_value = mktcap_value
        self.vol_value = vol_value
        self.fee_rate = fee_rate
        
        group_coin_count = {}
        final_value = {}
               
        group_mask_dict = self.__make_weekly_momentum_mask()
        group_weight_list = []
        
        
                
        for key, mask in tqdm(group_mask_dict.items()):    
            # 그룹의 marketcap 개산
            group_mktcap = (self.weekly_mktcap[self.strategy_start:] * mask).copy()
            # 추가: top num_cap개 코인 marketcap을 동일하게 맞춰야한다
            min = group_mktcap.apply(lambda row: row.nlargest(num_cap).min(), axis=1) # 5번쨰로 큰 마켓켑을 구한다
            top_indices = group_mktcap.apply(lambda row: row.nlargest(num_cap).index.to_list(), axis=1) # 5번째까지 큰 애들의 위치를 가져온다
            
            # 값을 변경한다
            for idx, lst_of_top in top_indices.items():
                group_mktcap.loc[idx, lst_of_top] = min[idx]
            
            # Weight를 계산
            group_weight = group_mktcap.apply(lambda x: x/np.nansum(x), axis=1)
            group_coin_count[key] = group_weight.count(1)
            group_weight_list.append(group_weight)
            
            # 투자 성과를 측정합니다
            #final_value["Long" + str(key)] = self.__simulate_strategy(group_weight=group_weight)
            
        # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 롱숏)
        #final_value["Long-Short"] = self.__simulate_strategy_long_short(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0])
        
        return group_weight_list
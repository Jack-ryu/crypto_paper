import numpy as np
import pandas as pd 
import ray

from multi_run.backtest_v2 import *   # 벡테스팅 버전 바뀌면 여기를 수정해야함
from multi_run.initialize_v2 import make_weekly_momentum_mask

ray.init(num_cpus=16, ignore_reinit_error=True)
 
 
@ ray.remote 
def weekly_momentum_value_weighted(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame,
                                   n_group:int, day_of_week:str, number_of_coin_group:int, mktcap_thresh=None, vol_thresh=None,
                                   fee_rate=0):
    '''
    Value Weighted로 Cross-Sectional Momentum 투자 비중을 생성합니다
    
        group_num : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_value : mktcap_value 이하는 스크리닝 (MA30)
        vol_value : vol_value 이하는 스크리닝 (MA30)
        fee_rate : 거래비용
    
    Return -> final_value, group_coin_count
    '''
    daily_rtn_df = price_df.pct_change(fill_method=None)
    
    #group_coin_count = {}
    final_value = {}
           
    group_mask_dict = make_weekly_momentum_mask(price_df, mktcap_df, vol_df, n_group, day_of_week, number_of_coin_group, mktcap_thresh, vol_thresh)
    group_weight_list = []
            
    for key, mask in group_mask_dict.items():
        # 그룹의 value weighted weight 생성
        real_idx = [idx for idx in mask.index if idx in mktcap_df.index[:-1]] # 2023-05-12 추가
        weekly_mktcap = mktcap_df.loc[real_idx]  # Binance인 경우 [:-1] 추가해야함
        group_weight = (weekly_mktcap * mask).apply(lambda x: x/np.nansum(x), axis=1)
        group_weight_list.append(group_weight)   # v5 추가(롱숏계산을 위해)
        #group_coin_count[key] = group_weight.count(1)
        
        # 투자 성과를 측정합니다
        final_value["Long_" + str(key)] = simulate_strategy(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
        
    # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 롱숏) v5 추가
    final_value["Long-Short"] = simulate_strategy_long_short(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
                                                             daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
    
    return final_value#,  group_coin_count
 
    
@ray.remote
def weekly_momentum_value_weighted_capped(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame,
                                          n_group:int, day_of_week:str, number_of_coin_group:int,
                                          mktcap_thresh=None, vol_thresh=None, fee_rate=0, num_cap=5):
    '''
    Value Weighted로 투자 비중을 생성합니다 (상위 num_cap만큼 marketcap을 제한 해준다)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_thresh 이하는 스크리닝 (MA30)
        vol_thresh : vol_thresh 이하는 스크리닝 (MA30)
        fee_rate : 거래비용
        num_cap : cap을 씌울 코인 개수 (Ex:5일 경우 그룹별로 Marketcap 상위 5개의 코인의 weight를 가장 낮은 weight로 맞춥니다)

        Return -> final_value, group_coin_count
    '''
    daily_rtn_df = price_df.pct_change(fill_method=None)
    
    #group_coin_count = {}
    final_value = {}
           
    group_mask_dict = make_weekly_momentum_mask(price_df, mktcap_df, vol_df, n_group, day_of_week, number_of_coin_group, mktcap_thresh, vol_thresh)
    group_weight_list = []
    
    for key, mask in group_mask_dict.items():   
        real_idx = [idx for idx in mask.index if idx in mktcap_df.index] # 2023-05-12 추가
        weekly_mktcap = mktcap_df.loc[real_idx]
        # 그룹의 marketcap 개산
        group_mktcap = (weekly_mktcap * mask).copy()
        # 추가: top num_cap개 코인 marketcap을 동일하게 맞춰야한다
        min = group_mktcap.apply(lambda row: row.nlargest(num_cap).min(), axis=1) # 5번쨰로 큰 마켓켑을 구한다
        top_indices = group_mktcap.apply(lambda row: row.nlargest(num_cap).index.to_list(), axis=1) # 5번째까지 큰 애들의 위치를 가져온다
        
        # 값을 변경한다
        for idx, lst_of_top in top_indices.items():
            group_mktcap.loc[idx, lst_of_top] = min[idx]
        
        # Weight를 계산
        group_weight = group_mktcap.apply(lambda x: x/np.nansum(x), axis=1)
        #group_coin_count[key] = group_weight.count(1)
        group_weight_list.append(group_weight)
                
        # 투자 성과를 측정합니다
        final_value["Long_" + str(key)] = simulate_strategy(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
            
    # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 롱숏)
    final_value["Long-Short"] = simulate_strategy_long_short(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
                                                             daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
    
    return final_value#, group_coin_count


@ray.remote
def weekly_momentum_momentum_weighted(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame,
                                      n_group:int, day_of_week:str, number_of_coin_group:int,
                                      mktcap_thresh=None, vol_thresh=None, fee_rate=0):
    
    '''
    Momentum 순으로 투자 비중을 생성합니다 (pct_rank)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_value 이하는 스크리닝 (MA30)
        vol_thresh : vol_value 이하는 스크리닝 (MA30)
        fee_rate : 거래 비용
     
    Return -> final_value, group_coin_count
    '''
    
    daily_rtn_df = price_df.pct_change(fill_method=None)
    
    #group_coin_count = {}
    final_value = {}
    group_weight_list = []
           
    group_mask_dict = make_weekly_momentum_mask(price_df, mktcap_df, vol_df, n_group, day_of_week, number_of_coin_group, mktcap_thresh, vol_thresh)
    weekly_rtn = price_df.pct_change(7,fill_method=None).resample("W-"+day_of_week).last()
    
    for key, mask in group_mask_dict.items():  
        # Momentum Weighted 수익 계산  
        group_weekly_rtn = (weekly_rtn.loc[mask.index] * mask)
        group_weight = group_weekly_rtn.rank(axis=1, method="first").apply(lambda x: x / np.nansum(x), axis=1)
        #group_coin_count[key] = group_weight.count(1)
        
        # G1은 숏칠때 weight를 다르게 줘야함 (ascending=False)
        if key =="Q1":
            group_weight_short = group_weekly_rtn.rank(axis=1, method="first", ascending=False).apply(lambda x: x / np.nansum(x), axis=1)
            
        group_weight_list.append(group_weight)
                   
        # 투자 성과를 측정합니다(Q1~Q(n) Long의 성과)
        final_value["Long" + str(key)] = simulate_strategy(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
        
    # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 ascending=False로 계산한 숏 수익이 들어가야함)
    final_value["Long-Short"] = simulate_strategy_long_short(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_short,
                                                             daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
    
    return final_value#, group_coin_count


@ray.remote
def weekly_momentum_momentum_weighted_capped(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame,
                                             n_group:int, day_of_week:str, number_of_coin_group:int,
                                             mktcap_thresh=None, vol_thresh=None, fee_rate=0, num_cap=5):
    
    '''
    Momentum 순으로 투자 비중을 생성합니다 (Weighted를 Capped 해줍니다)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_value 이하는 스크리닝 (MA30)
        vol_thresh : vol_value 이하는 스크리닝 (MA30)
        fee_rate : 거래 비용
        num_cap : weight cap을 적용할 코인의 수
     
    Return -> final_value, group_coin_count
    '''
    
    daily_rtn_df = price_df.pct_change(fill_method=None)
    
    #group_coin_count = {}
    final_value = {}
    group_weight_list = []
           
    group_mask_dict = make_weekly_momentum_mask(price_df, mktcap_df, vol_df, n_group, day_of_week, number_of_coin_group, mktcap_thresh, vol_thresh)
    weekly_rtn = price_df.pct_change(7,fill_method=None).resample("W-"+day_of_week).last()
    
    for key, mask in group_mask_dict.items():  
        # Momentum Weighted 수익 계산  
        group_weekly_rtn = (weekly_rtn.loc[mask.index] * mask)
        group_momentum_rank = group_weekly_rtn.rank(axis=1, method="first")
        
        min = group_momentum_rank.apply(lambda row: row.nlargest(num_cap).min(), axis=1) # 5번쨰로 큰 rank를 구한다
        top_indices = group_momentum_rank.apply(lambda row: row.nlargest(num_cap).index.to_list(), axis=1) # 5번째까지 큰 애들의 위치를 가져온다
        
        # 값을 변경한다
        for idx, lst_of_top in top_indices.items():
            group_momentum_rank.loc[idx, lst_of_top] = min[idx]
            
        group_weight = group_weekly_rtn.apply(lambda x: x / np.nansum(x), axis=1)
        
        #group_coin_count[key] = group_weight.count(1)
        
        # G1은 숏칠때 weight를 다르게 줘야함 (ascending=False)
        if key =="Q1":
            group_short_rank = group_weekly_rtn.rank(axis=1, method="first", ascending=False)
            min_ = group_short_rank.apply(lambda row: row[::-1].nlargest(num_cap).min(), axis=1) # 5번쨰로 큰 rank를 구한다
            top_indices_ = group_short_rank.apply(lambda row: row.nlargest(num_cap).index.to_list(), axis=1) # 5번째까지 큰 애들의 위치를 가져온다
        
            # 값을 변경한다
            for idx, lst_of_top in top_indices_.items():
                group_short_rank.loc[idx, lst_of_top] = min_[idx]

            group_short_weight = group_short_rank.apply(lambda x: x / np.nansum(x), axis=1)
        
        group_weight = group_weekly_rtn.apply(lambda x: x / np.nansum(x), axis=1)
        group_weight_list.append(group_weight)
                   
        # 투자 성과를 측정합니다(Q1~Q(n) Long의 성과)
        final_value["Long" + str(key)] = simulate_strategy(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
        
    # 롱숏 계산 (마지막 계산 그룹과 첫번째 그룹의 ascending=False로 계산한 숏 수익이 들어가야함)
    final_value["Long-Short"] = simulate_strategy_long_short(long_weight_df=group_weight_list[-1], short_weight_df=group_short_weight,
                                                             daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
    
    return final_value#, group_coin_count
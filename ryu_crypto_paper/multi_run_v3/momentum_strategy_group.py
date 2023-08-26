import numpy as np
import pandas as pd 

import ray

from multi_run_v3.momentum import make_group_mask#, make_group_jk_mask
#from multi_run_v3.backtest_v3 import simulate_longonly, simulate_longshort

ray.init(num_cpus=16, ignore_reinit_error=True)

@ ray.remote 
def weekly_momentum_value_weighted_group(price_df:pd.DataFrame, mktcap_df : pd.DataFrame,
                                        weekly_rtn_df:pd.DataFrame, mask_df:pd.DataFrame,
                                        n_group:int, day_of_week:str, 
                                        coin_group:int=20, look_back:int=7):
    '''
    Value Weighted로 Cross-Sectional Momentum 투자 비중을 생성합니다
    
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        fee_rate : 거래비용
    
    Return -> final_value, group_coin_count
    '''
    
    group_weight_list = []
    
    if look_back != 7:
        weekly_rtn_df = price_df.pct_change(look_back, fill_method=None)
        
    group_mask_dict = make_group_mask(price_df = price_df, weekly_rtn_df = weekly_rtn_df, 
                                      mask_df = mask_df, n_group = n_group, day_of_week = day_of_week,
                                      coin_group=coin_group)
    mktcap_pp = mktcap_df.loc[group_mask_dict["Q1"].index]  # Weekly mktcap이 나온다

    for key, mask in group_mask_dict.items():
        # 그룹의 value weighted weight 생성
        group_weight = (mktcap_pp * mask).apply(lambda x: x / np.nansum(x), axis=1)
        group_weight_list.append(group_weight)   # v5 추가(롱숏 계산을 위해)    

    return group_weight_list


@ray.remote
def weekly_momentum_volume_weighted(price_df:pd.DataFrame, mktcap_df : pd.DataFrame, vol_df:pd.DataFrame,
                                           daily_rtn_df:pd.DataFrame, weekly_rtn_df:pd.DataFrame, mask_df:pd.DataFrame,
                                           fee_rate:float, n_group:int, day_of_week:str, margin:str='cross',
                                           leverage_ratio:int=1, coin_group:int=20, look_back:int=7):
    '''
    Value Weighted로 투자 비중을 생성합니다 (상위 num_cap만큼 marketcap을 제한 해준다)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_thresh 이하는 스크리닝 (MA30)
        vol_thresh : vol_thresh 이하는 스크리닝 (MA30)
        fee_rate : 거래비용

        Return -> final_value, group_coin_count
    '''
    group_weight_list = []   
    
    if look_back != 7:
        weekly_rtn_df = price_df.pct_change(look_back, fill_method=None)
    
    group_mask_dict = make_group_mask(price_df = price_df, weekly_rtn_df = weekly_rtn_df, 
                                      mask_df = mask_df, n_group = n_group, day_of_week = day_of_week,
                                      coin_group=coin_group)
    mktcap_pp = mktcap_df.loc[group_mask_dict["Q1"].index]
    vol_pp = vol_df.loc[group_mask_dict["Q1"].index]
    mask_pp = mask_df.loc[group_mask_dict["Q1"].index]

    for key, mask in group_mask_dict.items():   
        # 그룹의 marketcap 개산
        group_vol = (vol_pp * mask)

        # Weight를 계산
        group_weight = group_vol.apply(lambda x: x / np.nansum(x), axis=1)
        group_weight_list.append(group_weight) 
        
    return group_weight_list           
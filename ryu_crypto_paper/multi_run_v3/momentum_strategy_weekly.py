import numpy as np
import pandas as pd 

import ray

from multi_run_v3.momentum import make_group_mask, make_group_jk_mask
from multi_run_v3.backtest import simulate_longonly, simulate_longshort

ray.init(num_cpus=16, ignore_reinit_error=True)

######################## - V3 - 2023-07-05 Edited ######################
# Value Capped Weekly로 리턴 찍는거 계산
# ######################################################################

@ ray.remote 
def weekly_momentum_value_weighted(price_df:pd.DataFrame, mktcap_df : pd.DataFrame,
                                   daily_rtn_df:pd.DataFrame, weekly_rtn_df:pd.DataFrame, mask_df:pd.DataFrame,
                                   fee_rate:float, n_group:int, day_of_week:str):
    '''
    Value Weighted로 Cross-Sectional Momentum 투자 비중을 생성합니다
    
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        fee_rate : 거래비용
    
    Return -> final_value, group_coin_count
    '''
    
    #group_coin_count = {}
    final_value = {}
    group_weight_list = []
    
    group_mask_dict = make_group_mask(price_df = price_df, weekly_rtn_df = weekly_rtn_df, 
                                      mask_df = mask_df, n_group = n_group, day_of_week = day_of_week)
    mktcap_pp = mktcap_df.loc[group_mask_dict["Q1"].index]  # Weekly mktcap이 나온다

    for key, mask in group_mask_dict.items():
        # 그룹의 value weighted weight 생성
        group_weight = (mktcap_pp * mask).apply(lambda x: x / np.nansum(x), axis=1)
        #group_coin_count[key] = group_weight.count(1)
        group_weight_list.append(group_weight)   # v5 추가(롱숏 계산을 위해)
        # 투자 성과를 측정합니다
        final_value["Long_" + str(key)] = simulate_longonly(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
             
    # 롱숏 계산 
    final_value["LS-cross"] = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
                                                 daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="cross")
    #final_value["LS-isolate"] = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
    #                                             daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="isolate")
    
    return final_value  # group_coin_count

    
@ray.remote
def weekly_momentum_value_weighted_capped(price_df:pd.DataFrame, mktcap_df : pd.DataFrame,
                                          daily_rtn_df:pd.DataFrame, weekly_rtn_df:pd.DataFrame, mask_df:pd.DataFrame,
                                          fee_rate:float, n_group:int, day_of_week:str, num_cap:float):
    '''
    Value Weighted로 투자 비중을 생성합니다 (상위 num_cap만큼 marketcap을 제한 해준다)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_thresh 이하는 스크리닝 (MA30)
        vol_thresh : vol_thresh 이하는 스크리닝 (MA30)
        fee_rate : 거래비용
        num_cap : 0.95 -> 5% cap을 의미함

        Return -> final_value, group_coin_count
    '''
    #group_coin_count = {}
    final_value = {}
    group_weight_list = []   
    
    group_mask_dict = make_group_mask(price_df = price_df, weekly_rtn_df = weekly_rtn_df, 
                                      mask_df = mask_df, n_group = n_group, day_of_week = day_of_week)
    mktcap_pp = mktcap_df.loc[group_mask_dict["Q1"].index]
    mask_pp = mask_df.loc[group_mask_dict["Q1"].index]
    
    # 주단위 리턴 계산을 위해 -> 2023-07-05 추가 (weekly 변경사항)
    last_day = daily_rtn_df.index[-1]
    weekly_weekly_rtn = weekly_rtn_df.resample("W-" + day_of_week).last()[:last_day]
    
    # Capped 씌워주기 (전체에서 Cap을 씌우고, 그룹을 나눠준다)
    mktcap_df_used = mktcap_pp.copy() * mask_pp
    mktcap_rank = mktcap_df_used.rank(1)
    rank_thresh = (mktcap_rank.max(1) * num_cap).dropna().map(int) 
    
    # index alignment
    mktcap_rank = mktcap_rank.loc[rank_thresh.index]
    coin_thresh_series = mktcap_rank.eq(rank_thresh, axis=0).idxmax(axis=1) # values가 타겟 코인의 컬럼명, index가 날짜인 시리즈
    filtered = mktcap_rank.apply(lambda x: x > rank_thresh, axis=0)
    
    for date, target_col in coin_thresh_series.items(): # for loop를 돌면서 mktcap을 변경합니다
        target = mktcap_df_used.loc[date, target_col]
        mask_row = filtered.loc[date]
        mktcap_df_used.loc[date, mask_row] = target 
    
    for key, mask in group_mask_dict.items():   
        # 그룹의 marketcap 개산
        group_mktcap = (mktcap_df_used * mask)

        # Weight를 계산
        group_weight = group_mktcap.apply(lambda x: x / np.nansum(x), axis=1)
        #group_coin_count[key] = group_weight.count(1)
        group_weight_list.append(group_weight)   # v5 추가(롱숏 계산을 위해)            
        # 투자 성과를 측정합니다
        final_value["Long_" + str(key)] = simulate_longonly(group_weight_df=group_weight, daily_rtn_df=weekly_weekly_rtn, fee_rate=fee_rate)
            
    # 롱숏 계산 
    final_value["LS-cross"] = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
                                                 daily_rtn_df=weekly_weekly_rtn, fee_rate=fee_rate, margin="cross")
    #final_value["LS-isolate"]  = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
    #                                               daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="isolate")
    
    return final_value # group_coin_count

@ray.remote
def jk_momentum_value_weighted_capped(mktcap_df : pd.DataFrame,
                                      daily_rtn_df:pd.DataFrame, mask_df:pd.DataFrame,
                                      fee_rate:float, n_group:int, day_of_week:str, num_cap:float):
    '''
    Value Weighted로 투자 비중을 생성합니다 (상위 num_cap만큼 marketcap을 제한 해준다)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_thresh 이하는 스크리닝 (MA30)
        vol_thresh : vol_thresh 이하는 스크리닝 (MA30)
        fee_rate : 거래비용
        num_cap : 0.95 -> 5% cap을 의미함

        Return -> final_value, group_coin_count
    '''
    #group_coin_count = {}
    final_value = {}
    group_weight_list = []   
    
    group_mask_dict = make_group_jk_mask(daily_rtn_df= daily_rtn_df, mask_df = mask_df,
                                         n_group = n_group, day_of_week = day_of_week)
    mktcap_pp = mktcap_df.loc[group_mask_dict["Q1"].index]
    mask_pp = mask_df.loc[group_mask_dict["Q1"].index]
    
    # Capped 씌워주기 (전체에서 Cap을 씌우고, 그룹을 나눠준다)
    mktcap_df_used = mktcap_pp.copy() * mask_pp
    mktcap_rank = mktcap_df_used.rank(1)
    rank_thresh = (mktcap_rank.max(1) * num_cap).dropna().map(int) 
    
    # index alignment
    mktcap_rank = mktcap_rank.loc[rank_thresh.index]
    coin_thresh_series = mktcap_rank.eq(rank_thresh, axis=0).idxmax(axis=1) # values가 타겟 코인의 컬럼명, index가 날짜인 시리즈
    filtered = mktcap_rank.apply(lambda x: x > rank_thresh, axis=0)
    
    for date, target_col in coin_thresh_series.items(): # for loop를 돌면서 mktcap을 변경합니다
        target = mktcap_df_used.loc[date, target_col]
        mask_row = filtered.loc[date]
        mktcap_df_used.loc[date, mask_row] = target 
    
    for key, mask in group_mask_dict.items():   
        # 그룹의 marketcap 개산
        group_mktcap = (mktcap_df_used * mask)

        # Weight를 계산
        group_weight = group_mktcap.apply(lambda x: x / np.nansum(x), axis=1)
        #group_coin_count[key] = group_weight.count(1)
        group_weight_list.append(group_weight)   # v5 추가(롱숏 계산을 위해)            
        # 투자 성과를 측정합니다
        final_value["Long_" + str(key)] = simulate_longonly(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
            
    # 롱숏 계산 
    final_value["LS-cross"] = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
                                                 daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="cross")
    #final_value["LS-isolate"]  = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
    #                                               daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="isolate")
    
    return final_value # group_coin_count


@ray.remote
def weekly_momentum_volume_weighted_capped(price_df:pd.DataFrame, mktcap_df : pd.DataFrame, vol_df:pd.DataFrame,
                                           daily_rtn_df:pd.DataFrame, weekly_rtn_df:pd.DataFrame, mask_df:pd.DataFrame,
                                           fee_rate:float, n_group:int, day_of_week:str, num_cap:float):
    '''
    Value Weighted로 투자 비중을 생성합니다 (상위 num_cap만큼 marketcap을 제한 해준다)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_thresh 이하는 스크리닝 (MA30)
        vol_thresh : vol_thresh 이하는 스크리닝 (MA30)
        fee_rate : 거래비용
        num_cap : 0.95 -> 5% cap을 의미함

        Return -> final_value, group_coin_count
    '''
    #group_coin_count = {}
    final_value = {}
    group_weight_list = []   
    
    group_mask_dict = make_group_mask(price_df = price_df, weekly_rtn_df = weekly_rtn_df, 
                                      mask_df = mask_df, n_group = n_group, day_of_week = day_of_week)
    mktcap_pp = mktcap_df.loc[group_mask_dict["Q1"].index]
    vol_pp = vol_df.loc[group_mask_dict["Q1"].index]
    mask_pp = mask_df.loc[group_mask_dict["Q1"].index]
    
    # Capped 씌워주기 (전체에서 Cap을 씌우고, 그룹을 나눠준다)
    vol_pp_used = vol_pp.copy() * mask_pp
    vol_rank = vol_pp_used.rank(1)
    rank_thresh = (vol_rank.max(1) * num_cap).dropna().map(int) 
    
    # index alignment
    vol_rank = vol_rank.loc[rank_thresh.index]
    coin_thresh_series = vol_rank.eq(rank_thresh, axis=0).idxmax(axis=1) # values가 타겟 코인의 컬럼명, index가 날짜인 시리즈
    filtered = vol_rank.apply(lambda x: x > rank_thresh, axis=0)
    
    for date, target_col in coin_thresh_series.items(): # for loop를 돌면서 mktcap을 변경합니다
        target = vol_pp_used.loc[date, target_col]
        mask_row = filtered.loc[date]
        vol_pp_used.loc[date, mask_row] = target 
    
    for key, mask in group_mask_dict.items():   
        # 그룹의 marketcap 개산
        group_vol = (vol_pp_used * mask)

        # Weight를 계산
        group_weight = group_vol.apply(lambda x: x / np.nansum(x), axis=1)
        #group_coin_count[key] = group_weight.count(1)
        group_weight_list.append(group_weight)   # v5 추가(롱숏 계산을 위해)            
        # 투자 성과를 측정합니다
        final_value["Long_" + str(key)] = simulate_longonly(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
            
    # 롱숏 계산 
    final_value["LS-cross"] = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
                                                 daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="cross")
    #final_value["LS-isolate"]  = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
    #                                               daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="isolate")
    
    return final_value # group_coin_count


@ray.remote
def jk_volume_weighted_capped(mktcap_df : pd.DataFrame, vol_df:pd.DataFrame,
                                           daily_rtn_df:pd.DataFrame,  mask_df:pd.DataFrame,
                                           fee_rate:float, n_group:int, day_of_week:str, num_cap:float):
    '''
    Value Weighted로 투자 비중을 생성합니다 (상위 num_cap만큼 marketcap을 제한 해준다)
    
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : mktcap_thresh 이하는 스크리닝 (MA30)
        vol_thresh : vol_thresh 이하는 스크리닝 (MA30)
        fee_rate : 거래비용
        num_cap : 0.95 -> 5% cap을 의미함

        Return -> final_value, group_coin_count
    '''
    #group_coin_count = {}
    final_value = {}
    group_weight_list = []   
    
    group_mask_dict = make_group_jk_mask(daily_rtn_df= daily_rtn_df, mask_df = mask_df,
                                         n_group = n_group, day_of_week = day_of_week)
    vol_pp = vol_df.loc[group_mask_dict["Q1"].index]
    mask_pp = mask_df.loc[group_mask_dict["Q1"].index]
    
    # Capped 씌워주기 (전체에서 Cap을 씌우고, 그룹을 나눠준다)
    vol_pp_used = vol_pp.copy() * mask_pp
    vol_rank = vol_pp_used.rank(1)
    rank_thresh = (vol_rank.max(1) * num_cap).dropna().map(int) 
    
    # index alignment
    vol_rank = vol_rank.loc[rank_thresh.index]
    coin_thresh_series = vol_rank.eq(rank_thresh, axis=0).idxmax(axis=1) # values가 타겟 코인의 컬럼명, index가 날짜인 시리즈
    filtered = vol_rank.apply(lambda x: x > rank_thresh, axis=0)
    
    for date, target_col in coin_thresh_series.items(): # for loop를 돌면서 mktcap을 변경합니다
        target = vol_pp_used.loc[date, target_col]
        mask_row = filtered.loc[date]
        vol_pp_used.loc[date, mask_row] = target 
    
    for key, mask in group_mask_dict.items():   
        # 그룹의 marketcap 개산
        group_vol = (vol_pp_used * mask)

        # Weight를 계산
        group_weight = group_vol.apply(lambda x: x / np.nansum(x), axis=1)
        #group_coin_count[key] = group_weight.count(1)
        group_weight_list.append(group_weight)   # v5 추가(롱숏 계산을 위해)            
        # 투자 성과를 측정합니다
        final_value["Long_" + str(key)] = simulate_longonly(group_weight_df=group_weight, daily_rtn_df=daily_rtn_df, fee_rate=fee_rate)
            
    # 롱숏 계산 
    final_value["LS-cross"] = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
                                                 daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="cross")
    #final_value["LS-isolate"]  = simulate_longshort(long_weight_df=group_weight_list[-1], short_weight_df=group_weight_list[0],
    #                                               daily_rtn_df=daily_rtn_df, fee_rate=fee_rate, margin="isolate")
    
    return final_value # group_coin_count
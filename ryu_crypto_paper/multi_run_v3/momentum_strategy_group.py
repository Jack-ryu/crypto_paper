import numpy as np
import pandas as pd 

import ray

from multi_run_v3.momentum import make_group_mask

ray.init(num_cpus=16, ignore_reinit_error=True)

@ray.remote 
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
def weekly_momentum_volume_weighted_group(price_df:pd.DataFrame, mktcap_df : pd.DataFrame, 
                                          vol_df:pd.DataFrame, weekly_rtn_df:pd.DataFrame,
                                          mask_df:pd.DataFrame, n_group:int,
                                          day_of_week:str, coin_group:int=20, look_back:int=7):
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


def make_group_mask_rtn(price_df:pd.DataFrame, weekly_rtn_df:pd.DataFrame,
                    mask_df:pd.DataFrame, n_group:int, day_of_week:str, 
                    reb:str='1', coin_group:int=20,
                    look_back:int=7): # 1이면 일주일, 2이면 2주일 간격 리벨런싱
    '''
    그룹의 마스크를 반환합니다

        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
    '''
    if look_back != 7:
        weekly_rtn_df = price_df.pct_change(look_back, fill_method=None)
    
    last_day = price_df.index[-1]
    weekly_mask = mask_df.resample(reb + "W-" + day_of_week).last()[:last_day]
    weekly_rtn = weekly_rtn_df.resample(reb + "W-" + day_of_week).last()[:last_day]
    weekly_rtn_masked = weekly_rtn * weekly_mask 
        
    # 언제부터 시작하는 지 (최소 q*n개의 코인이 필요)
    cnt = weekly_rtn_masked.count(1)
    thresh = cnt[cnt >= (n_group * coin_group)] # 여기서 start date가 나온다 / 각 그룹당 최소 20개의 코인이 필요함
    strategy_start = thresh.index[0] 
        
    # rank 계산
    weekly_rtn_masked_ = weekly_rtn_masked[strategy_start:]
    rank = weekly_rtn_masked_.rank(axis=1, method="first")
    coin_count = rank.count(axis=1)  
    rank_thresh = coin_count.apply(lambda x: [i for i in range(0, x, x//n_group)])
    
    # rank 기반으로 그룹을 나눈다    
    group_mask_dict = {}    
    for i in range(1, n_group+1):
        if i == 1: # 처음
            thresh = rank_thresh.apply(lambda x: x[i])  
            group_rank =  rank.apply(lambda x: x <= thresh, axis=0)
        elif i == n_group: # 마지막
            thresh = rank_thresh.apply(lambda x: x[i-1])
            group_rank = rank.apply(lambda x: thresh < x, axis=0)
        else:
            thresh = rank_thresh.apply(lambda x: x[i])
            thresh_1 = rank_thresh.apply(lambda x: x[i-1]) # 뒤에거를 가져와야함
            group_rank = rank.apply(lambda x: (thresh_1 < x) & (x <= thresh), axis=0)
            
        group_mask = np.where(group_rank == True, weekly_rtn_masked_, np.nan) # 속도 개선을 위해 replace()를 np.where로 수정 (2023-05-15 Edited)
        group_mask = pd.DataFrame(group_mask, index=rank.index, columns=rank.columns)
        group_mask_dict[f"Q{i}"] = group_mask
        
    return group_mask_dict
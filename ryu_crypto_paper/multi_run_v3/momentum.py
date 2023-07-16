import numpy as np
import pandas as pd 


def make_group_mask(price_df:pd.DataFrame, weekly_rtn_df:pd.DataFrame,
                    mask_df:pd.DataFrame, n_group:int, day_of_week:str):
    '''
    그룹의 마스크를 반환합니다

        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
    '''
    last_day = price_df.index[-1]
    
    weekly_mask = mask_df.resample("W-" + day_of_week).last()[:last_day]
    weekly_rtn = weekly_rtn_df.resample("W-" + day_of_week).last()[:last_day]
    weekly_rtn_masked = weekly_rtn * weekly_mask 
        
    # 언제부터 시작하는 지 (최소 q*n개의 코인이 필요)
    cnt = weekly_rtn_masked.count(1)
    thresh = cnt[cnt >= (n_group * 20)] # 여기서 start date가 나온다 / 각 그룹당 최소 20개의 코인이 필요함
    strategy_start = thresh.index[0] 
        
    # rank 계산
    rank = weekly_rtn_masked[strategy_start:].rank(axis=1, method="first")
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
            
        group_mask = np.where(group_rank == True, 1, np.nan) # 속도 개선을 위해 replace()를 np.where로 수정 (2023-05-15 Edited)
        group_mask = pd.DataFrame(group_mask, index=rank.index, columns=rank.columns)
        group_mask_dict[f"Q{i}"] = group_mask
        
    return group_mask_dict  #, cnt


def make_group_jk_mask(daily_rtn_df:pd.DataFrame, 
                       mask_df:pd.DataFrame,
                       n_group:int, day_of_week:str):
    '''
    그룹의 마스크를 반환합니다

        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
    '''
    j=8
    k=2
     
    last_day = daily_rtn_df.index[-1]
    weekly_mask = mask_df.resample("W-" + day_of_week).last()[:last_day]
      
    # Calculate cumulative returns for the j-k day window
    momentum_df = (daily_rtn_df+1).rolling(window=j-k+1).apply(np.prod, raw=True) - 1
    momentum_df = momentum_df.shift(k).resample("W-" + day_of_week).last()[:last_day]
    
    weekly_rtn_masked = momentum_df * weekly_mask
        
    # 언제부터 시작하는 지 (최소 q*n개의 코인이 필요)
    cnt = weekly_rtn_masked.count(1)
    thresh = cnt[cnt >= (n_group * 20)] # 여기서 start date가 나온다 / 각 그룹당 최소 20개의 코인이 필요함
    strategy_start = thresh.index[0] 
        
    # rank 계산
    rank = weekly_rtn_masked[strategy_start:].rank(axis=1, method="first")
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
            
        group_mask = np.where(group_rank == True, 1, np.nan) # 속도 개선을 위해 replace()를 np.where로 수정 (2023-05-15 Edited)
        group_mask = pd.DataFrame(group_mask, index=rank.index, columns=rank.columns)
        group_mask_dict[f"Q{i}"] = group_mask
        
    return group_mask_dict  #, cnt
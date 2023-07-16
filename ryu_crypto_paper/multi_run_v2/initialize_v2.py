import numpy as np
import numpy as np
import pandas as pd 
import ray

#ray.init(num_cpus=16, ignore_reinit_error=True)

########################### 추가사항(2023-05-15) #########################
# 속도 개선 (replace 제거)
# inner_data_pp 추가 -> 코드 효율성을 위해서
# 매주 모멘텀을 계산하고, 진입만 1/7씩 해준다 (Daily 단위로 모멘텀을 계산하지 않는다)
# 5월 17일 : screener 속도 개선 (np.where로)
##########################################################################


def data_pp(data:pd.DataFrame, vender:str):
    # Initialize the Data
    '''
    data : pivot 필요한 데이터,
    vender : [coinmarketcap, binance]
    
    return: dict[price, mktcap, vol]
    '''
       
    if vender == "coinmarketcap":
        v,p,m,col = "vol","close","mktcap","coin_id"
    elif vender == "binance":
        v,p,m,col = "tvol","prc","mcap","permno"
       
    vol_df = pd.pivot_table(data=data,
                         values=v,
                         index=data.index, 
                         columns=col)
    
    price_df = pd.pivot_table(data=data,
                           values=p, 
                           index=data.index, 
                           columns=col).replace({0:np.nan})
    #price_df_tmp = np.where(price_df == 0, np.nan, price_df)
    #price_df = pd.DataFrame(price_df_tmp, index=price_df.index, columns=price_df.columns)
    
    mktcap_df = pd.pivot_table(data=data,
                            values=m, 
                            index=data.index, 
                            columns=col)
    
    return_dict = {"price":price_df,
                   "mktcap": mktcap_df,
                   "vol": vol_df}
    
    return return_dict


def screener(mktcap_df:pd.DataFrame, vol_df:pd.DataFrame, mktcap_thresh, vol_thresh, ma=True): # 비워두면 None이 되나?
    '''
    mktcap_thresh: 최소 마켓켑 (MA30)
    vol_thresh: 최소 거래대금 (MA30)
    ma: MA를 사용해 스크리닝할지(Default: True)
    
    return mask
    '''
    # 마스크를 생성해서 리턴합니다
    # vol만 조건을 거는 경우는 없을 것으로 가정하고 코드 구현
    mktcap_df_ = mktcap_df.copy()
    if ma:
        if  mktcap_thresh != None:
            mktcap_mask = (mktcap_df_.rolling(window=30).mean() >= mktcap_thresh) 
            mktcap_mask_ = np.where(mktcap_mask==True, 1, np.nan)
            mktcap_mask = pd.DataFrame(mktcap_mask_, index=mktcap_mask.index, columns=mktcap_mask.columns)     
                              
            if vol_thresh != None:
                vol_mask = (vol_df.rolling(window=30).mean() >= vol_thresh) 
                vol_mask_ = np.where(vol_mask==True, 1, np.nan)
                vol_mask = pd.DataFrame(vol_mask_, index=vol_mask.index, columns=vol_mask.columns)
                              
                mask = mktcap_mask * vol_mask
            else: # mkt만 조건이 있고, vol은 조건이 없는 경우
                mask = mktcap_mask

        else: # 아무 조건이 안 걸리는 경우 (전부 1로 채운 mask 생성)
            mask = pd.DataFrame(1, index= mktcap_df.index, columns=mktcap_df.columns) 

    elif ma == False:
        if  mktcap_thresh != None:
            mktcap_mask = (mktcap_df_ >= mktcap_thresh) 
            mktcap_mask_ = np.where(mktcap_mask==True, 1, np.nan)
            mktcap_mask = pd.DataFrame(mktcap_mask_, index=mktcap_mask.index, columns=mktcap_mask.columns)   
                                             
            if vol_thresh != None:
                vol_mask = (vol_df >= vol_thresh) 
                vol_mask_ = np.where(vol_mask==True, 1, np.nan)
                vol_mask = pd.DataFrame(vol_mask_, index=vol_mask.index, columns=vol_mask.columns)
                    
                mask = mktcap_mask * vol_mask
            else: # mkt만 조건이 있고, vol은 조건이 없는 경우
                mask = mktcap_mask
            
        else: # 아무 조건이 안 걸리는 경우 (전부 1로 채운 mask 생성)
            mask = pd.DataFrame(1, index= mktcap_df.index, columns=mktcap_df.columns)     
    
    return mask # 여기까진 맞다


def inner_data_pp(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame, n_group:int,
                  day_of_week:str, number_of_coin_group:int, mktcap_thresh:int, vol_thresh:int, look_back:int):
    '''
    Signal DataFrame을 생성하기 직전 필요한 데이터를 생성합니다
    
    Return : Tuple  -> [daily_rtn_df, weekly_mktcap, group_mask_dict]
    '''
    daily_rtn_df = price_df.pct_change(fill_method=None)
    #group_coin_count = {}
           
    group_mask_dict, mask = make_weekly_momentum_signal(price_df, mktcap_df, vol_df, n_group, day_of_week, 
                                                number_of_coin_group, mktcap_thresh, vol_thresh, look_back=look_back) # 함수의 args를 바로 넘겨준다 / Binary Signal이 담긴 DataFrame을 리턴한다

    real_idx = [idx for idx in group_mask_dict["Q1"].index if idx in mktcap_df.index] # Q1은 무조건 존재하니까
    
    try:
        mktcap_used = mktcap_df.loc[real_idx] * mask.loc[real_idx]
    except:
        mktcap_used = mktcap_df.loc[real_idx[:-1]] * mask.loc[real_idx[:-1]] # Index 이슈가 있어서 추가했음(2023-05-15 Edited)

    return daily_rtn_df, mktcap_used, group_mask_dict, mask


def make_weekly_momentum_signal(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame, n_group:int, day_of_week:str,
                              number_of_coin_group:int, mktcap_thresh=None, vol_thresh=None, ma=True, look_back=14):
    '''
    횡단면 Weekly Momentum을 기준으로 그룹을 나눈 후, 그룹의 마스크를 반환합니다

        mktcap_df : pd.DataFrame
        vol_df : pd.DataFrame
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : 최소 마켓켑 (MA30)
        vol_thresh : 최소 거래대금 (MA30)
        ma: screen을 할 때, MA를 쓸지 안쓸지 (True or False)
        look_back: Default 7
    '''
    mktcap_df_ = mktcap_df.copy()
    mask = screener(mktcap_df_, vol_df, mktcap_thresh, vol_thresh, ma=ma) # Daily 마스크를 받는다
    
    mask_used = mask.resample("W-"+day_of_week).last()
    weekly_rtn = price_df.pct_change(look_back, fill_method=None).resample("W-"+day_of_week).last()
    weekly_rtn_masked = weekly_rtn * mask_used  # 그룹을 나눌때 사용함 (mktcap, vol 스크리닝 통과한 코인들만의 리턴이 담겨있다)
        
    # 언제부터 시작하는 지 (최소 q*n개의 코인이 필요)
    cnt = weekly_rtn_masked.count(1) # 이걸 리턴
    more_thresh = cnt.loc[cnt >= (n_group * number_of_coin_group)] # 여기서 start date가 나온다
    strategy_start = more_thresh.index[0] 
        
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
        
    return group_mask_dict, mask#, cnt


def make_market_index(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame, mktcap_thresh=None, vol_thresh=None, ma=True):
    '''
    return value weighted market index(Series)

        mktcap_thresh : mktcap_value 이하는 스크리닝 (MA30)
        vol_thresh : vol_value 이하는 스크리닝 (MA30)
        ma : 스크리닝을 MA로 할지 여부 (True/False)
        
    -> Return : 마켓 수익률_df, 코인 개수_df 
    ''' 
    rtn_df = price_df.pct_change(fill_method=None)
    
    # vol만 조건을 거는 경우는 없을 것으로 가정하고 코드 구현
    mask = screener(mktcap_df, vol_df, mktcap_thresh, vol_thresh, ma)
    mktcap_screened = mktcap_df * mask
    
    # mktcap, vol 스크리닝을 했기 때문에, 코인이 1개라도 포함되는 시작일을 찾아야 한다(mkt index 계산을 위해서)
    nan_counts = mktcap_screened.isnull().sum(1)
    start_idx = nan_counts[nan_counts < mktcap_screened.shape[1]].index[0] # Class에 비해 개선
                                
    weight = mktcap_screened.loc[start_idx:].apply(lambda x: x / np.nansum(x), axis=1)
    mkt_rtn = rtn_df.loc[start_idx:] * weight.shift(1)
    #time_series_coin_num = mkt_rtn.count(axis=1)
    mkt_index = mkt_rtn.sum(1) 
    
    return mkt_index#, time_series_coin_num # 수익이 담긴 pd.Series
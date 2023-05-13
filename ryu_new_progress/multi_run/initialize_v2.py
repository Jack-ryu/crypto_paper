import numpy as np
import numpy as np
import pandas as pd 
import ray

ray.init(num_cpus=16, ignore_reinit_error=True)


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
    mktcap_df = pd.pivot_table(data=data,
                            values=m, 
                            index=data.index, 
                            columns=col)
    return_dict = {"price":price_df,
                   "mktcap": mktcap_df,
                   "vol": vol_df}
    
    return return_dict


def screener(mktcap_df:pd.DataFrame, vol_df:pd.DataFrame, mktcap_thresh, vol_thresh): # 비워두면 None이 되나?
    '''
    mktcap_thresh: 최소 마켓켑 (MA30)
    vol_thresh: 최소 거래대금 (MA30)
    
    return mask
    '''
    # 마스크를 생성해서 리턴합니다
    # vol만 조건을 거는 경우는 없을 것으로 가정하고 코드 구현
    mktcap_df_ = mktcap_df.copy()
    if  mktcap_thresh != None:
        mktcap_mask = (mktcap_df_.rolling(window=30).mean() > mktcap_thresh) \
                                .replace({True:1, False:np.nan})   
        if vol_thresh != None:
            vol_mask = (vol_df.rolling(window=30).mean() > vol_thresh) \
                              .replace({True:1, False:np.nan})     
            mask = mktcap_mask * vol_mask
        else: # mkt만 조건이 있고, vol은 조건이 없는 경우
            mask = mktcap_mask
        
    else: # 아무 조건이 안 걸리는 경우 (전부 1로 채운 mask 생성)
        mask = pd.DataFrame(1, index= mktcap_df.index, columns=mktcap_df.columns) 
    
    return mask  # 여기까진 맞다
    

def make_weekly_momentum_mask(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame, n_group:int, day_of_week:str,
                              number_of_coin_group:int, mktcap_thresh=None, vol_thresh=None):
    '''
    횡단면 Weekly Momentum을 기준으로 그룹을 나눈 후, 그룹의 마스크를 반환합니다

        mktcap_df : pd.DataFrame
        vol_df : pd.DataFrame
        n_group : 몇 개의 그룹으로 나눌 지
        day_of_week : Rebalancing을 진행할 요일 [MON,TUE,WED,THU,FRI,SAT,SUN]
        number_of_coin_group : 그룹당 최소 필요한 코인 수
        mktcap_thresh : 최소 마켓켑 (MA30)
        vol_thresh : 최소 거래대금 (MA30)
    '''
    mktcap_df_ = mktcap_df.copy()
    mask = screener(mktcap_df_,vol_df, mktcap_thresh, vol_thresh) # Daily 마스크를 받는다
    weekly_mask = mask.resample("W-"+day_of_week).last()
        
    # 주간 데이터를 생성합니다
    weekly_rtn = price_df.pct_change(7,fill_method=None).resample("W-"+day_of_week).last()
    weekly_rtn_masked = weekly_rtn * weekly_mask  # 그룹을 나눌때 사용함 
        
    # 언제부터 시작하는 지 (최소 q*n개의 코인이 필요)
    cnt = weekly_rtn_masked.count(1) # 이걸 리턴
    more_thresh = cnt.loc[cnt >= (n_group * number_of_coin_group)] # 여기서 start date가 나온다
    strategy_start = more_thresh.index[0] 
        
    # rank 계산
    rank = weekly_rtn_masked[strategy_start:].rank(axis=1, method="first")
    coin_count = rank.count(axis=1)  
    rank_thresh = coin_count.apply(lambda x: [i for i in range(0,x, x//n_group)])
    
    # rank 기반으로 그룹을 나눈다    
    group_mask_dict = {}    
    for i in range(1, n_group+1):
        if i == 1: # 처음
            thresh = rank_thresh.apply(lambda x: x[i])
            group_mask = rank.apply(lambda x: x <= thresh, axis=0).replace({True:1, False:np.nan})
        elif i == n_group: # 마지막
            thresh = rank_thresh.apply(lambda x: x[i-1])
            group_mask = rank.apply(lambda x: thresh < x, axis=0).replace({True:1, False:np.nan})
        else:
            thresh = rank_thresh.apply(lambda x: x[i])
            thresh_1 = rank_thresh.apply(lambda x: x[i-1]) # 뒤에거를 가져와야함
            group_mask = rank.apply(lambda x: (thresh_1 < x) & (x <= thresh), axis=0).replace({True:1, False:np.nan})
        
        group_mask_dict[f"Q{i}"] = group_mask
        
    return group_mask_dict#, cnt


@ray.remote
def make_market_index(price_df:pd.DataFrame, mktcap_df:pd.DataFrame, vol_df:pd.DataFrame, mktcap_thresh=None, vol_thresh=None):
    '''
    return value weighted market index(Series)

        mktcap_thresh : mktcap_value 이하는 스크리닝 (MA30)
        vol_thresh : vol_value 이하는 스크리닝 (MA30)
        
    -> Return : 마켓 수익률_df, 코인 개수_df 
    ''' 
    rtn_df = price_df.pct_change(fill_method=None)
    
    # vol만 조건을 거는 경우는 없을 것으로 가정하고 코드 구현
    mask = screener(mktcap_df, vol_df, mktcap_thresh, vol_thresh)
    mktcap_screened = mktcap_df * mask
    
    # mktcap, vol 스크리닝을 했기 때문에, 코인이 1개라도 포함되는 시작일을 찾아야 한다(mkt index 계산을 위해서)
    nan_counts = mktcap_screened.isnull().sum(1)
    start_idx = nan_counts[nan_counts < mktcap_screened.shape[1]].index[0] # Class에 비해 개선
                                
    weight = mktcap_screened.loc[start_idx:].apply(lambda x: x / np.nansum(x), axis=1)
    mkt_rtn = rtn_df.loc[start_idx:] * weight.shift(1)
    time_series_coin_num = mkt_rtn.count(axis=1)
    mkt_index = mkt_rtn.sum(1) 
    
    return mkt_index#, time_series_coin_num # 수익이 담긴 pd.Series
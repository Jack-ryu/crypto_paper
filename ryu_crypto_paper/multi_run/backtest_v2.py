import numpy as np 
import pandas as pd 

#############################v2 수정사항###############################
# 벡테스팅 로직 수정(weight 벡터를 업데이트 하지 않고, 달러 벨류로만 계산) -> 코드 점검 완료
######################################################################

  
def simulate_strategy(group_weight_df:pd.DataFrame, daily_rtn_df:pd.DataFrame, fee_rate:float):
    '''
    전략의 수익을 평가합니다(Long-Only Portfolio) / Daily Rebalancing도 가능함(Rebalancing은 Depand on DataFrame's Index)
    '''
    
    pf_value = 1
    pf_dict = {}
    
    weight = group_weight_df.iloc[0] # 시작 weight를 지정해준다 (첫 weight에서 투자 시작, 장마감 직전에 포트폴리오 구성)
    dollar_value = weight * pf_value # Start Dollar Value를 지정
    
    rebalancing_idx = group_weight_df.index
    start_idx = rebalancing_idx[0]
    
    for idx, row in daily_rtn_df.loc[start_idx:].iloc[1:].iterrows(): #Daily로 반복 / 시작 weight 구성 다음 날부터 성과를 평가
        # 수익률 평가가 리밸런싱보다 선행해야함
        dollar_value = dollar_value * (1+np.nan_to_num(row)) # update the dollar value
        pf_value = np.nansum(dollar_value) # update the pf value
        
        if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
            weight = group_weight_df.loc[idx] # Weight Rebalancing
            target_dollar_value = np.nan_to_num(pf_value * weight) * (1 - fee_rate)
            dollar_fee = np.nansum(np.abs(target_dollar_value - np.nan_to_num(dollar_value)) * fee_rate)
            pf_value = pf_value - dollar_fee # fee 차감
            dollar_value = weight * pf_value  # dollar value를 Rebalancing 이후로 update
            
        pf_dict[idx] = pf_value
        
    # 결과를 pct로 정렬
    pf_result = pd.Series(pf_dict)
    idx = pf_result.index[0] - pd.Timedelta(days=1)
    pf_result[idx] = 1
    pf_result.sort_index(inplace=True)
    pf_result = pf_result.pct_change().fillna(0)
    
    return pf_result


def simulate_strategy_long_short(long_weight_df:pd.DataFrame, short_weight_df:pd.DataFrame, daily_rtn_df:pd.DataFrame, fee_rate:float):
    '''
    전략의 수익을 평가합니다 (long-short Portfolio)
    '''
    pf_value = 1
    pf_dict = {}
    
    # 시작 weight를 지정해준다(첫 weight에서 투자 시작, 장마감 직전에 포트폴리오 구성)
    short_weight = short_weight_df.iloc[0]
    long_weight = long_weight_df.iloc[0]  
    
    # 최초 Dollar Value 할당
    dollar_value_of_sell = short_weight * pf_value
    dollar_value_of_buy = long_weight * pf_value # dollar_value_of_sell - dollar_value_of_buy=0 성립
    
    rebalancing_idx = long_weight_df.index
    start_idx = rebalancing_idx[0]
    
    for idx, rtn in daily_rtn_df.loc[start_idx:].iloc[1:].iterrows(): # Daily로 반복, 첫 weight 구성 다음 날부터 성과를 평가
        # 수익률 평가가 리밸런싱보다 선행해야함
        short_rtn = 1 + np.nan_to_num(-rtn) 
        long_rtn  = 1 + np.nan_to_num(rtn) 
    
        # Update each dollar value
        dollar_value_of_sell_update = dollar_value_of_sell * short_rtn
        dollar_value_of_sell_update = np.where(dollar_value_of_sell_update > 0, dollar_value_of_sell_update, 0) # pf 가치가 음수로 안내려가도록 청산 시 0이 된다
        dollar_value_of_buy_update = dollar_value_of_buy * long_rtn
        
        # Update the portfolio value of every days
        dollar_gain_sell = np.nansum(dollar_value_of_sell_update - dollar_value_of_sell)
        dollar_gain_buy = np.nansum(dollar_value_of_buy_update - dollar_value_of_buy)
        pf_value = pf_value + dollar_gain_buy + dollar_gain_sell
            
        # 변수 다시 원래대로
        dollar_value_of_sell = dollar_value_of_sell_update
        dollar_value_of_buy = dollar_value_of_buy_update
        
        if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
            long_weight = long_weight_df.loc[idx]   # target weight (리벨런싱할 weight)
            short_weight = short_weight_df.loc[idx]
            
            target_dollar_value_of_sell = short_weight * pf_value * (1 - fee_rate) # Cash inflow
            target_dollar_value_of_buy  = long_weight  * pf_value * (1 - fee_rate)   # Cash outflow  : dollar_value_of_sell+dollar_value_of_buy=0 성립
            
            dv_delta_sell = np.abs(np.nan_to_num(target_dollar_value_of_sell) - np.nan_to_num(dollar_value_of_sell))
            dv_delta_buy = np.abs(np.nan_to_num(target_dollar_value_of_buy) - np.nan_to_num(dollar_value_of_buy))
            
            fee = (np.nansum(dv_delta_sell) + np.nansum(dv_delta_buy)) * fee_rate
            pf_value = pf_value - fee # fee 차감
            
            dollar_value_of_buy  = long_weight  * pf_value  # dollar value를 Rebalancing 이후로 update
            dollar_value_of_sell = short_weight * pf_value
            
        pf_dict[idx] = pf_value
            
    # 결과를 pct로 정렬
    pf_result = pd.Series(pf_dict)
    idx = pf_result.index[0] - pd.Timedelta(days=1)
    pf_result[idx] = 1
    pf_result.sort_index(inplace=True)
    pf_result = pf_result.pct_change().fillna(0)
    
    return pf_result

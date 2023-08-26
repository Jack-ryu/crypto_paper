import numpy as np 
import pandas as pd 

############################################################
# 레버리니
######################################################################

  
def simulate_longonly(group_weight_df:pd.DataFrame, daily_rtn_df:pd.DataFrame, fee_rate:float):
    '''
    전략의 수익을 평가합니다(Long-Only Portfolio) / Daily Rebalancing도 가능함(Rebalancing은 Depand on DataFrame's Index)
    '''
    
    pf_value = 1
    pf_dict = {}
    
    weight = group_weight_df.iloc[0] # 시작 weight를 지정해준다 (첫 weight에서 투자 시작, 장마감 직전에 포트폴리오 구성)
    dollar_value = weight * pf_value # Start Dollar Value를 지정
    
    rebalancing_idx = group_weight_df.index
    start_idx = rebalancing_idx[0]
    
    for idx, row in daily_rtn_df.loc[start_idx:].iloc[1:].iterrows(): # Daily로 반복 / 시작 weight 구성 다음 날부터 성과를 평가
        # 수익률 평가가 리밸런싱보다 선행해야함
        dollar_value_update = dollar_value * (1+np.nan_to_num(row)) # update the dollar value
        dollar_value_gain = dollar_value_update - dollar_value
        pf_value = pf_value + np.nansum(dollar_value_gain) # update the pf value
        
        # 달러벨류 원래대로
        dollar_value = dollar_value_update
        
        if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
            weight = group_weight_df.loc[idx] # Weight Rebalancing
            target_dollar_value = np.nan_to_num(pf_value * weight) * (1 - fee_rate)
            dollar_fee = np.nansum(np.abs(target_dollar_value - np.nan_to_num(dollar_value)) * fee_rate)
            pf_value = pf_value - dollar_fee # fee 차감
            dollar_value = weight * pf_value  # dollar value를 Rebalancing 이후로 update
            
        pf_dict[idx] = pf_value   
        
    # 결과를 pct로 정렬
    pf_result = pd.Series(pf_dict)    
    return pf_result


def simulate_longshort(long_weight_df:pd.DataFrame, short_weight_df:pd.DataFrame, daily_rtn_df:pd.DataFrame, fee_rate:float, margin:str):
    '''
    전략의 수익을 평가합니다 (long-short Portfolio)
    margin (str) : ["isolate","cross"]

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
    
    # 2023-06-18 추가 (weight변화를 기록할 데이터프레임)
    dv_df = pd.DataFrame(index=daily_rtn_df.loc[start_idx:].index,
                         columns=long_weight_df.columns)
    dv_df.loc[start_idx] = dollar_value_of_sell
    
    if margin == "isolate":
        for idx, rtn in daily_rtn_df.loc[start_idx:].iloc[1:].iterrows(): # Daily로 반복, 첫 weight 구성 다음 날부터 성과를 평가
            # 수익률 평가가 리밸런싱보다 선행해야함
            short_rtn = 1 + np.nan_to_num(-rtn) 
            long_rtn  = 1 + np.nan_to_num(rtn) 

            # Update each dollar value
            dollar_value_of_sell_update = dollar_value_of_sell * short_rtn 
            dollar_value_of_sell_update = np.where(dollar_value_of_sell_update > 0, dollar_value_of_sell_update, 0) # 개별 코인의 가치는 최소 0이 된다(청산)
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
            
    elif margin == "cross":
        for idx, rtn in daily_rtn_df.loc[start_idx:].iloc[1:].iterrows(): # Daily로 반복, 첫 weight 구성 다음 날부터 성과를 평가
            # 수익률 평가가 리밸런싱보다 선행해야함
            short_rtn = 1 + np.nan_to_num(-rtn) 
            long_rtn  = 1 + np.nan_to_num(rtn) 
            
            # Update each dollar value -> 여기서 개별 코인의 가치가 음수가 되는 일이 생긴다. 그러면 리턴을 곱해주면 음수는 음수가 더 커지는 현상이 발생한다
            # Short에서 DV 양수 리턴 곱해서 그대로 리턴 
            postive_dv = np.where(dollar_value_of_sell >= 0, dollar_value_of_sell, np.nan) # DV가 양수인 애들
            postive_dv = (postive_dv * short_rtn)
            
            # short에서 DV가 음수 
            negative_dv_mask = np.where(dollar_value_of_sell < 0, 1, np.nan) # DV가 음수인 애들
            
            positive_rtn = np.where((short_rtn*negative_dv_mask) >=0, short_rtn, np.nan) # DV 음수 중, 리턴이 양수
            delta = (dollar_value_of_sell - (dollar_value_of_sell * positive_rtn))
            negative_dv_positive_rtn = (dollar_value_of_sell + delta)
            
            negative_rtn = np.where((short_rtn*negative_dv_mask) < 0, short_rtn, np.nan)  # DV 음수 중, 리턴이 음수
            negative_dv_negative_rtn = -(dollar_value_of_sell * negative_rtn)
            
            # 결과
            dollar_value_of_sell_update = np.nan_to_num(postive_dv) + np.nan_to_num(negative_dv_positive_rtn) + np.nan_to_num(negative_dv_negative_rtn)

            # Long은 그냥 업데이트 해주면 된다
            dollar_value_of_buy_update = dollar_value_of_buy * long_rtn 

            # Update the portfolio value of every days
            dollar_gain_sell = np.nansum(dollar_value_of_sell_update - dollar_value_of_sell)
            dollar_gain_buy = np.nansum(dollar_value_of_buy_update - dollar_value_of_buy)
            pf_value = pf_value + dollar_gain_buy + dollar_gain_sell
            
            # 완전히 청산(pf_value 전체가 0이 되는 경우) 끝날떄까지 pf가치는 계속 0이 된다
            if pf_value <= 0:
                pf_value = 0
                dollar_value_of_sell_update = 0
                dollar_value_of_buy_update = 0
            
            # 변수 다시 원래대로
            dollar_value_of_sell = dollar_value_of_sell_update
            dollar_value_of_buy = dollar_value_of_buy_update

            if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
                long_weight = long_weight_df.loc[idx]   # target weight (리벨런싱할 weight)
                short_weight = short_weight_df.loc[idx]

                target_dollar_value_of_sell = short_weight * pf_value * (1 - fee_rate) # Cash inflow
                target_dollar_value_of_sell = np.where(target_dollar_value_of_sell < 0, 0, target_dollar_value_of_sell) # 2023-07-15 update -> dv가 음수인 애들은 거래비용을 차감하지 않는다
                target_dollar_value_of_buy  = long_weight  * pf_value * (1 - fee_rate)   # Cash outflow  : dollar_value_of_sell+dollar_value_of_buy=0 성립

                dv_delta_sell = np.abs(np.nan_to_num(target_dollar_value_of_sell) - np.nan_to_num(dollar_value_of_sell))
                dv_delta_buy = np.abs(np.nan_to_num(target_dollar_value_of_buy) - np.nan_to_num(dollar_value_of_buy))

                fee = (np.nansum(dv_delta_sell) + np.nansum(dv_delta_buy)) * fee_rate
                pf_value = pf_value - fee # fee 차감

                dollar_value_of_buy  = long_weight  * pf_value  # dollar value를 Rebalancing 이후로 update
                dollar_value_of_sell = short_weight * pf_value

            pf_dict[idx] = pf_value       
            # 2023-06-18추가
            dv_df.loc[idx] = dollar_value_of_sell       
                
    # 결과를 pct로 정렬
    pf_result = pd.Series(pf_dict)

    return pf_result
import numpy as np 
import pandas as pd

### Daily로 1/7씩 진입하는 경우의 Backtesting 

def simulate_longonly_d(group_weight_df:pd.DataFrame, daily_rtn_df:pd.DataFrame, fee_rate:float):
    '''
    전략의 수익을 평가합니다(Long-Only Portfolio)
    
    Daily로 1/7씩 진입
    '''
    
    start_idx = group_weight_df["Q1"].index[0]    
    
    # Initial value setting
    pf_dict = {}
    cash = 1
    
    # 투자 첫날 진입 
    initial_weight = (group_weight_df.iloc[0]/7) # 시작 weight를 지정해준다 (첫 weight에서 투자 시작, 장마감 직전에 포트폴리오 구성) 
    assets_value_ar = initial_weight * 1     # 첫날 1/7만큼 진입 Dollar Value를 지정 (Assets의 DV)

    fee = assets_value_ar * fee_rate
    assets_value_ar = assets_value_ar - fee # fee를 고려해서 asset_value_ar을 변경 -> 첫 일주일동안 이만큼 매일 매수한다
    asset_value = np.nansum(assets_value_ar)
    
    cash -= (1/7)
    pf_dict[start_idx] = asset_value + (cash) # 포트폴리오 값 저장
    
    first_week_start = start_idx + pd.Timedelta(days=1)
    first_week_end = start_idx + pd.Timedelta(days=6)
    
    # 투자시작 첫 7일: 1/7 달러만큼 매일 포지션을 진입한다 
    for idx, row in daily_rtn_df.loc[first_week_start:first_week_end].iterrows(): # Daily로 반복 / 시작 weight 구성 다음 날부터 성과를 평가
        assets_value_ar = assets_value_ar * (1+np.nan_to_num(row)) # update the 보유중인 assets value (Cash는 그대로)
        
        today_weight = group_weight_df.loc[idx] 
        new_assets_value_ar = np.nan_to_num(today_weight * (1/7) ) # 포트폴리오에 신규 추가할 자산의 dollar value
        cash -= (1/7)
    
        fee = np.abs(new_assets_value_ar - np.nan_to_num(assets_value_ar)) * fee_rate # fee 계산
        assets_value_ar = (np.nan_to_num(assets_value_ar) + new_assets_value_ar - fee)
        
        asset_value = np.nansum(assets_value_ar)
        pf_dict[idx] = asset_value + cash # Save Portfolio Value
        
    # 7일이 지나면, 현금을 전부 소진했다. asset_value = 1이 된다 -> 이제 코인만 가지고 수익을 평가하면 된다
    for idx, row in daily_rtn_df.loc[first_week_end:].iterrows():
        assets_value_ar = np.nan_to_num(assets_value_ar * (1+np.nan_to_num(row))) # update the assets_value
        asset_value = np.nansum(assets_value_ar)
        
        today_weight = group_weight_df.loc[idx]
        target_assets_value = today_weight * asset_value
        delta_assets_value = (np.nan_to_num(target_assets_value) - assets_value_ar)  * (1/7) # 변화해야하는 값어치의 1/7만 변화시킨다
        fee = np.abs(delta_assets_value) * fee_rate
        assets_value_ar = assets_value_ar + delta_assets_value - fee # Rebalancing 
        
        asset_value = np.nansum(assets_value_ar)
        pf_dict[idx] = asset_value # Save Portfolio Value
        
    # 결과를 pct로 정렬
    pf_result = pd.Series(pf_dict)
    idx = pf_result.index[0] - pd.Timedelta(days=1)
    pf_result[idx] = 1
    pf_result.sort_index(inplace=True)
    pf_result = pf_result.pct_change().fillna(0)
    
    return pf_result


def simulate_longshort_d(long_weight_df:pd.DataFrame, short_weight_df:pd.DataFrame, daily_rtn_df:pd.DataFrame, fee_rate:float, margin:str):
    '''
    전략의 수익을 평가합니다 (long-short Portfolio)
    margin (str) : ["isolate","cross"]
    
    
    Daily로 1/7씩 진입
    '''
    start_idx = long_weight_df.index[0]        
    
    # Initial value setting   
    pf_value = 1
    pf_dict = {}
    cash = 6/7
    
    # 시작 weight를 지정해준다(첫 weight에서 투자 시작, 장마감 직전에 포트폴리오 구성)
    short_weight = short_weight_df.iloc[0]
    long_weight = long_weight_df.iloc[0]  
    
    # 최초 Dollar Value 할당 / 투자 첫날
    dollar_value_of_sell = (short_weight * 0.5/7) 
    dollar_value_of_buy = (long_weight * 0.5/7)  # dollar_value_of_sell - dollar_value_of_buy=0 성립
    
    dollar_value_of_sell = dollar_value_of_sell * (1-fee_rate) # fee 차감
    dollar_value_of_buy = dollar_value_of_buy * (1-fee_rate) 
    
    first_week_start = start_idx + pd.Timedelta(days=1)
    first_week_end = start_idx + pd.Timedelta(days=6)
    
    # 첫 6일간 1/7씩 계속 진입해야한다
    for idx, row in daily_rtn_df.loc[first_week_start:first_week_end].iterrows(): # Daily로 반복 / 시작 weight 구성 다음 날부터 성과를 평가
        dollar_value_of_sell_update = dollar_value_of_sell * (1 + np.nan_to_num(-row)) # position 잡고 있는 애들은 가격 변화를 기록
        dollar_value_of_buy_update  = dollar_value_of_buy * (1 + np.nan_to_num(row)) 
        
        dollar_gain_sell = dollar_value_of_sell_update - dollar_value_of_sell
        dollar_gain_buy = dollar_value_of_buy_update - dollar_value_of_buy
        
        pf_value = np.nansum(dollar_value_of_buy_update) + np.nansum(dollar_value_of_sell_update) + cash # pf_Value 변화 기록
        print(pf_value)
        
        today_short_weight = short_weight_df.loc[idx] 
        today_long_weight = long_weight_df.loc[idx]
        
        new_dollar_value_of_sell = np.nan_to_num(today_short_weight * 0.5/7) * (1-fee_rate) # 신규 진입
        new_dollar_value_of_buy =  np.nan_to_num(today_long_weight * 0.5/7) * (1-fee_rate)
        
        dollar_value_of_sell = new_dollar_value_of_sell + np.nan_to_num(dollar_value_of_sell) # 신규 진입 반영
        dollar_value_of_buy = new_dollar_value_of_buy + np.nan_to_num(dollar_value_of_buy)
        
        
        cash -= 1/7 
        
        # 여기까지하면 최초 1/7씩 진입이 끝난다
        
    #if margin == "isolate":
    #    for idx, rtn in daily_rtn_df.loc[start_idx:].iloc[1:].iterrows(): # Daily로 반복, 첫 weight 구성 다음 날부터 성과를 평가
    #        # 수익률 평가가 리밸런싱보다 선행해야함
    #        short_rtn = 1 + np.nan_to_num(-rtn) 
    #        long_rtn  = 1 + np.nan_to_num(rtn) 
#
    #        # Update each dollar value
    #        dollar_value_of_sell_update = dollar_value_of_sell * short_rtn
    #        dollar_value_of_sell_update = np.where(dollar_value_of_sell_update > 0, dollar_value_of_sell_update, 0) # 개별 코인의 가치는 최소 0이 된다(청산)
    #        dollar_value_of_buy_update = dollar_value_of_buy * long_rtn
#
    #        # Update the portfolio value of every days
    #        dollar_gain_sell = np.nansum(dollar_value_of_sell_update - dollar_value_of_sell)
    #        dollar_gain_buy = np.nansum(dollar_value_of_buy_update - dollar_value_of_buy)
    #        pf_value = pf_value + dollar_gain_buy + dollar_gain_sell # 롱숏에서 발생한 달러가치의 변화분만 포트폴리오 가치에 더해준다
#
    #        # 변수 다시 원래대로
    #        dollar_value_of_sell = dollar_value_of_sell_update
    #        dollar_value_of_buy = dollar_value_of_buy_update
#
    #        if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
    #            long_weight = long_weight_df.loc[idx]   # target weight (리벨런싱할 weight)
    #            short_weight = short_weight_df.loc[idx]
#
    #            target_dollar_value_of_sell = short_weight * pf_value * (1 - fee_rate) # Cash inflow
    #            target_dollar_value_of_buy  = long_weight  * pf_value * (1 - fee_rate)   # Cash outflow  : dollar_value_of_sell+dollar_value_of_buy=0 성립
#
    #            dv_delta_sell = np.abs(np.nan_to_num(target_dollar_value_of_sell) - np.nan_to_num(dollar_value_of_sell))
    #            dv_delta_buy = np.abs(np.nan_to_num(target_dollar_value_of_buy) - np.nan_to_num(dollar_value_of_buy))
#
    #            fee = (np.nansum(dv_delta_sell) + np.nansum(dv_delta_buy)) * fee_rate
    #            pf_value = pf_value - fee # fee 차감
#
    #            dollar_value_of_buy  = long_weight  * pf_value  # dollar value를 Rebalancing 이후로 update
    #            dollar_value_of_sell = short_weight * pf_value
#
    #        pf_dict[idx] = pf_value
    #    
    #elif margin == "cross":
    #    for idx, rtn in daily_rtn_df.loc[start_idx:].iloc[1:].iterrows(): # Daily로 반복, 첫 weight 구성 다음 날부터 성과를 평가
    #        # 수익률 평가가 리밸런싱보다 선행해야함
    #        short_rtn = 1 + np.nan_to_num(-rtn) 
    #        long_rtn  = 1 + np.nan_to_num(rtn) 
#
    #        # Update each dollar value
    #        dollar_value_of_sell_update = dollar_value_of_sell * short_rtn
    #        #dollar_value_of_sell_update = np.where(dollar_value_of_sell_update > 0, dollar_value_of_sell_update, 0) # 개별 코인의 DV는 음수가 될 수 있다 (Cross-Margin)
    #        dollar_value_of_buy_update = dollar_value_of_buy * long_rtn
#
    #        # Update the portfolio value of every days
    #        dollar_gain_sell = np.nansum(dollar_value_of_sell_update - dollar_value_of_sell)
    #        dollar_gain_buy = np.nansum(dollar_value_of_buy_update - dollar_value_of_buy)
    #        pf_value = pf_value + dollar_gain_buy + dollar_gain_sell
    #        
    #        # 완전히 청산(pf_value 전체가 0이 되는 경우) 끝날떄까지 pf가치는 계속 0이 된다
    #        if pf_value <= 0:
    #            pf_value = 0
    #            dollar_value_of_sell_update = 0
    #            dollar_value_of_buy_update = 0
    #        
    #        # 변수 다시 원래대로
    #        dollar_value_of_sell = dollar_value_of_sell_update
    #        dollar_value_of_buy = dollar_value_of_buy_update
#
    #        if idx in rebalancing_idx: # Rebalancing Date (장마감 직전에 리벨런싱 실시)
    #            long_weight = long_weight_df.loc[idx]   # target weight (리벨런싱할 weight)
    #            short_weight = short_weight_df.loc[idx]
#
    #            target_dollar_value_of_sell = short_weight * pf_value * (1 - fee_rate) # Cash inflow
    #            target_dollar_value_of_buy  = long_weight  * pf_value * (1 - fee_rate)   # Cash outflow  : dollar_value_of_sell+dollar_value_of_buy=0 성립
#
    #            dv_delta_sell = np.abs(np.nan_to_num(target_dollar_value_of_sell) - np.nan_to_num(dollar_value_of_sell))
    #            dv_delta_buy = np.abs(np.nan_to_num(target_dollar_value_of_buy) - np.nan_to_num(dollar_value_of_buy))
#
    #            fee = (np.nansum(dv_delta_sell) + np.nansum(dv_delta_buy)) * fee_rate
    #            pf_value = pf_value - fee # fee 차감
#
    #            dollar_value_of_buy  = long_weight  * pf_value  # dollar value를 Rebalancing 이후로 update
    #            dollar_value_of_sell = short_weight * pf_value
#
    #        pf_dict[idx] = pf_value       
    #        
    ## 결과를 pct로 정렬
    #pf_result = pd.Series(pf_dict)
    #idx = pf_result.index[0] - pd.Timedelta(days=1)
    #pf_result[idx] = 1
    #pf_result.sort_index(inplace=True)
    #pf_result = pf_result.pct_change().fillna(0)
    #
    #return pf_result
    
    
    
    
#def simulate_longshort_d(long_weight_df:pd.DataFrame, short_weight_df:pd.DataFrame, price_df:pd.DataFrame, fee_rate:float, margin:str):
#    '''가격 기반으로, MTS 시스템과 일치하게 동작하도록 수정한다
#    
#       공매도하고 Cash를 받고, 받은만큼 주식을 산다
#    '''
#    
#    cash = 0
#    asset_value = 0
#    pf_value = cash + asset_value
#      
#    # 첫 일주일
#    for date in long_weight_df.index:
#        today_price = price_df.loc[date]
#        today_short_weight = short_weight_df.loc[date]
#        today_long_weight = long_weight_df.loc[date]
#        
#        short_coin_num = (today_short_weight * (1/7) / today_price) # 오늘 코인을 몇 개 사야하는 지 구한다(매일 1/7씩 진입)
#        long_coin_num  = (today_long_weight * (1/7)) / today_price
#        
#        # 공매도하고, 그만큼 현금을 받는다 
#        asset_value_array_short = short_coin_num * today_price
#        cash += np.nansum(asset_value_array_short)
#        asset_value_array_long = long_coin_num * cash # 현금만큼 Long을 한다 
#        cash -= np.nansum(asset_value_array_long) # 코인을 구입 했기 때문에 현금을 차금한다
#        
#        print(date,cash)
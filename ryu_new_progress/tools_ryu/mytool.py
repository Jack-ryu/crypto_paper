import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
from statsmodels.api import OLS, add_constant

def calculate_cagr(return_df):
    '''rtn을 받았을 때, CAGR을 계산합니다'''
    holding_year = (len(return_df) / 365)
    cum = (return_df+1).cumprod()
    cagr = (cum.iloc[-1] / cum.iloc[0]) ** (1/holding_year) -1
    return str(cagr.round(6)*100) + "%"

def run_alpha_regression(return_dict:dict, 
                         mkt_rtn:pd.Series,
                         constant=True):
    
    '''return_dict : dict(포트폴리오 리턴)
       mkt_rtn : pd.Series(마켓 인덱스의 리턴이 들어감)
       constant : True(Default)/ False
       
       Note) 투자 시작일(첫날)은 제외하고 회귀합니다
       '''
    
    for key, strategy_df in return_dict.items():  
        if str(key) =="count":
            continue
        
        if constant:
            model = OLS(strategy_df.iloc[1:], add_constant(mkt_rtn.iloc[1:]))
        else:
            model = OLS(strategy_df.iloc[1:], mkt_rtn.iloc[1:])
        result = model.fit()
        
        print(f"{key} Regression Result")
        print(result.summary2())
        
def print_statistics(return_dict:dict,
                     mkt_rtn=None):
    '''return_dict : dict(전략 수익률이 담긴 딕셔너리)
       mkt_rtn     : pd.Series (마켓 수익률이 담긴 시리즈)
       
       Note) mean, std, cagr 계산할 때 시작일은 포함하지 않습니다'''
        
    mean = [(df.iloc[1:].mean() * 365).round(6) for key, df in return_dict.items()]          
    std = [df.iloc[1:].std() * np.sqrt(365) for key, df in return_dict.items()]
    cagr =[calculate_cagr(df.iloc[1:]) for key, df in return_dict.items()]
    mdd = []
    
    for key, df in return_dict.items():
        cum_df = (df+1).cumprod()
        peak = cum_df.cummax()
        drawdown = (cum_df-peak)/peak
        mdd.append((-drawdown).max().round(3))
    
    return_df = pd.DataFrame([cagr,mean,std,mdd], index=["CAGR", "Mean","STD","MDD"])
    
    if mkt_rtn != None:
        cum_df = (mkt_rtn+1).cumprod()
        peak = cum_df.cummax()
        drawdown = (cum_df-peak)/peak
        mdd= (-drawdown).max().round(3)
        mkt = pd.DataFrame([calculate_cagr(mkt_rtn.iloc[1:]), mkt_rtn.iloc[1:].mean() * 365, mkt_rtn.iloc[1:].std() * np.sqrt(365), mdd],
                               index=["CAGR", "Mean","STD","MDD"], columns=["MKT"])
        
        return_df = pd.concat([return_df, mkt], axis=1)
    return_df.loc["Sharpe",:] = (return_df.loc["Mean",:]) / (return_df.loc["STD",:])
    
    return return_df

# Daily 손익으로 변환해준다
def change_weekly_to_daily(weekly_price_df, weekly_rtn_df, weekly_weight_df, daily_price, freq):
    '''모든 데이터는 Weekly로 줘야함
       '''
    pf_weekly_value = ((weekly_rtn_df * weekly_weight_df.shift(1)).sum(1) + 1).cumprod()
    each_input_money = (weekly_weight_df.T * pf_weekly_value).T # toy df,s 실험으로 찾음
    how_many_coin = each_input_money / weekly_price_df 
    daily_coin_converted = pd.DataFrame(index=daily_price.loc[freq:].index,  # 여기서 weight말고 코인 개수를 ffill 해야함
                                        columns=daily_price.loc[freq:].columns) \
                                    .fillna(how_many_coin) \
                                    .ffill(limit=6) 
                                    
    pf_value = ((daily_price * daily_coin_converted).loc[freq:].sum(1) \
                                                  .pct_change(fill_method=None)) \
                                                  .fillna(0)
    return pf_value

def draw_return_result(return_dict:dict, 
                       mkt_rtn=None):
    
    '''return_dict : dict(리턴이 담긴 딕셔러니)
       mkt_rtn     : Series(마켓 리턴이 담긴 시리즈)'''
        
    for key, df in return_dict.items():
        fig, axes = plt.subplots(3,1, sharex=True, figsize=(24,24), gridspec_kw={'height_ratios': [4, 1, 1]})
        cum_df = (df+1).cumprod()
        cum_df.plot(ax=axes[0])
            
        axes[0].set_title(f"Cross-Sectional Momentum Value Weighted Result of {key}")
        axes[0].grid()
        axes[0].legend(["Startegy","MKT"])
        
        peak = cum_df.cummax()
        drawdown = (cum_df-peak)/peak
        drawdown.plot(ax=axes[1])
        axes[1].set_title("Draw Down")
        axes[1].grid()
        
        df.plot(ax=axes[2])
        axes[2].grid()
        
        if mkt_rtn != None:
            mktcum = (mkt_rtn+1).cumprod()
            mktcum.plot(ax=axes[0])
            axes[0].grid()
            axes[0].legend(["Startegy","MKT"])
            
            peak = mktcum.cummax()
            drawdown = (mktcum-peak) / peak
            drawdown.plot(ax=axes[1], alpha=0.3)
            axes[1].grid()
            
            mkt_rtn.plot(ax=axes[2], alpha=0.3)
            axes[2].grid();      
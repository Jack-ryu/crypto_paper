import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import matplotlib as mpl 
from statsmodels.api import OLS, add_constant


def calculate_cagr(return_df):
    '''rtn을 받았을 때, CAGR을 계산합니다'''
    holding_year = (len(return_df) / 365)
    cum = (return_df+1).cumprod()
    cagr = (cum.iloc[-1] / cum.iloc[0]) ** (1/holding_year) -1
    return round(cagr, 4)


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
                     mkt_rtn=None,
                     start_date=None):
    '''return_dict : dict(전략 수익률이 담긴 딕셔너리)
       mkt_rtn     : pd.Series (마켓 수익률이 담긴 시리즈)
       
       Note) mean, std, cagr 계산할 때 시작일은 포함하지 않습니다(시작일 수익은 0이라서)'''

    mean = []
    std = []
    cagr = []
    mdd = []
    key_list = []

    for key, df in return_dict.items():
        key_list.append(key)
        
        if start_date != None:
            df2 = df.loc[start_date:].iloc[1:]
        else:
            df2 = df.iloc[1:]
            
        m = (df2.mean() * 365).round(5)   
        mean.append(m)
        
        s = (df2.std() * np.sqrt(365))
        std.append(s)
        
        ca = calculate_cagr(df2)
        cagr.append(ca)
        
        cum_df = (df2+1).cumprod()
        peak = cum_df.cummax()
        drawdown = (cum_df-peak)/peak
        mdd.append(round((-drawdown).max(),3))
    
    col = [key for key, df in return_dict.items()]
    
    return_df = pd.DataFrame([mean,std,cagr,mdd], index=["Mean","STD","CAGR","MDD"], columns=col)  
    
    if mkt_rtn != None:
        key_list.append("MKT")
       
        if start_date != None:
            mkt_rtn2 = mkt_rtn.loc[start_date:].iloc[1:]
        else:
            mkt_rtn2 = mkt_rtn.iloc[1:]
            
        cum_df = (mkt_rtn2+1).cumprod()
        peak = cum_df.cummax()
        drawdown = (cum_df-peak)/peak
        mdd= round((-drawdown).max(), 3)
        
        mkt = pd.DataFrame([mkt_rtn2.mean() * 365, 
                            mkt_rtn2.std() * np.sqrt(365),
                            calculate_cagr(mkt_rtn2), 
                            mdd],
                            index=["CAGR", "Mean","STD","MDD"], 
                            columns=["MKT"])
        
        return_df = pd.concat([return_df, mkt], axis=1, keys=key_list)
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
                       title:str,
                       log=True,
                       mkt_rtn=None,
                       one_plot=False,
                       start_date=None):
    
    '''
    return_dict : dict(리턴이 담긴 딕셔러니)
    title       : title을 지정할 수 있습니다
    log         : 로그 y축 (Default=True)
    mkt_rtn     : Series(마켓 리턴이 담긴 시리즈)
    one_plot    : T/F (한개에 모든 플랏을 그릴지 결정(start_date 수동으로 지정해야함))
    start_date : plot을 언제부터 그릴지 결정 (one_plot = True일 때만 사용가능)
       '''
       
    mpl.style.use("seaborn")
    
    # 전부 하나의 plot에 그리는 경우...
    if one_plot == True:
        fig, axes = plt.subplots(3,1, sharex=True, figsize=(24,24), gridspec_kw={'height_ratios': [4, 1, 1]})
        
        for key, df in return_dict.items():
            df = df.loc[start_date:]
            df.loc[start_date] = 0 # 투자 시작일 값은 0으로 셋팅(그래야 포폴 가치가 1이 됨)
            cum_df = (df+1).cumprod()
            cum_df.plot(ax=axes[0], label=key, logy=log)

            axes[0].set_title(f"{title}")
            axes[0].legend()
            #axes[0].grid(axis="both")

            peak = cum_df.cummax()
            drawdown = (cum_df-peak)/peak
            drawdown.plot(ax=axes[1])
            axes[1].set_title("Draw Down")
            #axes[1].grid(axis="both")

            df.plot(ax=axes[2])
            #axes[2].grid(axis="both")

            if mkt_rtn != None:
                mkt_rtn2 = mkt_rtn.loc[start_date:]
                mktcum = (mkt_rtn2+1).cumprod()
                mktcum.plot(ax=axes[0], logy=log)
                #axes[0].grid(axis="both")
                
                
                peak = mktcum.cummax()
                drawdown = (mktcum-peak) / peak
                drawdown.plot(ax=axes[1], alpha=0.3)
                #axes[1].grid(axis="both")

                mkt_rtn.plot(ax=axes[2], alpha=0.3)
                #axes[2].grid(axis="both")
                
        plt.tight_layout()
   
    # 전부 별개의 plot에 그리는 경우...            
    else:
        for key, df in return_dict.items():
            fig, axes = plt.subplots(3,1, sharex=True, figsize=(24,24), gridspec_kw={'height_ratios': [4, 1, 1]})
            
            cum_df = (df+1).cumprod()
            cum_df.plot(ax=axes[0],logy=log)

            axes[0].set_title(f"{title} - {key}")
            #axes[0].grid(axis="both")
            axes[0].legend([f"{key}","MKT"])

            peak = cum_df.cummax()
            drawdown = (cum_df-peak)/peak
            drawdown.plot(ax=axes[1])
            axes[1].set_title("Draw Down")
            #axes[1].grid(axis="both")

            df.plot(ax=axes[2])
            #axes[2].grid(axis="both")

            if mkt_rtn != None:
                mktcum = (mkt_rtn+1).cumprod()
                mktcum.plot(ax=axes[0], logy=log)
                #axes[0].grid(axis="both")
                axes[0].legend(["Startegy","MKT"])

                peak = mktcum.cummax()
                drawdown = (mktcum-peak) / peak
                drawdown.plot(ax=axes[1], alpha=0.3)
                #axes[1].grid(axis="both")

                mkt_rtn.plot(ax=axes[2], alpha=0.3)
                #axes[2].grid(axis="both")
            plt.tight_layout()
            plt.legend();


def draw_coin_count(time_series_coin_num:dict, draw_mkt=True):
    '''time_series_coin_num :  코인 개수가 담긴 딕셔너리,
       draw_mkt = True(딕셔너리), False(딕셔너리의 딕셔너리를 줘야함)
    '''
    fig, ax = plt.subplots(1,1)
    plt.title("Change of Number of coins")
    plt.ylabel("Number of Coins")
    
    if draw_mkt:
        for key, df in time_series_coin_num.items():
            df.plot(figsize=(24,12), ax=ax, label=key)
    else:
        for outer_key, df_dict in time_series_coin_num.items(): #바깥쪽 Loop
            sum_df = pd.DataFrame()
            for key, df in df_dict.items(): #안쪽 Loop
                sum_df = pd.concat([sum_df, df], axis=1).sum(1)
            sum_df.plot(figsize=(24,12), ax=ax, label=outer_key)
        
    plt.legend()
    plt.tight_layout();
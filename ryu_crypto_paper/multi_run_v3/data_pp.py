import numpy as np
import numpy as np
import pandas as pd 

### 새로운거 개발 중... 2023-06-25
# 코드를 돌릴때 딱 한번만 실행하면 되는 애들은 여기에다 개발

class DataPreprocess:

    def __init__(self):
        self.vol_df    = None
        self.price_df  = None
        self.mktcap_df = None
        self.daily_rtn_df = None
        self.mask = None
        
        
    def initialize(self,
                   data:pd.DataFrame):
        v,p,m,col = "vol","close","mktcap","coin_id" 

        df_pivot = pd.pivot_table(data=data,      # 2023-06-25 수정... pivot_table을 한 번만 call 한다
                                  values=[v,p,m],
                                  index=data.index, 
                                  columns=col)

        vol_df = df_pivot[v]
        price_df = df_pivot[p].replace(0, np.nan)
        mktcap_df = df_pivot[m]
        
        #return_dict = {"price":price_df,
        #               "mktcap": mktcap_df,
        #               "vol": vol_df}  # 리턴을 줄지, 인스턴스로 줄지..
        
        self.vol_df = vol_df
        self.price_df = price_df
        self.mktcap_df = mktcap_df
        self.daily_rtn_df = price_df.pct_change(fill_method=None)   
        self.weekly_rtn_df = price_df.pct_change(7, fill_method=None)
        
    def make_mask(self,
                  mktcap_thresh:int,
                  vol_thresh:int,
                  ma=bool):
    
        mktcap_df = self.mktcap_df#.replace(0, np.nan) # Test
        vol_df = self.vol_df#.replace(0, np.nan)
        
        if ma:
            if  mktcap_thresh != None:
                mktcap_mask = (mktcap_df.rolling(window=30).mean() >= mktcap_thresh) 
                mktcap_mask_ = np.where(mktcap_mask==True, 1, np.nan)
                mktcap_mask = pd.DataFrame(mktcap_mask_,
                                           index=mktcap_mask.index,
                                           columns=mktcap_mask.columns)     

                if vol_thresh != None:
                    vol_mask = (vol_df.rolling(window=30).mean() >= vol_thresh) 
                    vol_mask_ = np.where(vol_mask==True, 1, np.nan)
                    vol_mask = pd.DataFrame(vol_mask_, 
                                            index=vol_mask.index, 
                                            columns=vol_mask.columns)

                    mask = mktcap_mask * vol_mask
                else: # mkt만 조건이 있고, vol은 조건이 없는 경우
                    mask = mktcap_mask

            else: # 아무 조건이 안 걸리는 경우 (전부 1로 채운 mask 생성)
                mask = pd.DataFrame(1, 
                                    index = mktcap_df.index, 
                                    columns = mktcap_df.columns) 

        elif ma == False:
            if  mktcap_thresh != None:
                mktcap_mask = (mktcap_df >= mktcap_thresh) 
                mktcap_mask_ = np.where(mktcap_mask==True, 1, np.nan)
                mktcap_mask = pd.DataFrame(mktcap_mask_, 
                                           index=mktcap_mask.index, 
                                           columns=mktcap_mask.columns)   

                if vol_thresh != None:
                    vol_mask = (vol_df >= vol_thresh) 
                    vol_mask_ = np.where(vol_mask==True, 1, np.nan)
                    vol_mask = pd.DataFrame(vol_mask_, 
                                            index=vol_mask.index, 
                                            columns=vol_mask.columns)

                    mask = mktcap_mask * vol_mask
                else: # mkt만 조건이 있고, vol은 조건이 없는 경우
                    mask = mktcap_mask

            else: # 아무 조건이 안 걸리는 경우 (전부 1로 채운 mask 생성)
                mask = pd.DataFrame(1,
                                    index= mktcap_df.index, 
                                    columns=mktcap_df.columns)     
                
        self.mask = mask
        

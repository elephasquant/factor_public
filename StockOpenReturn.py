from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd

import rqdatac as rq

from factorbase.factor import Factor, SecurityType, Frequency, FactorType


class StockOpenReturn(Factor):
    def __init__(self):
        rq.init()
    
    def factor_name(self) -> str:
        return "StockOpenReturn"
    
    def author(self) -> str:
        return "xitong"
    
    def desc(self) -> str:
        return "stock return using open price"
    
    def factor_type(self) -> FactorType:
        return FactorType.NORMAL
    
    def first_start_time(self) -> datetime:
        return datetime(2010,1,1)

    @Factor.checker
    def frequency(self) -> Frequency:
        return Frequency.DAILY

    @Factor.checker
    def security_type(self) -> SecurityType:
        return SecurityType.STOCK

    @Factor.checker
    def trigger_time(self) -> str:
        return "0 30 9 * * * *"

    @Factor.checker
    def run(self, start_time: datetime, end_time: datetime) -> Tuple[pd.DataFrame, Exception]:
        codes = rq.all_instruments(type="Stock")['order_book_id'].to_list()
        codes = sorted(list(codes))
        
        df_px = rq.get_price(codes,
                          start_date=start_time.strftime('%Y-%m-%d'),
                          end_date=end_time.strftime('%Y-%m-%d'),
                          frequency='1d',
                          adjust_type='pre',
                          fields=['open'])
               
        df = pd.DataFrame(index=df_px.index.levels[1].rename('datetime'), columns=['gen_time'] + codes)
        for code in codes:
            try:
                df[code] = df_px.loc[code]
            except:
                pass
        df = df.shift(-1) / df - 1
        df['gen_time'] = df.index.map(lambda x: x + timedelta(hours=9,minutes=30))
        df['gen_time'] = df['gen_time'].shift(-1)
            
        return df, None



if __name__ == '__main__':
    now = datetime.now()
    close = StockOpenReturn()
    try:
        df, err = close.run(datetime(2010,1,1), now)
    except Exception as e:
        print("error: ", e)
        exit(-1)
    
    print(df, err)
    df.to_pickle("StockOpenReturn.pkl")
    

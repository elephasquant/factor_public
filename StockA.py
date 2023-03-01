from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd
from functools import reduce

import rqdatac as rq

from factorbase.factor import Factor, SecurityType, Frequency, FactorType


class StockA(Factor):
    def __init__(self):
        rq.init()

    def factor_name(self) -> str:
        return "StockA"
    
    def factor_type(self) -> FactorType:
        return FactorType.POOL
    
    def first_start_time(self) -> datetime:
        return datetime(2010,1,1)

    def author(self) -> str:
        return "xitong"

    def desc(self) -> str:
        return "all China market A stocks"


    @Factor.checker
    def frequency(self) -> Frequency:
        return Frequency.DAILY

    @Factor.checker
    def security_type(self) -> SecurityType:
        return SecurityType.STOCK

    @Factor.checker
    def trigger_time(self) -> str:
        return "0 1 0 * * * *"

    @Factor.checker
    def run(self, start_time: datetime, end_time: datetime) -> Tuple[pd.DataFrame, Exception]:
        codes = rq.all_instruments(type="Stock")['order_book_id'].to_list()
        codes = sorted(list(codes))
        
        df_px = rq.get_price(codes,
                          start_date=start_time.strftime('%Y-%m-%d'),
                          end_date=end_time.strftime('%Y-%m-%d'),
                          frequency='1d',
                          adjust_type='none',
                          fields=['open'])
               
        df = pd.DataFrame(index=df_px.index.levels[1].rename('datetime'), columns=['gen_time'] + codes)
        for code in codes:
            try:
                df[code] = df_px.loc[code]
            except:
                pass
        
        df['gen_time'] = df.index.map(lambda x: x + timedelta(hours=15))
        df = df.fillna(0)
        df = df.where(df==0, 1)
            
        return df, None
        


if __name__ == '__main__':
    now = datetime.now()
    factor = StockA()
    try:
        df, err = factor.run(datetime(2010, 1, 1), now)
    except Exception as e:
        print("error: ", e)
        exit(-1)

    print(df, err)
    df.to_pickle("StockA.pkl")

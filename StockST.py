from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd

import rqdatac as rq

from factorbase.factor import Factor, SecurityType, Frequency, FactorType


class StockST(Factor):
    def __init__(self):
        rq.init()
    
    def factor_name(self) -> str:
        return "StockST"
    
    def author(self) -> str:
        return "xitong"

    def factor_type(self) -> FactorType:
        return FactorType.POOL
    
    def first_start_time(self) -> datetime:
        return datetime(2010,1,1)
    
    def desc(self)->str:
        return "is ST stock"

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
        
        df = rq.is_st_stock(codes, start_date=start_time, end_date=end_time)
        codes = sorted(list(df.columns))
        
        df['gen_time'] = df.index.map(lambda x: x + timedelta(hours=0))
        
        df = df[['gen_time'] + codes]
        df.index.name = 'datetime'
        df = df.astype(int)
            
        return df, None



if __name__ == '__main__':
    now = datetime.now()
    st = StockST()
    try:
        df, err = st.run(datetime(2010,1,1), now)
    except Exception as e:
        print("error: ", e)
        exit(-1)
    
    print(df, err)
    df.to_pickle("StockST.pkl")
    

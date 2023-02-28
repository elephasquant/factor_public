from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd

import rqdatac as rq

from factorbase.factor import Factor, SecurityType, Frequency


class StockIndustryCitics2019First(Factor):
    def __init__(self):
        rq.init()
    
    def factor_name(self) -> str:
        return "StockIndustryCitics2019First"
    
    def author(self) -> str:
        return "xitong"
    
    def desc(self)->str:
        return "stock citics first level industry"

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
        
        trade_days = rq.get_trading_dates(start_date=start_time, end_date=end_time)
        trade_days = sorted(list(map(lambda day: datetime(day.year, day.month, day.day), trade_days)))
        
        df = pd.DataFrame(index=trade_days, columns=['gen_time']+codes)
        
        for trade_day in trade_days:
            df_indu = rq.get_instrument_industry(order_book_ids=codes,
                                            date=trade_day,
                                            level=1,
                                            source='citics_2019')
            df.loc[trade_day] = df_indu['first_industry_name']
            
        df.index = df.index.rename("datetime")            
        df['gen_time'] = df.index
        return df, None



if __name__ == '__main__':
    now = datetime.now()
    factor = StockIndustryCitics2019First()
    try:
        df, err = factor.run(datetime(2010,1,1), now)
    except Exception as e:
        print("error: ", e)
        exit(-1)
    
    print(df, err)
    df.to_pickle("StockIndustryCitics2019First.pkl")
    

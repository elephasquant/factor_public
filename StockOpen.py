from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd

import rqdatac as rq

from factorbase.factor import Factor, SecurityType, Frequency, FactorType


class StockOpen(Factor):
    def __init__(self):
        rq.init()
    
    def factor_name(self) -> str:
        return "StockOpen"
    
    def author(self) -> str:
        return "xitong"
    
    def factor_type(self) -> FactorType:
        return FactorType.NORMAL
    
    def first_start_time(self) -> datetime:
        return datetime(2010,1,1)
    
    def desc(self)->str:
        return "stock open price (no adjust)"

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
                          adjust_type='none',
                          fields=['open'])

        now_datetime = datetime.now()
        now_datetime_str = now_datetime.strftime('%Y-%m-%d')

        today_open_price_dict = {}
        if now_datetime.date() <= end_time.date() and self.is_trading_time():
            today_tick_obj_list = rq.current_snapshot(codes)

            for tick_obj in today_tick_obj_list:
                today_open_price_dict[tick_obj.order_book_id] = tick_obj.open

        if today_open_price_dict:
            for stock_code, open_price in today_open_price_dict.items():
                df_px.loc[(stock_code, now_datetime_str), 'open'] = open_price

        df = pd.DataFrame(index=df_px.index.levels[1].rename('datetime'), columns=['gen_time'] + codes)
        for code in codes:
            try:
                df[code] = df_px.loc[code]
            except:
                pass
        df['gen_time'] = df.index.map(lambda x: x + timedelta(hours=9,minutes=30))
            
        return df, None

    def is_trading_time(self):

        now_datetime = datetime.now()

        morning_open_start = datetime(year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
                                      hour=9, minute=30)

        morning_open_end = datetime(year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
                                    hour=11, minute=30)

        afternoon_open_start = datetime(year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
                                        hour=13, minute=0)

        afternoon_open_end = datetime(year=now_datetime.year, month=now_datetime.month, day=now_datetime.day,
                                      hour=15, minute=0)

        if now_datetime.weekday() <= 4 and \
                (morning_open_start <= now_datetime <= morning_open_end or
                 afternoon_open_start <= now_datetime <= afternoon_open_end):
            return True

        return False



if __name__ == '__main__':
    now = datetime.now()
    close = StockOpen()
    try:
        df, err = close.run(datetime(2010,1,1), now)
    except Exception as e:
        print("error: ", e)
        exit(-1)
    
    print(df, err)
    df.to_pickle("StockOpen.pkl")
    

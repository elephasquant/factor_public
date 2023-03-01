from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd
from functools import reduce

import rqdatac as rq

from factorbase.factor import Factor, SecurityType, Frequency, FactorType


class Stock300(Factor):
    def __init__(self):
        rq.init()

    def factor_name(self) -> str:
        return "Stock300"
    
    def factor_type(self) -> FactorType:
        return FactorType.POOL
    
    def first_start_time(self) -> datetime:
        return datetime(2010,1,1)

    def author(self) -> str:
        return "xitong"

    def desc(self) -> str:
        return "000300.XSHG components"

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
        cs = rq.index_components(
            '000300.XSHG', start_date=start_time, end_date=end_time)

        index = pd.Series(data=cs.keys(), name='datetime')
        codes = sorted(list(reduce(lambda a, b: set(a) | set(b), cs.values())))

        df = pd.DataFrame(index=index, columns=['gen_time'] + codes)
        df['gen_time'] = df.index.map(lambda x: x + timedelta(hours=0))

        for dt in index:
            df.loc[dt, cs[dt]] = 1
        df = df.fillna(0)
        return df, None


if __name__ == '__main__':
    now = datetime.now()
    s300 = Stock300()
    try:
        df, err = s300.run(datetime(2010, 1, 1), now)
    except Exception as e:
        print("error: ", e)
        exit(-1)

    print(df, err)
    df.to_pickle("Stock300.pkl")

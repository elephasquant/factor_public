from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd
import os

from factorbase.factor import Factor, SecurityType, Frequency, FactorType


class StockBarraLeverage(Factor):
    def __init__(self):
        pass
    
    def factor_name(self) -> str:
        return "StockBarraLeverage"
    
    def author(self) -> str:
        return "xitong"
    
    def factor_type(self) -> FactorType:
        return FactorType.RISK
    
    def first_start_time(self) -> datetime:
        return datetime(2019,1,1)
    
    def desc(self)->str:
        return "stock barra leverage factor"

    @Factor.checker
    def frequency(self) -> Frequency:
        return Frequency.DAILY

    @Factor.checker
    def security_type(self) -> SecurityType:
        return SecurityType.STOCK

    @Factor.checker
    def trigger_time(self) -> str:
        return "0 0 4 * * * *"

    @Factor.checker
    def run(self, start_time: datetime, end_time: datetime) -> Tuple[pd.DataFrame, Exception]:
        factor_name = 'leverage'
        src = '/mnt/Q/users/liujianyu/risk_management/exposures'
        rows = []
        for file in sorted(os.listdir(src)):
            dt = datetime.strptime(file[:-4], '%Y%m%d')
            if dt < start_time or dt > end_time:
                continue
            file_path = os.path.join(src, file)
            df = pd.read_csv(file_path)
            df['stock_code'] = df['stock_code'].map(lambda x: x[2:] + '.' + ('XSHG' if x[:2]== 'SH' else 'XSHE'))
            df = df.set_index(['stock_code'])
            
            row = df[factor_name].copy()
            row['datetime'] = dt
            row = row.to_frame().T
            rows.append(row)
        
        df = pd.concat(rows,axis=0).set_index(['datetime'])
        df['gen_time'] = df.index.map(lambda dt: dt + timedelta(hours=15))
        
        columns = sorted(list(df.columns))
        columns.remove('gen_time')
        df = df[['gen_time'] + columns]
        
        return df, None
        


if __name__ == '__main__':
    now = datetime.now()
    factor = StockBarraLeverage()
    try:
        df, err = factor.run(datetime(2010,1,1), now)
    except Exception as e:
        print("error: ", e)
        exit(-1)
    
    print(df, err)
    df.to_pickle("StockBarraLeverage.pkl")
        

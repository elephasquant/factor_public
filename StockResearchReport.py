import os
from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd
import numpy as np
import rqdatac as rq

from factorbase.factor import Factor, SecurityType, Frequency, FactorType


class StockResearchReport(Factor):

    def __init__(self):
        rq.init()

    def factor_name(self) -> str:
        return "StockResearchReport"
    
    def factor_type(self) -> FactorType:
        return FactorType.POOL
    
    def first_start_time(self) -> datetime:
        return datetime(2010,1,1)
    
    def author(self) -> str:
        return "shijiachen"

    def desc(self) -> str:
        return "research report data factor"

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

        origin_dir = '/mnt/Q/users/shijiachen/研报/results'

        file_name_list = os.listdir(origin_dir)
        file_name_list = [i for i in file_name_list if i.endswith('.csv')]
        file_name_list = sorted(file_name_list, reverse=True)

        src_path = '/'.join((origin_dir, file_name_list[0]))

        for name in file_name_list:
            name_time_obj = datetime.strptime(name, '%Y%m%d.csv')
            if start_time.date() <= name_time_obj.date() <= end_time.date():
                src_path = '/'.join((origin_dir, name))
                break

        # src_df = pd.read_csv('./20230515.csv')
        src_df = pd.read_csv(src_path)

        col_name_list = src_df.columns.to_list()

        new_col_dict = {'trade_date': 'datetime'}

        for col_name in col_name_list:

            if 'SH' in col_name:
                stock_code = col_name.replace('SH', '') + '.XSHG'

            elif 'SZ' in col_name:
                stock_code = col_name.replace('SZ', '') + '.XSHE'

            elif 'BJ' in col_name:
                stock_code = col_name.replace('BJ', '') + '.BJSE'

            else:
                continue

            new_col_dict[col_name] = stock_code

        df = src_df.rename(columns=new_col_dict)

        df.sort_values(by='datetime', inplace=True)
        # print(df)

        first_row = df.head(1)
        last_row = df.tail(1)
        csv_start_str = first_row.at[0, 'datetime']
        csv_end_str = last_row.iloc[-1]['datetime']

        csv_start_time_obj = datetime.strptime(csv_start_str, '%Y-%m-%d')
        csv_end_time_obj = datetime.strptime(csv_end_str, '%Y-%m-%d')

        # print(csv_start_time_obj, csv_end_time_obj)

        resp = rq.get_trading_dates(
            start_date=csv_start_time_obj,
            end_date=csv_end_time_obj
        )
        trading_date_str_list = [i.strftime('%Y-%m-%d') for i in resp]

        df_columns = df.columns.to_list()
        df_columns.remove('datetime')

        df_table = pd.DataFrame(
            index=trading_date_str_list,
            columns=df_columns
        )
        df_table.index.name = 'datetime'

        df = df.set_index('datetime')

        # print(df)

        existed_index_datetime_list = df.index.to_list()

        existed_index_datetime_set = set(existed_index_datetime_list)

        trading_date_str_set = set(trading_date_str_list)

        adding_index_list = list(existed_index_datetime_set - trading_date_str_set)
        if adding_index_list:
            empty_row = {stock_code: np.nan for stock_code in df_columns}
            for a in adding_index_list:
                df.loc[a] = empty_row

        # print(df)

        new_df = df
        new_df.sort_values(by='datetime', inplace=True)

        # print(new_df)

        new_df.fillna(axis=0, method='ffill', inplace=True)

        # print(new_df)

        new_index_str_set = set(new_df.index.to_list())
        trading_date_str_set = set(trading_date_str_list)

        drop_row_index_list = new_index_str_set - trading_date_str_set
        # print(drop_row_index_list)
        for i in drop_row_index_list:
            new_df.drop(index=i, inplace=True)

        df = new_df.reset_index()
        # print(df)

        # exit()

        df.insert(0, 'gen_time', df['datetime'].copy())
        datetime_col = df['datetime'].astype('datetime64')
        gen_time_col = df['gen_time'].astype('datetime64')

        df['datetime'] = datetime_col
        df['gen_time'] = gen_time_col

        df = df.set_index('datetime')

        # print(df)

        return df, None


if __name__ == '__main__':

    now = datetime.now()
    obj = StockResearchReport()

    try:
        df, err = obj.run(datetime(2008, 1, 1), now)

    except Exception as e:
        print("error: ", e)
        exit(-1)

    print(df, err)
    df.to_pickle("StockResearchReport.pkl")

"""Contains methods and classes to collect data from
futu api
"""

from __future__ import annotations
from futu import *

import pandas as pd

class FutuDownloader:
    def __init__(self, start_date: str, end_date: str, ticker_list: list):
        self.start_date = start_date
        self.end_date = end_date
        self.ticker_list = ticker_list
    
    def fetch_data(self, proxy=None) -> pd.DataFrame:
        
        quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
        data_df = pd.DataFrame()
        for currentTic in self.ticker_list:
            ret, data, page_req_key = quote_ctx.request_history_kline(currentTic, start=self.start_date, end=self.end_date, max_count=5)  # 每页5个，请求第一页
            if ret == RET_OK:
                # print(data)
                # print(data['code'][0])  # 取第一条的股票代码
                # print(data['close'].values.tolist())   # 第一页收盘价转为 list
                data_df = data
            else:
                print('error:', data)
            while page_req_key != None:  # 请求后面的所有结果
                print('*************************************')
                print(page_req_key)
                ret, data, page_req_key = quote_ctx.request_history_kline(currentTic, start=self.start_date, end=self.end_date, max_count=5, page_req_key=page_req_key) 
                data_df = pd.concat([data_df, data], axis=0)
                # 请求翻页后的数据
            if ret == RET_OK:
                print(data)
            else:
                print('error:', data)
                print('All pages are finished!')

        quote_ctx.close() # 结束后记得关闭当条连接，防止连接条数用尽
        
        data_df = data_df.reset_index()
        
        """Fetches data from Yahoo API
        Parameters
        ----------

        Returns
        -------
        `pd.DataFrame`
            7 columns: A date, open, high, low, close, volume and tick symbol
            for the specified stock ticker
        """
        data_df["day"] = data_df["time_key"]
        data_df["date"] = data_df["time_key"]
        data_df["tic"] = data_df["code"]
        data_df["adjcp"] = data_df["close"]

        # convert date to standard string format, easy to filter
        # data_df["day"] = data_df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
        # drop missing data
        data_df = data_df.dropna()
        # data_df = data_df.reset_index(drop=True)
        # print("Shape of DataFrame: ", data_df.shape)
        # print("Display DataFrame: ", data_df.head())

        # data_df = data_df.sort_values(by=["date", "tic"]).reset_index(drop=True)
        print(data_df)
        return data_df

    def select_equal_rows_stock(self, df):
        df_check = df.tic.value_counts()
        df_check = pd.DataFrame(df_check).reset_index()
        df_check.columns = ["tic", "counts"]
        mean_df = df_check.counts.mean()
        equal_list = list(df.tic.value_counts() >= mean_df)
        names = df.tic.value_counts().index
        select_stocks_list = list(names[equal_list])
        df = df[df.tic.isin(select_stocks_list)]
        return df

# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import math
import time
import datetime
from six.moves import reduce

from rqalpha.utils.datetime_func import convert_dt_to_int


def is_holiday_today():
    today = datetime.date.today()
    from rqalpha.environment import Environment
    return not Environment.get_instance().data_proxy.is_trading_date(today)


def is_tradetime_now():
    now_time = time.localtime()
    now = (now_time.tm_hour, now_time.tm_min, now_time.tm_sec)
    if (9, 15, 0) <= now <= (11, 30, 0) or (13, 0, 0) <= now <= (15, 0, 0):
        return True
    return False


TUSHARE_CODE_MAPPING = {
    "sh": "000001.XSHG",
    "sz": "399001.XSHE",
    "sz50": "000016.XSHG",
    "hs300": "000300.XSHG",
    "sz500": "000905.XSHG",
    "zxb": "399005.XSHE",
    "cyb": "399006.XSHE",
}


def tushare_code_2_order_book_id(code):
    try:
        return TUSHARE_CODE_MAPPING[code]
    except KeyError:
        if code.startswith("6"):
            return "{}.XSHG".format(code)
        elif code[0] in ["3", "0"]:
            return "{}.XSHE".format(code)
        else:
            raise RuntimeError("Unknown code")
def easyquotation_code_2_order_book_id(code):
    if code[:2]=='sh' :
        return code[2:]+'.'+'XSHG'
    elif code[:2]=='sz' :
        return code[2:]+'.'+'XSHE'
    else:
            raise RuntimeError("Unknown code")

def order_book_id_2_tushare_code(order_book_id):
    return order_book_id.split(".")[0]


def get_realtime_quotes(order_book_id_list, open_only=False, include_limit=False):
    import tushare as ts
    import QUANTAXIS as QA
    import easyquotation as eq
    import pandas as pd
    
    stock_list = QA.QAFetch.QATdx.QA_fetch_get_stock_list('stock')
    code_stock_list=[]
    i=0
    for code in stock_list['code'] :
        code_stock_list.append(stock_list['sse'][i]+stock_list['code'][i])
        i +=1

    index_list = QA.QAFetch.QATdx.QA_fetch_get_stock_list('index')
    code_index_list=[]
    i=0
    for code in index_list['code'] :
        code_index_list.append(index_list['sse'][i]+index_list['code'][i])
        i +=1

    code_list=code_index_list+code_stock_list

    quotation = eq.use('sina')
    data=quotation.stocks(code_list, prefix=True) 
    total_df=pd.DataFrame(data).T
    total_df=total_df.reset_index()
    total_df=total_df.rename(columns={"index": "code",'close':"pre_close",'now':'price','turnover':'amount'})

    columns = set(total_df.columns) - set(["name", "time", "date", "code"])
    # columns = filter(lambda x: "_v" not in x, columns)
    for label in columns:
        total_df[label] = total_df[label].map(lambda x: 0 if str(x).strip() == "" else x)
        total_df[label] = total_df[label].astype(float)

    total_df["chg"] = total_df["price"] / total_df["pre_close"] - 1

    total_df["order_book_id"] = total_df["code"]
    total_df["order_book_id"] = total_df["order_book_id"].apply(easyquotation_code_2_order_book_id)
    
    total_df = total_df.set_index("order_book_id").sort_index()
    total_df["order_book_id"] = total_df.index

    total_df["datetime"] = total_df["date"] + " " + total_df["time"]

    # total_df["datetime"] = total_df["datetime"].apply(
    #     lambda x: convert_dt_to_int(datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))

    total_df["close"] = total_df["price"]
    total_df["last"] = total_df["price"]

    total_df = total_df.rename(columns={
        "{}{}_p".format(base_name, i): "{}{}".format(base_name, i)
        for i in range(1, 6) for base_name in ["a", "b"]
    })
    total_df = total_df.rename(columns={"pre_close": "prev_close"})

    del total_df["code"]
    del total_df["date"]
    del total_df["time"]
    
    if include_limit:
        total_df["limit_up"] = total_df.apply(
            lambda row: row.prev_close * (1.1 if "ST" not in row["name"] else 1.05), axis=1).round(2)
        total_df["limit_down"] = total_df.apply(
            lambda row: row.prev_close * (0.9 if "ST" not in row["name"] else 0.95), axis=1).round(2)

    if open_only:
        total_df = total_df[total_df.open > 0]

    return total_df

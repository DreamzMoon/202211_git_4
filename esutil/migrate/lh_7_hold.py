# -*- coding: utf-8 -*-

# @Time : 2022/1/28 15:16

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : lh_7_hold.py

import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
from datetime import timedelta,date
from functools import reduce
from analyzeserver.common import *
import time
import requests

import time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import numpy as np
import time

es = Elasticsearch()


def str_to_date(x):
    try:
        if x:
           return pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S')
        else:
            pass
    except:
       return "error"

'''批量持有入库'''

conn_lh = direct_get_conn(lianghao_mysql_conf)
logger.info(conn_lh)
sql = '''select * from lh_pretty_hold_9 where del_flag  = 0 limit 3000000,3000000'''
datas = pd.read_sql(sql,conn_lh)


datas["thaw_time"] = datas["thaw_time"].apply(lambda x: str_to_date(x))
datas["use_time"] = datas["use_time"].apply(lambda x: str_to_date(x))
datas["create_time"] = datas["create_time"].apply(lambda x: str_to_date(x))
datas["update_time"] = datas["update_time"].apply(lambda x: str_to_date(x))
datas["pay_time"] = datas["pay_time"].apply(lambda x: str_to_date(x))
datas["second_hand_thaw_time"] = datas["second_hand_thaw_time"].apply(lambda x: str_to_date(x))

datas.fillna("",inplace=True)
datas["thaw_time"].replace("error","", inplace=True)
datas["use_time"].replace("error","", inplace=True)
datas["create_time"].replace("error","", inplace=True)
datas["update_time"].replace("error","", inplace=True)
datas["pay_time"].replace("error","", inplace=True)
datas["second_hand_thaw_time"].replace("error","", inplace=True)


logger.info("sql data ready ok")
datas = datas.to_dict("records")
# logger.info(datas)
conn_lh.close()

count = len(datas)
logger.info("数据读取完成")

# datas = [datas[-6]]

'''批量'''

def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        print('共耗时约 {:.2f} 秒'.format(time.time() - start))
        return res

    return wrapper



@timer
def gen():
    """ 使用生成器批量写入数据 """
    action = ({
        "_index": "lhusers",
        "_type": "hold",
        "_id":data["id"],
        "order_sn":data["order_sn"],
        "pay_type":data["pay_type"],
        "sort":data["sort"],
        "thaw_time":data["thaw_time"],
        "pretty_id":data["pretty_id"],
        "pretty_account":data["pretty_account"],
        "pretty_type_id":data["pretty_type_id"],
        "pretty_type_name":data["pretty_type_name"],
        "pretty_type_length":data["pretty_type_length"],
        "hold_user_id":data["hold_user_id"],
        "hold_nick_name":data["hold_nick_name"],
        "hold_phone":data["hold_phone"],
        "status":data["status"],
        "user_id":data["user_id"],
        "nick_name":data["nick_name"],
        "phone":data["phone"],
        "use_time":data["use_time"],
        "del_flag":data["del_flag"],
        "create_by":data["create_by"],
        "create_time":data["create_time"],
        "update_by":data["update_by"],
        "update_time":data["update_time"],
        "platform_id":data["platform_id"],
        "pretty_account_key":data["pretty_account_key"],
        "unit_price":data["unit_price"],
        "sell_order_sn":data["sell_order_sn"],
        "pay_time":data["update_time"],
        "second_hand_thaw_time":data["update_time"],
        "order_type":data["order_type"],
        "is_sell":data["is_sell"],
        "retail_unit_price":data["retail_unit_price"],
        "is_up":data["is_up"],
        "is_open_vip":data["is_open_vip"],
        "is_show":data["is_show"],
        "type_wholesale":data["type_wholesale"],
        "ad_money":data["ad_money"],
        "common_money":data["common_money"],
        "newer_money":data["newer_money"],
        "is_send_money":data["is_send_money"],
        "cycle_id":data["cycle_id"],
    } for i,data in enumerate(datas))
    helpers.bulk(es, action)


if __name__ == '__main__':
    gen()


logger.info("ending")
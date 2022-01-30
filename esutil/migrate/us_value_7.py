# -*- coding: utf-8 -*-

# @Time : 2022/1/28 16:29

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : us_value_7.py

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

es = Elasticsearch()


conn_analyze = direct_get_conn(analyze_mysql_conf)
logger.info(conn_analyze)
sql = '''select * from user_storage_value'''
datas = pd.read_sql(sql,conn_analyze)
# datas.fillna("",inplace=True)
# logger.info("sql data ready ok")
# datas = datas.to_dict("records")
conn_analyze.close()

count = len(datas)
logger.info("数据读取完成")

'''批量'''

def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        print('共耗时约 {:.2f} 秒'.format(time.time() - start))
        return res

    return wrapper

# @timer
# def to_sql():
#     conn = sqlalchemy_conn(analyze_mysql_conf)
#     datas.to_sql("test",conn)

@timer
def gen():
    """ 使用生成器批量写入数据 """
    action = ({
        "_index": "users",
        "_type": "price",
        "_id":i,
        "day_time":datetime.datetime.strftime(data["day_time"], '%Y-%m-%d'),
        "unionid":data["unionid"],
        "hold_phone":data["hold_phone"],
        "name":data["name"],
        "nickname":data["nickname"],
        "parentid":data["parentid"],
        "parent_phone":data["parent_phone"],
        "operate_id":data["operate_id"],
        "operatename":data["operatename"],
        "leader_unionid":data["leader_unionid"],
        "leader":data["leader"],
        "leader_phone":data["leader_phone"],
        "no_tran_price":data["no_tran_price"],
        "no_tran_count":data["no_tran_count"],
        "transferred_count":data["transferred_count"],
        "transferred_price":data["transferred_price"],
        "public_count":data["public_count"],
        "public_price":data["public_price"],
        "use_total_price":data["use_total_price"],
        "use_count":data["use_count"],
        "hold_price":data["hold_price"],
        "hold_count":data["hold_count"],
        "tran_price":data["tran_price"],
        "tran_count":data["tran_count"],
        "addtime":datetime.datetime.strftime(data["addtime"], '%Y-%m-%d %H:%M:%S'),
        "updtime":datetime.datetime.strftime(data["updtime"], '%Y-%m-%d %H:%M:%S'),

    } for i,data in enumerate(datas))
    helpers.bulk(es, action)


if __name__ == '__main__':
    gen()
    # to_sql()


logger.info("ending")



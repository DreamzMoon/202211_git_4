# -*- coding: utf-8 -*-

# @Time : 2022/1/28 14:57

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : us_value.py

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
import threading

def to_es(i,data):
    data["day_time"] = datetime.datetime.strftime(data["day_time"], '%Y-%m-%d')
    data["addtime"] = datetime.datetime.strftime(data["addtime"], '%Y-%m-%d %H:%M:%S')
    data["updtime"] = datetime.datetime.strftime(data["updtime"], '%Y-%m-%d %H:%M:%S')
    logger.info(data)
    logger.info("current in %s and sum is %s" % (i + 1, count))
    es_url = '''http://localhost:9200/users/price/''' + str(i + 1)
    res = requests.post(url=es_url, data=json.dumps(data), headers={"Content-Type": "application/json"})
    logger.info(res.status_code)
    logger.info(res.text)
    logger.info("-------------------------")

conn_analyze = direct_get_conn(analyze_mysql_conf)
logger.info(conn_analyze)
sql = '''select * from user_storage_value'''
datas = pd.read_sql(sql,conn_analyze)
logger.info("sql data ready ok")
datas = datas.to_dict("records")
conn_analyze.close()

count = len(datas)
logger.info("数据读取完成")

for i,data in enumerate(datas):
    data["day_time"] = datetime.datetime.strftime(data["day_time"], '%Y-%m-%d')
    data["addtime"] = datetime.datetime.strftime(data["addtime"], '%Y-%m-%d %H:%M:%S')
    data["updtime"] = datetime.datetime.strftime(data["addtime"], '%Y-%m-%d %H:%M:%S')
    logger.info(data)
    logger.info("current in %s and sum is %s" % (i + 1, count))
    es_url = '''http://localhost:9200/users/price/''' + str(i + 1)
    res = requests.post(url=es_url, data=json.dumps(data), headers={"Content-Type": "application/json"})
    logger.info(res.status_code)
    logger.info(res.text)
    logger.info("-------------------------")

logger.info("ending")
# -*- coding: utf-8 -*-

# @Time : 2021/11/22 16:44

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : delete_table.py

import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback
import pandas as pd
from analyzeserver.common import *
import numpy as np
import time


# 要删除的表名
delete_list = ["lh_user","crm_user"]
delete_day = 7

conn_analyze = direct_get_conn(analyze_mysql_conf)
cursor_analyze = conn_analyze.cursor()

for dl in delete_list:
    for i in range(1,delete_day+1):
        try:
            logger.info("表:%s i：%s" %(dl,i))

            drop_sql = '''drop table %s_%s''' %(dl,(date.today() + timedelta(days=-i)).strftime("%Y%m%d"))
            logger.info(drop_sql)
            cursor_analyze.execute(drop_sql)
            conn_analyze.commit()
            logger.info("删除成功")
        except:
            pass

conn_analyze.close()



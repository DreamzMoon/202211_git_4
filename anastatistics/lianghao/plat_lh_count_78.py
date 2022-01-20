# -*- coding: utf-8 -*-

# @Time : 2022/1/7 10:34

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : plat_lh_count_78.py

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

#设置7位靓号数量 每30 分跑一次

seven_sql = '''select sum(count) seven_count from(
select count(*) count from lh_pretty_0 union all
select count(*) count from lh_pretty_1 union all
select count(*) count from lh_pretty_2 union all
select count(*) count from lh_pretty_3 union all
select count(*) count from lh_pretty_4 union all
select count(*) count from lh_pretty_5 union all
select count(*) count from lh_pretty_6 union all
select count(*) count from lh_pretty_7 union all
select count(*) count from lh_pretty_8 union all
select count(*) count from lh_pretty_9 union all 
select count(*) count from lh_free_pretty_0 union all
select count(*) count from lh_free_pretty_1 union all
select count(*) count from lh_free_pretty_2 union all
select count(*) count from lh_free_pretty_3 union all
select count(*) count from lh_free_pretty_4 union all
select count(*) count from lh_free_pretty_5 union all
select count(*) count from lh_free_pretty_6 union all
select count(*) count from lh_free_pretty_7 union all
select count(*) count from lh_free_pretty_8 union all
select count(*) count from lh_free_pretty_9
) t
'''

eight_sql = '''select sum(count) eight_count from(
select count(*) count from le_pretty_0 union all
select count(*) count from le_pretty_1 union all
select count(*) count from le_pretty_2 union all
select count(*) count from le_pretty_3 union all
select count(*) count from le_pretty_4 union all
select count(*) count from le_pretty_5 union all
select count(*) count from le_pretty_6 union all
select count(*) count from le_pretty_7 union all
select count(*) count from le_pretty_8 union all
select count(*) count from le_pretty_9 
) t
'''


try:
    conn_lh = direct_get_conn(lianghao_mysql_conf)
    plat_total_count_seven_data = pd.read_sql(seven_sql,conn_lh)
    logger.info(plat_total_count_seven_data)
    plat_total_count_7 = int(plat_total_count_seven_data["seven_count"][0])
    r = get_redis()
    r.set("plat_lh_total_count_seven",plat_total_count_7)

    plat_total_count_eight_data = pd.read_sql(eight_sql, conn_lh)
    logger.info(plat_total_count_eight_data)
    plat_total_count_8 = int(plat_total_count_eight_data["eight_count"][0])
    r = get_redis()
    r.set("plat_lh_total_count_eight", plat_total_count_8)

except:
    logger.info("异常")
finally:
    conn_lh.close()
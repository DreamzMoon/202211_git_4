# -*- coding: utf-8 -*-

# @Time : 2021/12/24 13:51

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : plat_storage_value_insert.py

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

# 名片网总数量
def plat_lh_count():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        sql = '''
        select sum(count) plat_total_count,now() from (
        select count(*) count from lh_free_pretty_0 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_1 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_2 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_3 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_4 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_5 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_6 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_7 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_8 where del_flag = 0 union all
        select count(*) count from lh_free_pretty_9 where del_flag = 0 ) lh_pretty
                '''
        plat_total_count = pd.read_sql(sql,conn_lh)
        return True,plat_total_count
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()




if __name__ == "__main__":
    sqlalchemy_an = sqlalchemy_conn(analyze_mysql_conf)
    plat_total_count_result = plat_lh_count()
    if plat_total_count_result[0] == 1:
        plat_total_count = plat_total_count_result[1]
        logger.info(plat_total_count)
        logger.info(type(plat_total_count))
        plat_total_count.to_sql('plat_lh_total_today', con=sqlalchemy_an, if_exists="append", index=False)



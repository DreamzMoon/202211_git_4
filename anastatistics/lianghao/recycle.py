# -*- coding: utf-8 -*-

# @Time : 2021/10/25 19:29

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : recycle.py

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

try:
    data = {}
    conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)
    with conn_read.cursor() as cursor:
        sql = '''
        select DATE_FORMAT(create_time,"%Y%m%d") statistic_time,count(*) recycle_count from (
            select * from lh_transfer_log_0 where del_flag = 0 and type=3   union all
            select * from lh_transfer_log_1 where del_flag = 0 and type=3   union all
            select * from lh_transfer_log_2 where del_flag = 0 and type=3   union all
            select * from lh_transfer_log_3 where del_flag = 0 and type=3   union all
            select * from lh_transfer_log_4 where del_flag = 0 and type=3  union all
            select * from lh_transfer_log_5 where del_flag = 0 and type=3   union all
            select * from lh_transfer_log_6 where del_flag = 0 and type=3 union all
            select * from lh_transfer_log_7 where del_flag = 0 and type=3   union all
            select * from lh_transfer_log_8 where del_flag = 0 and type=3   union all
            select * from lh_transfer_log_9 where del_flag = 0 and type=3 ) t GROUP BY statistic_time
            HAVING statistic_time = DATE_ADD(CURRENT_DATE,INTERVAL -1 day)
            order by statistic_time desc
        '''
        cursor.execute(sql)
        data = cursor.fetchone()

    conn_read.close()
    logger.info(data)
    if not data:
        data = ((date.today()+timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S"),0)


    conn_rw = ssh_get_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
    with conn_rw.cursor() as cursor:
        insert_sql = '''insert into lh_recycle (statistic_time,recycle_count) values (%s,%s)'''
        cursor.execute(insert_sql,data)
        conn_rw.commit()
    conn_rw.close()
except:
    logger.exception(traceback.format_exception())
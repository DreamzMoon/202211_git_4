# -*- coding: utf-8 -*-

# @Time : 2021/12/10 16:35

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : secondorderbydata.py
'''


'''

import os
import sys
from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback
from functools import reduce

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

conn_lh = direct_get_conn(lianghao_mysql_conf)

#第一次
# buy_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) buy_order_count,sum(count) buy_lh_count,sum(total_price) buy_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0 group by statistic_time having statistic_time != CURRENT_DATE order by statistic_time desc'''
# sell_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) sell_order_count,sum(count) sell_lh_count,sum(total_price) sell_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0 group by statistic_time having statistic_time != CURRENT_DATE order by statistic_time desc'''
# public_sql = '''select DATE_FORMAT(lh_sell.create_time,"%Y-%m-%d") statistic_time,count(*) public_order_count,sum(lh_sell.count) public_lh_count,sum(lh_sell.total_price) public_total_price,sum(sell_fee) total_sell_fee,(sum(lh_sell.total_price)-sum(sell_fee)) total_real_money from lh_sell
# left join lh_order on lh_sell.id = lh_order.sell_id
# where lh_sell.del_flag = 0 and lh_sell.`status`!=1 and lh_order.del_flag = 0
# group by statistic_time  having statistic_time != CURRENT_DATE
# order by statistic_time desc'''


buy_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) buy_order_count,sum(count) buy_lh_count,sum(total_price) buy_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0 group by statistic_time having statistic_time = date_add(CURRENT_DATE(),INTERVAL -1 day) order by statistic_time desc'''
sell_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) sell_order_count,sum(count) sell_lh_count,sum(total_price) sell_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0 group by statistic_time having statistic_time = date_add(CURRENT_DATE(),INTERVAL -1 day) order by statistic_time desc'''
public_sql = '''select DATE_FORMAT(lh_sell.create_time,"%Y-%m-%d") statistic_time,count(*) public_order_count,sum(lh_sell.count) public_lh_count,sum(lh_sell.total_price) public_total_price,sum(sell_fee) total_sell_fee,(sum(lh_sell.total_price)-sum(sell_fee)) total_real_money from lh_sell 
left join lh_order on lh_sell.id = lh_order.sell_id
where lh_sell.del_flag = 0 and lh_sell.`status`!=1 and lh_order.del_flag = 0 
group by statistic_time  having statistic_time = date_add(CURRENT_DATE(),INTERVAL -1 day)
order by statistic_time desc'''

buy_data = pd.read_sql(buy_sql,conn_lh)
sell_data = pd.read_sql(sell_sql,conn_lh)
public_data = pd.read_sql(public_sql,conn_lh)

df_list = []
df_list.append(buy_data)
df_list.append(sell_data)
df_list.append(public_data)
df_merged = reduce(lambda left, right: pd.merge(left, right, on=['statistic_time'], how='outer'), df_list)

conn_lh.close()

orm_conn = sqlalchemy_conn(analyze_mysql_conf)
df_merged.to_sql("lh_secorder_day", con=orm_conn, if_exists="append", index=False)
logger.info("写入成功")


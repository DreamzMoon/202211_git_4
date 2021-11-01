# -*- coding: utf-8 -*-

# @Time : 2021/11/1 11:11

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : plat_transfer_statistic.py
'''
平台上转卖和采购情况统计
'''

import time
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

    conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)

    # 第一次需要查询全部的数据
    # 统计sql  出售订单 出售靓号数量 出售订单金额 出售实收  按订单来说 采购和出售是一样的 所以采购值直接按照出售来走
    # sql = '''select DATE_FORMAT(create_time, '%Y%m%d') AS statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time HAVING statistic_time != CURRENT_DATE() order by statistic_time desc'''
    # order_datas = pd.read_sql(sql,conn_read)
    # logger.info(order_datas.shape)
    #
    # #出售sql  发布订单数量 发布订单金额 发布靓号数量
    # sql = '''select DATE_FORMAT(create_time, '%Y%m%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0
    # group by statistic_time
    # having statistic_time != CURRENT_DATE()
    # order by statistic_time desc
    #     '''
    # sell_datas = pd.read_sql(sql,conn_read)
    # logger.info(sell_datas.shape)
    #
    # last_datas = order_datas.merge(sell_datas,how="outer",on="statistic_time")
    # logger.info(last_datas.shape)

    #统计昨天的入库
    sql = '''select DATE_FORMAT(create_time, '%Y%m%d') AS statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = DATE_ADD(CURRENT_DATE(),INTERVAL -1 day)'''
    order_datas = pd.read_sql(sql, conn_read)
    logger.info(order_datas.shape)
    logger.info(order_datas)

    # 出售sql  发布订单数量 发布订单金额 发布靓号数量
    sql = '''select DATE_FORMAT(create_time, '%Y%m%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 
    and DATE_FORMAT(create_time, '%Y%m%d') = DATE_ADD(CURRENT_DATE(),INTERVAL -1 day) '''
    sell_datas = pd.read_sql(sql, conn_read)
    logger.info(sell_datas.shape)
    logger.info(sell_datas)

    last_datas = order_datas.merge(sell_datas, how="left", on="statistic_time")
    logger.info(last_datas.shape)

    conn_read.close()

    logger.info(last_datas.loc[0])
    logger.info("准备写写入")
    conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
    last_datas.to_sql("lh_plat_transfer", con=conn_rw, if_exists="append", index=False)
    logger.info("写入成功")


except:
    logger.exception(traceback.format_exc())
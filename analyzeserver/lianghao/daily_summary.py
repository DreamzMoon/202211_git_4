# -*- coding: utf-8 -*-

# @Time : 2021/12/13 11:17

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : daily_summary.py


import sys
sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
from functools import reduce

dailybp = Blueprint('daily', __name__, url_prefix='/lh/daily')

'''平台每日订单数据统计报表'''
@dailybp.route("plat",methods=["POST"])
def daily_plat_summary():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)


        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        buy_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) buy_order_count,sum(count) buy_lh_count,sum(total_price) buy_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0 group by statistic_time having statistic_time != CURRENT_DATE order by statistic_time desc'''
        sell_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) sell_order_count,sum(count) sell_lh_count,sum(total_price) sell_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0 group by statistic_time having statistic_time != CURRENT_DATE order by statistic_time desc'''
        public_sql = '''select DATE_FORMAT(lh_sell.create_time,"%Y-%m-%d") statistic_time,count(*) public_order_count,sum(lh_sell.count) public_lh_count,sum(lh_sell.total_price) public_total_price,sum(sell_fee) total_sell_fee,(sum(lh_sell.total_price)-sum(sell_fee)) total_real_money from lh_sell
        left join lh_order on lh_sell.id = lh_order.sell_id
        where lh_sell.del_flag = 0 and lh_sell.`status`!=1 and lh_order.del_flag = 0
        group by statistic_time  having statistic_time != CURRENT_DATE
        order by statistic_time desc'''

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
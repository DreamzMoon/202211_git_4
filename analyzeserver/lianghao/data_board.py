# -*- coding: utf-8 -*-

# @Time : 2022/3/7 17:19

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : data_board.py


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
import json

lhpersonboardbp = Blueprint('lhpersonboard', __name__, url_prefix='')

@lhpersonboardbp.route("lh/personboard/sell",methods=["GET"])
def lh_personboard_sell():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_analyze:
            return {"code": 10001, "status": "failed", "msg": message["10001"]}

        request_headers = request.headers
        logger.info(request_headers)
        token = request_headers["Token"]
        user_id = request.args.get("user_id")

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        # 先查询看板时间
        kanban_sql = '''select status,time_type,start_time,end_time,inside_publish_phone,inside_recovery_phone from data_board_settings where del_flag = 0 and market_type = 1'''
        kanban_data = pd.read_sql(kanban_sql, conn_analyze).to_dict("records")
        logger.info(kanban_data)
        if kanban_data[0]["status"] == 0:
            return {"code": "11036", "status": "failed", "msg": message["11036"]}

        # 上架总数
        sell_sum_count_sql = '''select sum(count) sell_count from lh_sell where `status` != 1 and del_flag = 0'''

        # 上架价值
        sell_sum_price_sql = '''select sum(total_price) sell_total_price from lh_sell where `status` != 1 and del_flag = 0'''

        # 内部渠道上架总数
        inside_sell_count_sql = '''select sum(count) inside_sell_count from lh_sell where `status` != 1 and del_flag = 0 and sell_phone in (%s)''' % (
        kanban_data[0]["inside_publish_phone"][1:-1])
        # 内部渠道商家价值
        inside_sell_price_sql = '''select sum(total_price) inside_sell_total_price from lh_sell where `status` != 1 and del_flag = 0 and sell_phone in (%s)''' % (
        kanban_data[0]["inside_publish_phone"][1:-1])

        # 判断是否有官方号
        if kanban_data[0]["inside_publish_phone"][1:-1]:
            inside_sell_count_sql = inside_sell_count_sql
            inside_sell_price_sql = inside_sell_price_sql
        else:
            inside_sell_count = 0
            inside_sell_total_price = 0

        sell_count = pd.read_sql(sell_sum_count_sql, conn_lh).to_dict("records")[0]["sell_count"]
        sell_count = 0 if sell_count is None else sell_count

        sell_total_price = pd.read_sql(sell_sum_price_sql, conn_lh).to_dict("records")[0]["sell_total_price"]
        sell_total_price = 0 if sell_total_price is None else sell_total_price

        if kanban_data[0]["inside_publish_phone"][1:-1]:
            inside_sell_count = pd.read_sql(inside_sell_count_sql, conn_lh).to_dict("records")[0]["inside_sell_count"]
            inside_sell_count = 0 if inside_sell_count is None else inside_sell_count

            inside_sell_total_price = pd.read_sql(inside_sell_price_sql, conn_lh).to_dict("records")[0][
                "inside_sell_total_price"]
            inside_sell_total_price = 0 if inside_sell_total_price is None else inside_sell_total_price

        user_sell_count = sell_count - inside_sell_count
        user_sell_total_price = sell_total_price - inside_sell_total_price

        # 上架采购总数 上架采购总数价值 内部渠道上架回收 内部渠道上架回收 用户上架采购 用户上架采购价值
        order_sum_count_sql = '''select sum(count) order_sum_count from lh_order where del_flag = 0 and type in (1,4) and status = 1'''
        order_sum_price_sql = '''select sum(total_price) order_sum_price from lh_order where del_flag = 0 and type in (1,4) and status = 1'''
        inside_order_count_sql = '''select sum(count) inside_order_count from lh_order where del_flag = 0 and type in (1,4) and status = 1 and phone in (%s)''' % (
        kanban_data[0]["inside_publish_phone"][1:-1])
        inside_order_price_sql = '''select sum(total_price) inside_order_price from lh_order where del_flag = 0 and type in (1,4) and status = 1 and phone in (%s)''' % (
        kanban_data[0]["inside_publish_phone"][1:-1])

        # 判断是否有官方号
        if kanban_data[0]["inside_publish_phone"][1:-1]:
            inside_order_count_sql = inside_order_count_sql
            inside_order_price_sql = inside_order_price_sql
        else:
            inside_order_count = 0
            inside_order_price = 0

        if kanban_data[0]["time_type"] == 0:
            order_sum_count_sql = order_sum_count_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            order_sum_price_sql = order_sum_price_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            inside_order_count_sql = inside_order_count_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            inside_order_price_sql = inside_order_price_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
        else:
            order_sum_count_sql = order_sum_count_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            order_sum_price_sql = order_sum_price_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            inside_order_count_sql = inside_order_count_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            inside_order_price_sql = inside_order_price_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])

        order_sum_count = pd.read_sql(order_sum_count_sql, conn_lh).to_dict("records")[0]["order_sum_count"]
        order_sum_price = pd.read_sql(order_sum_price_sql, conn_lh).to_dict("records")[0]["order_sum_price"]

        if kanban_data[0]["inside_publish_phone"][1:-1]:
            inside_order_count = pd.read_sql(inside_order_count_sql, conn_lh).to_dict("records")[0][
                "inside_order_count"]
            inside_order_price = pd.read_sql(inside_order_price_sql, conn_lh).to_dict("records")[0][
                "inside_order_price"]

        order_sum_count = 0 if order_sum_count is None else order_sum_count
        order_sum_price = 0 if order_sum_price is None else order_sum_price
        inside_order_count = 0 if inside_order_count is None else inside_order_count
        inside_order_price = 0 if inside_order_price is None else inside_order_price
        user_order_count = order_sum_count - inside_order_count
        user_order_price = order_sum_price - inside_order_price

        # 在售 在售剩余总数 在售剩余总数价值 内部渠道在售剩余总数 内部渠道在售剩余价值 用户在售剩余总数 用户在售剩余价值

        # 上架总数
        surplus_sell_count_sql = '''select sum(count) on_sell_count from lh_sell where `status` = 0 and del_flag = 0'''

        # 上架价值
        surplus_sell_price_sql = '''select sum(total_price) on_sell_total_price from lh_sell where `status` = 0 and del_flag = 0'''

        # 内部渠道上架总数
        surplus_inside_sell_count_sql = '''select sum(count) on_inside_sell_count from lh_sell where `status` = 0 and del_flag = 0 and sell_phone in (%s)''' % (
        kanban_data[0]["inside_publish_phone"][1:-1])
        # 内部渠道商家价值
        surplus_inside_sell_price_sql = '''select sum(total_price) on_inside_sell_total_price from lh_sell where `status` = 0 and del_flag = 0 and sell_phone in (%s)''' % (
        kanban_data[0]["inside_publish_phone"][1:-1])

        # 判断是否有官方号
        if kanban_data[0]["inside_publish_phone"][1:-1]:
            surplus_inside_sell_count_sql = surplus_inside_sell_count_sql
            surplus_inside_sell_price_sql = surplus_inside_sell_price_sql
        else:
            surplus_inside_sell_count = 0
            surplus_inside_sell_price = 0

        # 0是当前时间 1是开始和结束时间
        if kanban_data[0]["time_type"] == 0:
            surplus_sell_count_sql = surplus_sell_count_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            surplus_sell_price_sql = surplus_sell_price_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            surplus_inside_sell_count_sql = surplus_inside_sell_count_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            surplus_inside_sell_price_sql = surplus_inside_sell_price_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
        else:
            surplus_sell_count_sql = surplus_sell_count_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            surplus_sell_price_sql = surplus_sell_price_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            surplus_inside_sell_count_sql = surplus_inside_sell_count_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            surplus_inside_sell_price_sql = surplus_inside_sell_price_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(
                kanban_data[0]["start_time"], kanban_data[0]["end_time"])

        surplus_sell_count = pd.read_sql(surplus_sell_count_sql, conn_lh).to_dict("records")[0]["on_sell_count"]
        surplus_sell_price = pd.read_sql(surplus_sell_price_sql, conn_lh).to_dict("records")[0]["on_sell_total_price"]

        if kanban_data[0]["inside_publish_phone"][1:-1]:
            surplus_inside_sell_count = pd.read_sql(surplus_inside_sell_count_sql, conn_lh).to_dict("records")[0][
                "on_inside_sell_count"]
            surplus_inside_sell_price = pd.read_sql(surplus_inside_sell_price_sql, conn_lh).to_dict("records")[0][
                "on_inside_sell_total_price"]

        surplus_sell_count = 0 if surplus_sell_count is None else surplus_sell_count
        surplus_sell_price = 0 if surplus_sell_price is None else surplus_sell_price
        surplus_inside_sell_count = 0 if surplus_inside_sell_count is None else surplus_inside_sell_count
        surplus_inside_sell_price = 0 if surplus_inside_sell_price is None else surplus_inside_sell_price
        surplus_user_sell_count = surplus_sell_count - surplus_inside_sell_count
        surplus_user_sell_price = surplus_sell_price - surplus_inside_sell_price

        # 最早上架时间
        early_sql = '''select create_time from lh_sell where `status` != 1 and del_flag = 0 and sell_phone not in (%s)''' % (kanban_data[0]["inside_publish_phone"][1:-1])
        xj_sql = '''select sum(total_price) xj_total_price from lh_order where pay_type in (3,4) and type in (1,4) and del_flag = 0 and `status` =1 and phone not in (%s)''' % (kanban_data[0]["inside_publish_phone"][1:-1])
        clt_sql = '''select sum(total_price) clt_total_price from lh_order where pay_type = 2 and type in (1,4) and del_flag = 0 and `status` =1  and phone not in (%s)''' % (kanban_data[0]["inside_publish_phone"][1:-1])
        sell_fee_sql = '''select sum(sell_fee) total_sell_fee from lh_order where  type in (1,4) and del_flag = 0 and `status` =1 and phone not in (%s)''' % (kanban_data[0]["inside_publish_phone"][1:-1])
        cgj_sql = '''select sum(total_price) total_purchase_money from lh_order where pay_type = 9 and del_flag = 0 and type in (1,4) and phone not in (%s)''' % (
        kanban_data[0]["inside_publish_phone"][1:-1])
        # 判断是否有官方号
        if kanban_data[0]["inside_publish_phone"][1:-1]:
            pass
        else:
            early_time = 0
            xj_total_price = 0
            clt_total_price = 0
            total_sell_fee = 0
            total_purchase_money = 0

        if kanban_data[0]["time_type"] == 0:
            early_sql = early_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            xj_sql = xj_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            clt_sql = clt_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            sell_fee_sql = sell_fee_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            cgj_sql = cgj_sql + ''' and DATE_FORMAT(create_time,"%Y-%m-%d") =  CURRENT_DATE() '''
        else:
            early_sql = early_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            xj_sql = xj_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            clt_sql = clt_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format( kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            sell_fee_sql = sell_fee_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(kanban_data[0]["start_time"], kanban_data[0]["end_time"])
            cgj_sql = cgj_sql + ''' and create_time>= "{}" and create_time <= "{}" '''.format(kanban_data[0]["start_time"], kanban_data[0]["end_time"])
        early_sql = early_sql + " order by create_time asc limit 1"
        logger.info(early_sql)

        if kanban_data[0]["inside_publish_phone"][1:-1]:
            early_time = pd.read_sql(early_sql, conn_lh).to_dict("records")[0]["create_time"]
            early_time = datetime.datetime.strftime(early_time, "%Y-%m-%d %H:%M:%S")
            xj_total_price = pd.read_sql(xj_sql, conn_lh).to_dict("records")[0]["xj_total_price"]
            clt_total_price = pd.read_sql(clt_sql, conn_lh).to_dict("records")[0]["clt_total_price"]

            total_sell_fee = pd.read_sql(sell_fee_sql, conn_lh).to_dict("records")[0]["total_sell_fee"]
            logger.info(cgj_sql)
            total_purchase_money = pd.read_sql(cgj_sql, conn_lh).to_dict("records")[0]["total_purchase_money"]

        # pure_money = user_sell_total_price + inside_sell_total_price - user_order_price - inside_order_price
        # pure_money = user_sell_total_price - user_order_price

        #净营收
        pure_money = user_order_price - inside_order_price

        msg = {"sell_count": sell_count, "sell_total_price": sell_total_price, "inside_sell_count": inside_sell_count,
               "inside_sell_total_price": inside_sell_total_price, "user_sell_count": user_sell_count,
               "user_sell_total_price": user_sell_total_price,
               "order_sum_count": order_sum_count, "order_sum_price": order_sum_price,
               "inside_order_count": inside_order_count,
               "inside_order_price": inside_order_price, "user_order_count": user_order_count,
               "user_order_price": user_order_price,
               "surplus_sell_count": surplus_sell_count, "surplus_sell_price": surplus_sell_price,
               "surplus_inside_sell_count": surplus_inside_sell_count,
               "surplus_inside_sell_price": surplus_inside_sell_price,
               "surplus_user_sell_count": surplus_user_sell_count, "surplus_user_sell_price": surplus_user_sell_price,
               "early_time": early_time, "xj_total_price": xj_total_price, "pure_money": pure_money,
               "clt_total_price": clt_total_price, "total_purchase_money": total_purchase_money,
               "total_sell_fee": total_sell_fee,
               "time_type": kanban_data[0]["time_type"], "start_time": kanban_data[0]["start_time"],
               "end_time": kanban_data[0]["end_time"]
               }
        if msg["start_time"]:
            msg["start_time"] = datetime.datetime.strftime(msg["start_time"], '%Y-%m-%d %H:%M:%S')
        if msg["end_time"]:
            msg["end_time"] = datetime.datetime.strftime(msg["end_time"], '%Y-%m-%d %H:%M:%S')
        logger.info(msg)
        return {"code": "0000", "status": "success", "msg": msg}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
        conn_analyze.close()
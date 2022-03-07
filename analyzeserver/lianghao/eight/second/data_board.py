# -*- coding: utf-8 -*-

# @Time : 2022/3/7 10:01

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

boardsecondbp = Blueprint('boardsecond', __name__, url_prefix='')

@boardsecondbp.route("board/see",methods=["GET"])
def board_see():

    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": 10001, "status": "failed", "msg": message["10001"]}
        kanban_sql = '''select market_type,status from data_board_settings where del_flag = 0'''
        kanban_data = pd.read_sql(kanban_sql, conn_analyze).to_dict("records")
        return_data = {}
        return_data["kanban_status"] = kanban_data
        return {"code": "0000", "status": "success", "msg": return_data}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()

'''上架总数 上架总数价值 内部渠道上架总数 内部渠道上架价值 用户上架总数 用户上架价值'''
@boardsecondbp.route("le/secboard/sell",methods=["GET"])
def le_secboard_sell():
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

        #先查询看板时间
        kanban_sql = '''select status,time_type,start_time,end_time,inside_publish_phone,inside_recovery_phone from data_board_settings where del_flag = 0 and market_type = 2'''
        kanban_data = pd.read_sql(kanban_sql,conn_analyze).to_dict("records")
        logger.info(kanban_data)
        if kanban_data[0]["status"] == 0:
            return {"code": "11036", "status": "failed", "msg": message["11036"]}

        #上架总数
        sell_sum_count_sql = '''select count(*) sell_count from le_second_hand_sell where `status` != 1 and del_flag = 0'''

        #上架价值
        sell_sum_price_sql = '''select sum(total_price) sell_total_price from le_second_hand_sell where `status` != 1 and del_flag = 0'''

        #内部渠道上架总数
        inside_sell_count_sql = '''select count(*) inside_sell_count from le_second_hand_sell where `status` != 1 and del_flag = 0 and sell_phone in (%s)''' %(kanban_data[0]["inside_publish_phone"][1:-1])

        #内部渠道商家价值
        inside_sell_price_sql = '''select sum(total_price) inside_sell_total_price from le_second_hand_sell where `status` != 1 and del_flag = 0 and sell_phone in (%s)''' % (kanban_data[0]["inside_publish_phone"][1:-1])

        # 0是当前时间 1是开始和结束时间
        if kanban_data[0]["time_type"] == 0:
            sell_sum_count_sql = sell_sum_count_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            sell_sum_price_sql = sell_sum_price_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            inside_sell_count_sql = inside_sell_count_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
            inside_sell_price_sql = inside_sell_price_sql + ''' and DATE_FORMAT(up_time,"%Y-%m-%d") =  CURRENT_DATE() '''
        else:
            sell_sum_count_sql = sell_sum_count_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(kanban_data[0]["start_time"],kanban_data[0]["end_time"])
            sell_sum_price_sql = sell_sum_price_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(kanban_data[0]["start_time"],kanban_data[0]["end_time"])
            inside_sell_count_sql = inside_sell_count_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(kanban_data[0]["start_time"],kanban_data[0]["end_time"])
            inside_sell_price_sql = inside_sell_price_sql + ''' and up_time>= "{}" and up_time <= "{}" '''.format(kanban_data[0]["start_time"],kanban_data[0]["end_time"])

        sell_count = pd.read_sql(sell_sum_count_sql,conn_lh).to_dict("records")[0]["sell_count"]
        sell_total_price = pd.read_sql(sell_sum_price_sql,conn_lh).to_dict("records")[0]["sell_total_price"]
        sell_total_price = 0 if sell_total_price is None else sell_total_price
        inside_sell_count = pd.read_sql(inside_sell_count_sql,conn_lh).to_dict("records")[0]["inside_sell_count"]
        inside_sell_total_price = pd.read_sql(inside_sell_price_sql,conn_lh).to_dict("records")[0]["inside_sell_total_price"]
        inside_sell_total_price = 0 if inside_sell_total_price is None else inside_sell_total_price
        user_sell_count = sell_count - inside_sell_count
        user_sell_total_price = sell_total_price - inside_sell_total_price

        #采购的



        msg={"sell_count":sell_count,"sell_total_price":sell_total_price,"inside_sell_count":inside_sell_count,"inside_sell_total_price":inside_sell_total_price,
             "user_sell_count":user_sell_count,"user_sell_total_price":user_sell_total_price}
        logger.info(msg)
        return {"code":"0000","status":"success","msg":msg}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
        conn_analyze.close()
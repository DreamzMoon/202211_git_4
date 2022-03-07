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
        kanban_sql = '''select status,time_type,start_time,end_time from data_board_settings where del_flag = 0 and market_type'''
        kanban_data = pd.read_sql(kanban_sql,conn_analyze).to_dict("records")



    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
        conn_analyze.close()
# -*- coding: utf-8 -*-

# @Time : 2021/11/16 17:11

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : home_page.py

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


homebp = Blueprint('homepage', __name__, url_prefix='/home')

@homebp.route("deal/person",methods=["GET"])
def deal():
    try:
        try:
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        return {"1"}
        # conn_lh = direct_get_conn(lianghao_mysql_conf)
        # logger.info(conn_lh)
        # cursor_lh = conn_lh.cursor()
        # sql = '''select nick_name,phone,sum(total_price) total_money from lh_order where del_flag = 0 and `status`=1 and type in (1,4) and DATE_FORMAT(create_time,"%Y%m%d") =CURRENT_DATE() group by phone order by total_money desc limit 3'''
        # logger.info(sql)
        # datas = pd.read_sql(sql,conn_lh)
        # logger.info(datas)

    except Exception as e:
        logger.error(e)
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    # finally:
        # conn_lh.close()


@homebp.route("/today/data", methods=["POST"])
def today_data():
    try:
        logger.info(request.json)
        # 参数个数错误
        if len(request.json) != 2:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        # 1今日 2昨日 3自定义-->必须传起始和结束时间
        time_type = request.json['time_type']
        # 首次发布时间
        start_time = request.json['start_time']
        end_time = request.json['end_time']

        if (time_type != 3 and start_time and end_time) or time_type not in range(1, 3) or (
                time_type == 3 and not start_time and not end_time):
            return {"code": "10014", "status": "failed", "msg": message["10014"]}
        # 时间判断
        elif start_time or end_time:
            judge_result = judge_start_and_end_time(start_time, end_time)
            if not judge_result[0]:
                return {"code": judge_result[1], "status": "failed", "msg": message[judge_result[1]]}
            sub_day = judge_result[1] - judge_result[0]
            if sub_day.days + sub_day.seconds / (24.0 * 60.0 * 60.0) > 30:
                return {"code": "11018", "status": "failed", "msg": message["11018"]}
            request.json['start_time'] = judge_result[0]
            request.json['end_time'] = judge_result[1]

    except Exception as e:
        # 参数名错误
        logger.error(e)
        return {"code": "10009", "status": "failed", "msg": message["10009"]}

    pass
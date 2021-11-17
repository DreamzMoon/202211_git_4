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
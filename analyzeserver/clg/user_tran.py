# -*- coding: utf-8 -*-
# @Time : 2022/2/11  16:15
# @Author : shihong
# @File : .py
# --------------------------------------
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


clgtranuserbp = Blueprint('clgtranuser', __name__, url_prefix='/clgtranuser')

@clgtranuserbp.route("/data", methods=["POST"])
def clg_user_tran():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 6:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            keyword = request.json['keyword']
            start_time = request.json["start_time"]
            end_time = request.json["end_time"]
            page = request.json['page']
            size = request.json['size']
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_clg = direct_get_conn(clg_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze or not conn_clg:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        cursor_analyze = conn_analyze.cursor()
        cursor_clg = conn_clg.cursor()

    except:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
            conn_analyze.close()
        except:
            pass

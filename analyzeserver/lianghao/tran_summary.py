# -*- coding: utf-8 -*-
# @Time : 2022/3/1  20:30
# @Author : shihong
# @File : .py
# --------------------------------------
'''个人名片网转让数据列表汇总'''
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime
from analyzeserver.user.sysuser import check_token

transummarybp = Blueprint('transummary', __name__, url_prefix='/lh/transummary')

@transummarybp.route('/user', methods=["POST"])
def user_summary():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 8:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            keyword = request.json('keyword') # 关键词
            operate_id = request.json['operate_id'] # 归属运营中心
            parent = request.json['parent'] # 归属上级
            login_start_time = request.json['login_start_time'] # 最近登录起始时间
            login_end_time = request.json['login_end_time'] # 最近登录结束时间
            time_type = request.json['time_type'] # 时间类型
            start_time = request.json['start_time'] # 起始时间
            end_time = request.json['end_time'] # 结束时间
            page = request.json['page']
            size = request.json['size']
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}



        return {"code": "0000", "status": "success", "msg": '更新成功'}
    except:
        conn_analyze.rollback()
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass
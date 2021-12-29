# -*- coding: utf-8 -*-

# @Time : 2021/12/29 15:03

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : system_log.py
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from analyzeserver.common import *
from config import *
import traceback
from util.help_fun import *
import pandas as pd
from analyzeserver.user.sysuser import check_token


syslogbp = Blueprint('syslog', __name__, url_prefix='/sys/log')

@syslogbp.route("mes",methods=["POST"])
def sys_log():
    try:
        conn = direct_get_conn(analyze_mysql_conf)
        cursor = conn.cursor()
        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        page = request.json.get("page")
        size = request.json.get("size")

        count_sql = '''select count(*) count from sys_log'''
        cursor.execute(count_sql)
        count = cursor.fetchone()[0]

        sql = '''select sys_user.username,sys_log.log_time,sys_log.log_action,sys_log.remark from sys_log left join sys_user on sys_log.user_id = sys_user.id order by log_time desc limit %s,%s''' %((page-1)*size,page*size)
        log_datas = pd.read_sql(sql,conn)
        log_datas = log_datas.to_dict("records")
        for log_data in log_datas:
            log_data["log_time"] = log_data["log_time"].strftime('%Y-%m-%d %H:%M:%S')
        return {"code":"0000","msg":log_datas,"count":count,"status":"success"}

    except Exception as e:
        logger.error(e)
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
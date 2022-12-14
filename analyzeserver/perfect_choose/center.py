# -*- coding: utf-8 -*-

# @Time : 2021/11/3 15:11

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : center.py

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

ocbp = Blueprint('operatecenter', __name__, url_prefix='/operations')

# 支持crm的
@ocbp.route("/center/list",methods=["GET"])
def center_list():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        token = request.headers["Token"]
        user_id = request.args.get("user_id")

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        with conn_analyze.cursor() as cursor:
            # sql = '''select id,operatename from luke_lukebus.operationcenter where capacity = 1 and crm = 1'''
            sql = '''
                select id, operatename from
                (
                    select id,operatename, CONVERT(left(trim(operatename), 1) using gbk) sort_type from operationcenter where crm = 1{}
                    order by sort_type asc
                )t1
            '''
            if check_close_operate:
                check_sql = ''
            else:
                check_sql = ' and status=1'
            sql = sql.format(check_sql)
            cursor.execute(sql)
            datas = cursor.fetchall()
            datas = pd.DataFrame(datas)
            datas.columns=["id","operatename"]
            # logger.info(len(datas))
            datas = datas.to_dict("records")
            logger.info(datas)
            return {"code":"0000","status":"success","msg":datas}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()

# 运营中心管理--运营中心状态
@ocbp.route("/center/status/list",methods=["GET"])
def center_status_list():
    try:
        center_status_list = [
            {
                "status_id": 1,
                "status": "正常"
            },
            {
                "status_id": 2,
                "status": "关闭"
            }
        ]
        crm_status_list = [
            {
                "status_id": 1,
                "status": "支持"
            },
            {
                "status_id": 0,
                "status": "不支持"
            }
        ]
        return_data = {
            "center_status_list": center_status_list,
            "crm_status_list": crm_status_list
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}





# 不支持crm的
@ocbp.route("bus/center/list",methods=["GET"])
def bus_center_list():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        token = request.headers["Token"]
        user_id = request.args.get("user_id")

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        with conn_analyze.cursor() as cursor:
            # sql = '''select id,operatename from luke_lukebus.operationcenter where capacity = 1 and crm = 1'''
            sql = '''
                select id, operatename from
                (
                    select id,operatename, CONVERT(left(trim(operatename), 1) using gbk) sort_type from operationcenter{}
                    order by sort_type asc
                )t1
            '''
            if check_close_operate:
                check_sql = ''
            else:
                check_sql = ' where status=1'
            sql = sql.format(check_sql)
            cursor.execute(sql)
            datas = cursor.fetchall()

            datas = pd.DataFrame(datas)
            datas.columns = ["id", "operatename"]
            datas = datas.to_dict("records")
            # logger.info(len(datas))
            logger.info(datas)
            return {"code":"0000","status":"success","msg":datas}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
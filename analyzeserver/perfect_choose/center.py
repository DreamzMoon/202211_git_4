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

@ocbp.route("/center/list",methods=["GET"])
def center_list():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)

        token = request.headers["Token"]
        user_id = request.args.get("user_id")

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        with conn_crm.cursor() as cursor:
            # sql = '''select id,operatename from luke_lukebus.operationcenter where capacity = 1 and crm = 1'''
            sql = '''
                select id, operatename from
                (
                    select id,operatename, CONVERT(left(trim(operatename), 1) using gbk) sort_type from luke_lukebus.operationcenter where capacity = 1 and crm = 1
                    order by sort_type asc
                )t1
            '''
            cursor.execute(sql)
            datas = cursor.fetchall()
            logger.info(datas)
            return {"code":"0000","status":"success","msg":datas}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_crm.close()
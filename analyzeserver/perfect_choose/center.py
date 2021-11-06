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

ocbp = Blueprint('operatecenter', __name__, url_prefix='/operations')

@ocbp.route("/center/list",methods=["GET"])
def center_list():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = '''select id,operatename from luke_lukebus.operationcenter'''
            cursor.execute(sql)
            datas = cursor.fetchall()
            logger.info(datas)
            return {"code":"0000","status":"success","msg":datas}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_crm.close()
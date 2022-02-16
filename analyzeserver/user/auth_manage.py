# -*- coding: utf-8 -*-
# @Time : 2022/2/15  14:05
# @Author : shihong
# @File : .py
# --------------------------------------

'''
                                                  用户权限
'''
import sys
sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
import base64
from util.help_fun import *
import time
import datetime
from datetime import timedelta
from analyzeserver.user.sysuser import check_token

userauthbp = Blueprint('userauth', __name__, url_prefix='/userauth')

# 查
@userauthbp.route("/check", methods=["GET"])
def check_sys_user():
    try:
        sys_user_sql = '''
            select id, username, phone, password, is_use, is_control_user, date_format(addtime, "%Y-%m-%d %H:%i:%S") addtime, date_format(uptime, "%Y-%m-%d %H:%i:%S") uptime from lh_analyze.sys_user
        '''
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        sys_user_data = pd.read_sql(sys_user_sql, conn_analyze)
        sys_user_data['password'] = sys_user_data['password'].apply(lambda x: base64.b64decode(x).decode(('utf-8')).split('okok')[0])
        return {"code": "0000", "status": "success", "msg": sys_user_data.to_dict("records")}
    except:
        logger.info(traceback.format_exc())
        return "失败"
    finally:
        try:
            conn_analyze.close()
        except:
            pass

# 改
@userauthbp.route("/update", methods=["POST"])
def update_user():
    pass
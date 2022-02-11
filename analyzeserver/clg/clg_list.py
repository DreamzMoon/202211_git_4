# -*- coding: utf-8 -*-

# @Time : 2022/2/10 17:16

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : clg_list.py

import sys

import pandas as pd

sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime
from datetime import timedelta
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
from analyzeserver.common import *
import threading
from functools import reduce

clglistbp = Blueprint('clglist', __name__, url_prefix='/clglist')

@clglistbp.route("/shop",methods=["GET"])
def clg_shop():
    try:
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        cursor_clg = conn_clg.cursor()
        try:
            logger.info("env:%s" % ENV)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}
        sql = '''
        select msi.id shop_id,msi.name shop_name from member_shop_info msi
        where msi.del_flag = 0 order by shop_id 
        '''
        # cursor_clg.execute(sql)
        # shop_data = cursor_clg.fetchall()

        shop_data = pd.read_sql(sql,conn_clg)
        shop_data = shop_data.to_dict("records")
        return {"code":"0000","status":"success","msg":shop_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()



@clglistbp.route("/shoptype",methods=["GET"])
def clg_shop_type():
    try:

        try:
            logger.info("env:%s" % ENV)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}


        type = [{"type_id":1,"type_name":"专营店"},{"type_id":2,"type_name":"普通店铺"}]

        return {"code":"0000","status":"success","msg":type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# -*- coding: utf-8 -*-

# @Time : 2022/2/25 13:40

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : clm_list.py

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

clmlisttbp = Blueprint('clmlist', __name__, url_prefix='/clmlist')

@clmlisttbp.route("shop",methods=["GET"])
def clg_shop():
    try:
        conn_clm = direct_get_conn(crm_mysql_conf)
        logger.info(conn_clm)
        cursor_clm = conn_clm.cursor()
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
        sql = '''
        select id shop_id,`name` from luke_marketing.shop where `name` != "" and `name` is not null
        order by shop_id
        '''
        shop_data = pd.read_sql(sql,conn_clm)
        shop_data = shop_data.to_dict("records")
        return {"code":"0000","status":"success","msg":shop_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clm.close()


@clmlisttbp.route("manage",methods=["GET"])
def manage():
    try:
        conn_clm = direct_get_conn(crm_mysql_conf)
        logger.info(conn_clm)
        cursor_clm = conn_clm.cursor()
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
        sql = '''
        select cate_id,cate_name from shop_category where is_del = 0 
        '''
        shop_data = pd.read_sql(sql,conn_clm)
        shop_data = shop_data.to_dict("records")
        return {"code":"0000","status":"success","msg":shop_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clm.close()


@clmlisttbp.route("shop/type",methods=["GET"])
def shop_type():
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

        shop_type = [{"id":1,"name":"运营店铺"},{"id":2,"name":"普通店铺"}]

        return {"code":"0000","status":"success","msg":shop_type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}



@clmlisttbp.route("wx/status",methods=["GET"])
def wx_status():
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

        type = [
                     {"id":0,"name":"待申请"},
                     {"id":1,"name":"审核中"},
                     {"id":2,"name":"已驳回"},
                     {"id":3,"name":"待账户验证"},
                     {"id":4,"name":"待签约"},
                     {"id":5,"name":"开通权限中"},
                     {"id":6,"name":"已完成"},
                     {"id":7,"name":"已作废"}
                     ]
        return {"code":"0000","status":"success","msg":type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}



@clmlisttbp.route("zfb/status",methods=["GET"])
def zfb_status():
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

        type = [
                     {"id":0,"name":"待申请"},
                     {"id":1,"name":"审核中"},
                     {"id":2,"name":"已驳回"},
                     {"id":3,"name":"待账户验证"},
                     {"id":4,"name":"待签约"},
                     {"id":5,"name":"开通权限中"},
                     {"id":6,"name":"已完成"},
                     {"id":7,"name":"已作废"}
                     ]
        return {"code":"0000","status":"success","msg":type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}


@clmlisttbp.route("shop/status",methods=["GET"])
def shop_status():
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

        type = [
                     {"id":1,"name":"正常"},
                     {"id":2,"name":"冻结"},
                     {"id":3,"name":"待审核"},
                     {"id":4,"name":"已拒绝"},
                     {"id":5,"name":"未申请"}

                     ]
        return {"code":"0000","status":"success","msg":type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 支付方式
@clmlisttbp.route("pay/status",methods=["GET"])
def pay_status():
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

        type = [
             {"id":0,"name":"未知"},
             {"id":1,"name":"微信支付"},
             {"id":2,"name":"支付宝支付"},
             {"id":3,"name":"余额支付"},
         ]
        return {"code":"0000","status":"success","msg":type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 订单状态
@clmlisttbp.route("order/status",methods=["GET"])
def order_status():
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

        type = [
             {"id":1,"name":"待支付"},
             {"id":2,"name":"已支付"},
             {"id":3,"name":"已退款"},
         ]
        return {"code":"0000","status":"success","msg":type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
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

# 店铺列表
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


#店铺类型
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


#订单状态
@clglistbp.route("/order/status",methods=["GET"])
def clgh_order_status():
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

        # 10/15都是完成状态

        order_type = [
            {"id":1,"order_status":"创建订单"},{"id":2,"order_status":"待付款"},
            {"id":3,"order_status":"支付中"},{"id":4,"order_status":"待发货"},
            {"id":5,"order_status":"待收货"},{"id":6,"order_status":"确认收货(待评价)"},
            {"id":7,"order_status":"交易关闭"},{"id":10,"order_status":"交易成功"},
            {"id":11,"order_status":"售后中(退换货)"},{"id":12,"order_status":"退款成功"},
            {"id":15,"order_status":"结算成功"}
                ]

        return {"code":"0000","status":"success","msg":order_type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}


@clglistbp.route("/pay/type",methods=["GET"])
def clg_pay_type():
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

        # 10/15都是完成状态

        pay_type = [
            {"id":1,"pay_type":"支付宝"},{"id":2,"pay_type":"微信"},
            {"id":3,"pay_type":"账户"},{"id":4,"pay_type":"微信内支付"},
            {"id":5,"pay_type":"全抵用金支付"},{"id":6,"pay_type":"支付宝APP支付"},
            {"id":7,"pay_type":"微信APP支付"}

                ]

        return {"code":"0000","status":"success","msg":pay_type}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
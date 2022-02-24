# -*- coding: utf-8 -*-

# @Time : 2022/2/24 18:36

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : transfer.py

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

clmtranbp = Blueprint('clmtran', __name__, url_prefix='/clmtran')


@clmtranbp.route("/all",methods=["POST"])
def clg_tran_shop_all():
    try:
        conn_clm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()
        try:
            logger.info("env:%s" % ENV)
            token = request.headers["Token"]
            user_id = request.json.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}



        #店铺信息
        shop_sql = '''select shop.id,shop.`name`,shop.phone,shop.types,sc.cate_name,shop.status from shop 
        left join shop_category sc on shop.cate_id = sc.cate_id'''
        shop_data = pd.read_sql(shop_sql, conn_clm)
        logger.info("店铺数据读取完成")
        serach_phone = list(set(shop_data["phone"].to_list()))
        crm_sql = '''select unionid,if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) nickname,phone from crm_user where del_flag = 0 and phone is not null and phone != "" and phone in (%s)''' % ",".join(serach_phone)
        crm_data = pd.read_sql(crm_sql, conn_analyze)
        logger.info("用户数据完成")

        #订单情况
        order_sql = '''select shop_id,pay_money,pay_status,pay_types,count(*) count from orders where is_del = 0
group by shop_id'''
        order_data = pd.read_sql(order_sql)




    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clm.close()
        conn_analyze.close()
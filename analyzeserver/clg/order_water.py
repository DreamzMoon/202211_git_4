# -*- coding: utf-8 -*-

# @Time : 2022/2/11 16:18

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : order_water.py

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

clgorderdbp = Blueprint('clgorder', __name__, url_prefix='/clgorder')


@clgorderdbp.route("/flow",methods=["POST"])
def clg_tran_good_all():
    try:
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()
        try:

            token = request.headers["Token"]
            user_id = request.json.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        shop_id = request.json.get("shop_id")
        order_status = request.json.get("order_status")

        sql = '''

        select o.order_sn,"诚聊购订单" order_source,goods_id,goods_name,goods_sku_name,goods_price,buy_num,shop_id,shop_name,carrier_id,order_commission,phone,consignee_mobile,consignee_name,CONCAT(ifnull(consignee_country,""),IFNULL(consignee_province,""),IFNULL(consignee_city,""),IFNULL(consignee_county,""),ifnull(consignee_town,""),IFNULL(consignee_address,"")) address,if(o.voucherMoneyType=1,pay_money,voucherPayMoney) pay_money,if(o.voucherMoneyType=1,0,voucherMoney) voucherMoney,item_freight_money,order_status,o.create_time,ob.pay_type from trade_order_info o

        left join trade_order_item od on o.order_sn = od.order_sn
        left join trade_pay_bill ob on o.pay_sn = ob.id
        where o.del_flag = 0
        '''


        condition = []
        if shop_id:
            sql = sql + ''' and shop_id = %s''' %shop_id





        # user_sql = '''select if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username,phone,unionid from crm_user where del_flag = 0 and phone != "" and phone is not null'''
        # crm_user = pd.read_sql(user_sql)




        return "1"
        # return {"code":"0000","status":"success","data":data,"count":count}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

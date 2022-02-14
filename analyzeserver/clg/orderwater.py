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
        logger.info(request.json)
        shop_id = request.json.get("shop_id")
        order_status = request.json.get("order_status")
        pay_type = request.json.get("pay_type")
        recv_keyword = request.json.get("recv_keyword")
        order_sn = request.json.get("order_sn")
        goods_keyword = request.json.get("goods_keyword")
        # 传年月日时分秒
        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")

        buy_keyword = request.json.get("buy_keyword")
        carry_keyword = request.json.get("carry_keyword")

        page = request.json.get("page")
        size = request.json.get("size")


        sql = '''


        select o.order_sn,"诚聊购订单" order_source,goods_id,goods_name,goods_sku_name,goods_price,buy_num,shop_id,shop_name,carrier_id carry_unionid,order_commission,phone,consignee_mobile,consignee_name,CONCAT(ifnull(consignee_country,""),IFNULL(consignee_province,""),IFNULL(consignee_city,""),IFNULL(consignee_county,""),ifnull(consignee_town,""),IFNULL(consignee_address,"")) address,if(o.voucherMoneyType=1,pay_money,voucherPayMoney) pay_money,if(o.voucherMoneyType=1,0,voucherMoney) voucherMoney,item_freight_money,order_status,o.create_time,ob.pay_type from trade_order_info o


        left join trade_order_item od on o.order_sn = od.order_sn
        left join trade_pay_bill ob on o.pay_sn = ob.id
        where o.del_flag = 0
        '''

        if shop_id:
            sql = sql + ''' and shop_id = %s ''' %shop_id
        if order_status:
            sql = sql + ''' and order_status = %s ''' %order_status
        if pay_type:
            sql = sql + ''' and ob.pay_type = %s ''' %pay_type
        if recv_keyword:
            sql = sql + ''' and (consignee_mobile like "%%%s%%" or consignee_name like "%%%s%%") ''' %(recv_keyword,recv_keyword)
        if order_sn:
            sql = sql + ''' and o.order_sn = "%s" ''' %order_sn
        if goods_keyword:
            sql = sql + ''' and (goods_id like "%%%s%%" or goods_name like "%%%s%%") ''' %(goods_keyword,goods_keyword)
        if start_time and end_time:
            sql = sql + ''' and o.create_time >="%s" and o.create_time<="%s" ''' % (start_time, end_time)
        logger.info(sql)
        order_data = pd.read_sql(sql,conn_clg)
        # order_data["unionid"] = order_data["unionid"].astype("str")
        if order_data.empty:
            return {"code":"0000","status":"success","msg":[],"count":0}

        user_sql = '''select if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username,phone,unionid from crm_user where del_flag = 0 and phone != "" and phone is not null'''
        crm_user = pd.read_sql(user_sql,conn_analyze)
        crm_user["unionid"] = crm_user["unionid"].astype("object")

        user_data = crm_user.rename(columns={"username":"username","phone":"phone","unionid":"buy_unionid"})
        order_data = order_data.merge(user_data,how="left",on="phone")
        crm_user.rename(columns={"username":"carry_username","phone":"carry_phone","unionid":"carry_unionid"},inplace=True)
        order_data = order_data.merge(crm_user,how="left",on="carry_unionid")


        if buy_keyword:
            order_data = order_data[(order_data["username"].str.contains(buy_keyword))|(order_data["phone"].str.contains(buy_keyword))]
        if carry_keyword:
            order_data = order_data[(order_data["carry_username"].str.contains(carry_keyword))|(order_data["carry_phone"].str.contains(carry_keyword))]

        count = order_data.shape[0]

        order_data['create_time'] = pd.to_datetime(order_data['create_time'])
        order_data['create_time'] = order_data['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        order_data.sort_values(by=["create_time", "buy_num", "pay_money"], ascending=[False, False, False], inplace=True)

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size
            order_data = order_data[code_page:code_size]


        order_data.fillna("",inplace=True)
        order_data = order_data.to_dict("records")



        return {"code":"0000","status":"success","data":order_data,"count":count}


    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

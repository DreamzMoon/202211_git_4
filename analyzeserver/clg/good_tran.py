# -*- coding: utf-8 -*-

# @Time : 2022/2/11 13:56

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : good_tran.py


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

clgtrangoodbp = Blueprint('clgtrangood', __name__, url_prefix='/clgtrangood')


@clgtrangoodbp.route("/all",methods=["POST"])
def clg_tran_good_all():
    try:
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
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

        page = request.json.get("page")
        size = request.json.get("size")
        keyword = request.json.get("keyword")
        # 只要年月日
        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")

        shop_id = request.json.get("shop_id")
        shoptype = request.json.get("shoptype")

        #店铺信息
        shop_sql = '''select msi.id shop_id,msi.`name` shop_name,ggi.goods_id,ggi.goods_name,msi.phone from member_shop_info msi 
        left join goods_goods_info ggi on msi.id = ggi.shop_id
        where msi.del_flag = 0 and ggi.del_flag = 0 '''
        if shop_id:
            shop_sql = shop_sql + " and msi.id = %s" %shop_id
            logger.info("shopid")
        if shoptype:
            shop_sql = shop_sql + " and shoptype = %s" %shoptype

        shop_data = pd.read_sql(shop_sql,conn_clg)
        logger.info("店铺数据读取完成")

        crm_sql = '''select unionid,if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) nickname,phone from crm_user where del_flag = 0 and phone is not null and phone != ""'''
        crm_data = pd.read_sql(crm_sql,conn_analyze)
        logger.info("用户数据完成")

        #交易订单
        order_sql = '''
        select shop_id,goods_id,sum(count) count,sum(buy_num) buy_num,sum(pay_money) pay_money,sum(voucherMoney) voucherMoney,order_status from (
        select shop_id,count(*) count,sum(buy_num) buy_num,
        if(toi.voucherMoneyType = 1,sum(pay_money),sum(voucherPayMoney)) pay_money,
        if(toi.voucherMoneyType = 1,0,sum(voucherMoney)) voucherMoney,order_status,goods_id from trade_order_info toi
        left join trade_order_item tod on toi.order_sn = tod.order_sn 
        where toi.del_flag = 0 
        '''
        order_group_sql = ''' group by shop_id,order_status,toi.voucherMoneyType,goods_id ) t group by shop_id,order_status,goods_id '''


        condition = []
        if start_time and end_time:
            # condition.append(''' and date_format(toi.create_time,"%%Y-%%m-%%d")>="%s" and date_format(toi.create_time,"%%Y-%%m-%%d")<="%s" ''' %(start_time,end_time))
            condition.append(''' and toi.create_time >="%s" and toi.create_time<="%s" ''' %(start_time,end_time))
        if shop_id:
            condition.append(''' and shop_id = %s ''' %shop_id)

        if condition:
            for i in range(0,len(condition)):
                order_sql = order_sql + condition[i]
        order_sql = order_sql + order_group_sql
        logger.info(order_sql)

        #总订单
        tran_order = pd.read_sql(order_sql,conn_clg)
        logger.info(tran_order)
        logger.info("订单数据读取完成")

        #下面由于pandas本本问题
        #交易订单 交易订单数量 交易商品数量 交易金额 交易抵用金
        tran_data = tran_order.copy()
        tran_data.drop(["order_status"], axis=1, inplace=True)
        tran_data = tran_order.groupby(["shop_id","goods_id"]).agg({"count":"sum","buy_num":"sum","pay_money":"sum","voucherMoney":"sum"}).rename(columns=
            {"count":"tran_count","buy_num":"tran_buy_count","pay_money":"tran_pay","voucherMoney":"tran_voucher"}).reset_index()
        if "shop_id" not in tran_data:
            tran_data["shop_id"] = ""
        if "goods_id" not in tran_data:
            tran_data["goods_id"] = ""
        if "index" in tran_data:
            logger.info("indexindex")
            tran_data.drop(["index"],inplace=True,axis=1)

        logger.info(tran_data)
        logger.info("交易订单数据处理完成")


        # 有效订单 有效订单 有效金额 有效抵用金
        ok_order = tran_order[tran_order["order_status"].isin([3,4,5,6,10,15])]
        ok_order.drop(["buy_num","order_status"],axis=1,inplace=True)
        yes_data = ok_order.groupby(["shop_id","goods_id"]).agg(
            {"count": "sum",  "pay_money": "sum", "voucherMoney": "sum"}).rename(columns=
            {"count": "ok_count", "pay_money": "ok_pay",
             "voucherMoney": "ok_voucher"}).reset_index()
        if "shop_id" not in yes_data:
            yes_data["shop_id"] = ""
        if "goods_id" not in yes_data:
            yes_data["goods_id"] = ""
        if "index" in yes_data:
            yes_data.drop(["index"],inplace=True,axis=1)
        logger.info(yes_data)
        logger.info("有效订单数据处理完成")

        # 已退款订单 已退款订单 已退款金额 已退款抵用金
        refund_order = tran_order[tran_order["order_status"].isin([12])]
        refund_order.drop(["buy_num", "order_status"], axis=1, inplace=True)
        refund_data = refund_order.groupby(["shop_id","goods_id"]).agg(
            {"count": "sum", "pay_money": "sum", "voucherMoney": "sum"}).rename(columns=
            {"count": "refund_count", "pay_money": "refund_pay",
             "voucherMoney": "refund_voucher"}).reset_index()

        if "shop_id" not in refund_data:
            refund_data["shop_id"] = ""
        if "goods_id" not in refund_data:
            refund_data["goods_id"] = ""
        if "index" in refund_data:
            refund_data.drop(["index"],inplace=True,axis=1)
        logger.info(refund_data)
        logger.info("退款订单数据处理完成")


        # # 已取消订单 已取消订单 已取消金额 已取消抵用金
        cancel_order = tran_order[tran_order["order_status"].isin([7,11])]
        cancel_order.drop(["buy_num", "order_status"], axis=1, inplace=True)
        cancel_data = cancel_order.groupby(["shop_id","goods_id"]).agg(
            {"count": "sum", "pay_money": "sum", "voucherMoney": "sum"}).rename(columns=
            {"count": "cancel_count", "pay_money": "cancel_pay",
             "voucherMoney": "cancel_voucher"}).reset_index()
        if "shop_id" not in cancel_data:
            cancel_data["shop_id"] = ""
        if "goods_id" not in cancel_data:
            cancel_data["goods_id"] = ""
        if "index" in cancel_data:
            cancel_data.drop(["index"],inplace=True,axis=1)
        logger.info(cancel_data)
        logger.info("取消订单数据处理完成")

        df_list = []
        df_list.append(tran_data)
        df_list.append(yes_data)
        df_list.append(refund_data)
        df_list.append(cancel_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['shop_id','goods_id'], how='outer'), df_list)
        logger.info("-------------------------")
        logger.info(df_merged)
        logger.info("数据合并汇总")

        logger.info(shop_data)
        if shop_data.empty:
            return {"code":"0000","status":"success","msg":[],"count":0}

        #这边可以按需拼接
        shop_mes_data = shop_data.merge(crm_data,how="left",on="phone")
        shop_mes_data.fillna("",inplace=True)
        logger.info(shop_mes_data.iloc[0])
        if keyword:
            shop_mes_data = shop_mes_data[(shop_mes_data["nickname"].str.contains(keyword))|(shop_mes_data["phone"].str.contains(keyword))|(shop_mes_data["unionid"].str.contains(keyword))]

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        count = shop_mes_data.shape[0]
        logger.info("-----------------------------------")
        logger.info(df_merged)
        logger.info(shop_mes_data)
        last_data = shop_mes_data.merge(df_merged, how="left", on=["shop_id","goods_id"])
        logger.info(last_data.shape)
        logger.info(last_data)

        # 统计上面那一栏数量
        all_data = {}
        all_data["shop_count"] = last_data.shape[0]
        all_data["tran_count"] = int(last_data["tran_count"].sum())
        all_data["tran_buy_count"] = int(last_data["tran_buy_count"].sum())
        all_data["tran_pay"] = round(last_data["tran_pay"].sum(), 2)
        all_data["ok_count"] = int(last_data["ok_count"].sum())
        all_data["ok_pay"] = round(last_data["ok_pay"].sum(), 2)
        all_data["refund_count"] = int(last_data["refund_count"].sum())
        all_data["refund_pay"] = round(last_data["refund_pay"].sum(), 2)
        all_data["cancel_count"] = int(last_data["cancel_count"].sum())
        all_data["cancel_pay"] = round(last_data["cancel_pay"].sum(), 2)
        logger.info("求和")


        last_data.sort_values('tran_count', ascending=False, inplace=True)
        last_data.fillna("", inplace=True)
        if page and size:
            last_data = last_data[code_page:code_size]
        else:
            last_data = last_data.copy()

        last_data = last_data.to_dict("records")
        data = {"all_data":all_data,"data":last_data}

        return {"code":"0000","status":"success","msg":data,"count":count}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()
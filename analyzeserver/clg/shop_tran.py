# -*- coding: utf-8 -*-

# @Time : 2022/2/9 14:58

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : shop_tran.py

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

clgtranshopbp = Blueprint('clgtranshop', __name__, url_prefix='/clgtranshop')


@clgtranshopbp.route("/all",methods=["POST"])
def clg_tran_shop_all():
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

        #店铺信息
        shop_sql = '''select msi.id shop_id,msi.name shop_name,msi.phone,msi.shopType shoptype,ggc.name cate_name from member_shop_info msi
        left join goods_goods_category ggc on msi.category_id = ggc.id
        where msi.del_flag = 0'''
        shop_data = pd.read_sql(shop_sql,conn_clg)

        #订单情况 总的交易金额
        order_sql = '''
        select shop_id,count(*) count,sum(buy_num) buy_num,
        if(toi.voucherMoneyType = 1,sum(pay_money),sum(voucherPayMoney)) pay_money,
        if(toi.voucherMoneyType = 1,0,sum(voucherMoney)) voucherMoney,order_status from trade_order_info toi
        left join trade_order_item tod on toi.order_sn = tod.order_sn 
        where toi.del_flag = 0
        group by shop_id,order_status
        '''

        #总订单
        tran_order = pd.read_sql(order_sql,conn_clg)

        #交易订单 交易订单数量 交易商品数量 交易金额 交易抵用金
        tran_data = tran_order.copy()
        logger.info(tran_data)
        tran_data.drop(["order_status"], axis=1, inplace=True)
        tran_data = tran_order.groupby(["shop_id"]).agg({"count":"sum","buy_num":"sum","pay_money":"sum","voucherMoney":"sum"}).rename(columns=
            {"count":"tran_count","buy_num":"tran_buy_count","pay_money":"tran_pay","voucherMoney":"tran_voucher"}).reset_index()

        logger.info(tran_data)

        # 有效订单 有效订单 有效金额 有效抵用金
        ok_order = tran_order[tran_order["order_status"].isin([3,4,5,6,10,15])]
        ok_order.drop(["buy_num","order_status"],axis=1,inplace=True)
        yes_data = ok_order.groupby(["shop_id"]).agg(
            {"count": "sum",  "pay_money": "sum", "voucherMoney": "sum"}).rename(columns=
            {"count": "ok_count", "pay_money": "ok_pay",
             "voucherMoney": "ok_voucher"}).reset_index()

        # 已退款订单 已退款订单 已退款金额 已退款抵用金
        refund_order = tran_order[tran_order["order_status"].isin([11,12])]
        refund_order.drop(["buy_num", "order_status"], axis=1, inplace=True)
        refund_data = refund_order.groupby(["shop_id"]).agg(
            {"count": "sum", "pay_money": "sum", "voucherMoney": "sum"}).rename(columns=
            {"count": "refund_count", "pay_money": "refund_pay",
             "voucherMoney": "refund_voucher"}).reset_index()


        # # 已取消订单 已取消订单 已取消金额 已取消抵用金
        cancel_order = tran_order[tran_order["order_status"].isin([7])]
        cancel_order.drop(["buy_num", "order_status"], axis=1, inplace=True)
        cancel_data = cancel_order.groupby(["shop_id"]).agg(
            {"count": "sum", "pay_money": "sum", "voucherMoney": "sum"}).rename(columns=
            {"count": "cancel_count", "pay_money": "cancel_pay",
             "voucherMoney": "cancel_voucher"}).reset_index()

        df_list = []
        df_list.append(tran_data)
        df_list.append(yes_data)
        df_list.append(refund_data)
        df_list.append(cancel_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['shop_id'], how='outer'), df_list)
        last_data = shop_data.merge(df_merged,how="left",on="shop_id")
        return "1"

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()
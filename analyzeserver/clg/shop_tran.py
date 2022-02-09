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

clgtranshopbp = Blueprint('clgtranshop', __name__, url_prefix='/clgtranshop')


@clgtranshopbp.route("all",methods=["POST"])
def clg_tran_shop_all():
    try:
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()
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

        #店铺信息
        shop_sql = '''select msi.id,msi.name shop_name,spm.username,spm.phone,msi.shopType,ggc.name cate_name,msi.unionid from member_shop_info msi
        left join shop_personnel_management spm on msi.id = spm.shopInfoId
        left join goods_goods_category ggc on msi.category_id = ggc.id
        where spm.usertype = 1 and spm.isFlag = 0 and msi.del_flag = 0'''



        #订单情况 总的交易金额
        order_sql = '''
        select shop_id,order_shop_name,sum(count) count,sum(buy_num) buy_num,sum(pay_money) pay_money,sum(voucherMoney) voucherMoney,order_status from (
        select shop_id,order_shop_name,count(*) count,sum(buy_num) buy_num,sum(pay_money) pay_money,0 voucherMoney,order_status,trade_order_info.create_time from trade_order_info
        left join trade_order_item on trade_order_info.order_sn = trade_order_item.order_sn
        where trade_order_info.voucherMoneyType = 1
        group by shop_id
        UNION all
        select shop_id,order_shop_name,count(*) count,sum(buy_num) buy_num,sum(voucherPayMoney) pay_money,sum(voucherMoney) voucherMoney,order_status,trade_order_info.create_time from trade_order_info
        left join trade_order_item on trade_order_info.order_sn = trade_order_item.order_sn
        where trade_order_info.voucherMoneyType = 2
        group by shop_id ) orders group by orders.shop_id
        '''

        # 交易订单
        tran_order = pd.read_sql(order_sql,conn_clg)

        # 有效订单
        ok_order = tran_order[tran_order["order_status"].isin([3,4,5,6,10,15])]

        # 已退款订单
        refund_order = tran_order[tran_order["order_status"].isin([11,12])]

        # 已取消订单
        cancel_order = tran_order[tran_order["order_status"]].isin([7])

        # return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()
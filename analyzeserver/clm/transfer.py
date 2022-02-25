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


@clmtranbp.route("/user",methods=["POST"])
def clm_tran_user_all():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 6:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            keyword = request.json['keyword']
            start_time = request.json["start_time"]
            end_time = request.json["end_time"]
            page = request.json['page']
            size = request.json['size']
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze or not conn_crm:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # 空结果
        null_summary_data = {
            "user_count": 0,
            "order_count": 0,
            "total_money": 0,
            "wechat_money": 0,
            "alipay_money": 0,
            "unknown_money": 0,
            "success_order_count": 0,
            "success_order_money": 0,
            "refund_order_money": 0,
            "refund_order_count": 0,
            "wait_order_count": 0,
            "wait_order_money": 0
        }
        return_null_data = {
            "data": [],
            "summary": null_summary_data
        }

        # 用户数据
        user_info_sql = '''
            select unionid, phone, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) name from lh_analyze.crm_user where del_flag=0
        '''
        # 用户订单数据
        user_order_sql = '''
            select unionid, pay_money, pay_status,  pay_types
            from luke_marketing.orders
            where is_del=0
        '''
        if keyword != '':
            keyword_result = get_phone_by_keyword(keyword)
            if not keyword_result[0]:
                return {"code": "0000", "status": "success", "msg": return_null_data, "count": 0}
            keyword_phone_list = keyword_result[1]
            user_info_sql += ''' and phone in (%s)''' % ','.join(keyword_phone_list)

            user_info_data = pd.read_sql(user_info_sql, conn_analyze)
            unionid_list = [str(unionid) for unionid in user_info_data['unionid'].tolist()]
            user_order_sql += ''' and unionid in (%s)''' % ','.join(unionid_list)
        else:
            user_info_data = pd.read_sql(user_info_sql, conn_analyze)
            # pass
        logger.info(user_info_data.shape)
        if start_time and end_time:
            user_order_sql += ''' and from_unixtime(addtime, "%Y-%m-%d %H:%i:%S")>="{start_time}" and from_unixtime(addtime, "%Y-%m-%d %H:%i:%S")<="{end_time}"'''.format(start_time=start_time, end_time=end_time)

        df_list = []
        user_order_df = pd.read_sql(user_order_sql, conn_crm)
        df_list.append(user_info_data)

        # 交易订单与金额
        total_order_df = user_order_df.groupby('unionid').agg({"pay_money": "sum", "unionid": "count"}).rename(
            columns={"unionid": "order_count", "pay_money": "total_money"}).reset_index()
        df_list.append(total_order_df)
        # 未知收款
        unknown_order_df = user_order_df[user_order_df['pay_types'] == 0].groupby('unionid').agg({"pay_money": "sum"}).rename(
            columns={"pay_money": "unknown_money"}).reset_index()
        df_list.append(unknown_order_df)
        # 微信收款
        wechat_order_df = user_order_df[user_order_df['pay_types'] == 1].groupby('unionid').agg({"pay_money": "sum"}).rename(
            columns={"pay_money": "wechat_money"}).reset_index()
        df_list.append(wechat_order_df)
        # 支付宝收款
        alipay_order_df = user_order_df[user_order_df['pay_types'] == 2].groupby('unionid').agg({"pay_money": "sum"}).rename(
            columns={"pay_money": "alipay_money"}).reset_index()
        df_list.append(alipay_order_df)
        # 余额收款
        balance_order_df = user_order_df[user_order_df['pay_types'] == 3].groupby('unionid').agg({"pay_money": "sum"}).rename(
            columns={"pay_money": "balance_money"}).reset_index()
        df_list.append(balance_order_df)
        # 有效订单
        success_order_df = user_order_df[user_order_df['pay_status'] == 2].groupby('unionid').agg({"pay_money": "sum", "unionid": "count"}).rename(
            columns={"unionid": "success_order_count", "pay_money": "success_order_money"}).reset_index()
        df_list.append(success_order_df)
        # 已退款订单
        refund_order_df = user_order_df[user_order_df['pay_status'] == 3].groupby('unionid').agg({"pay_money": "sum", "unionid": "count"}).rename(
            columns={"unionid": "refund_order_count", "pay_money": "refund_order_money"}).reset_index()
        df_list.append(refund_order_df)
        # 待支付订单
        wait_order_df = user_order_df[user_order_df['pay_status'] == 1].groupby('unionid').agg({"pay_money": "sum", "unionid": "count"}).rename(
            columns={"unionid": "wait_order_count", "pay_money": "wait_order_money"}).reset_index()
        df_list.append(wait_order_df)

        # 合并所有订单
        fina_df = reduce(lambda left, right: pd.merge(left, right, on='unionid', how='outer'), df_list)
        fina_df['name'].fillna('', inplace=True)
        fina_df['phone'].fillna('', inplace=True)
        fina_df.fillna(0, inplace=True)
        # 汇总数据
        ignore_columns_list =['unionid', 'name', 'phone', 'balance_money']
        summary_data = fina_df.loc[:, [column for column in fina_df.columns if not column in ignore_columns_list]].sum().to_dict()
        for i in [column for column, value in summary_data.items() if 'money' in column]:
            summary_data[i] = round(summary_data[i], 2)
        summary_data['user_count'] = fina_df.shape[0]

        # 根据订单数排序
        fina_df.sort_values('order_count', ascending=False, inplace=True)
        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
            cut_data = fina_df[start_index:end_index]
        else:
            cut_data = fina_df.copy()

        return_data = {
            'data': cut_data.to_dict("records"),
            'summary_data': summary_data
        }
        return {"code": "0000", "status": "success", "msg": return_data, "count": fina_df.shape[0]}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
            conn_analyze.close()
        except:
            pass

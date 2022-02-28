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

        page = request.json.get("page")
        size = request.json.get("size")
        keyword = request.json.get("keyword")
        # 商家名称
        shop_id = request.json.get("shop_id")
        # 经营分类
        cate_id = request.json.get("cate_id")
        # 店铺类型
        shop_type = request.json.get("shop_type")
        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")
        wx_status = request.json.get("wx_status")
        zfb_status = request.json.get("zfb_status")
        shop_status = request.json.get("shop_status")

        # 商家类型
        dianpu_type = request.json.get("dianpu_type")



        #店铺信息
        shop_sql = '''select shop.id shop_id,shop.`name`,shop.phone,shop.types,sc.cate_name,shop.status,if(user.capacity = 1,1,2) capacity,alipaystatus,wechatstatus from luke_marketing.shop shop
            left join luke_lukebus.user user on shop.phone = user.phone
            left join luke_marketing.shop_category sc on shop.cate_id = sc.cate_id'''
        logger.info(shop_id)
        condition = []
        # 店铺id
        if str(shop_id) and str(shop_id)!="None":
            condition.append(''' shop.id = %s ''' %shop_id)
        # 经营id
        if str(cate_id) and str(cate_id)!="None":
            condition.append(''' sc.cate_id = %s ''' %cate_id)
        # 店铺类型
        if str(shop_type) and str(shop_type)!="None":
            condition.append(''' shop.types = %s ''' %shop_type)
        # 商家类型 运营 普通
        if str(dianpu_type) and str(dianpu_type)!="None":
            condition.append(''' capacity = %s ''' %dianpu_type)
        if str(wx_status) and str(wx_status)!="None":
            condition.append(''' wechatstatus = %s ''' %wx_status)
        if str(zfb_status) and str(zfb_status)!="None":
            condition.append(''' alipaystatus = %s ''' %zfb_status)
        if str(shop_status) and str(shop_status)!="None":
            condition.append(''' shop_status = %s ''' %shop_status)

        if condition:
            for i in range(0,len(condition)):
                if i == 0:
                    shop_sql = shop_sql + " where " + condition[i]
                else:
                    shop_sql = shop_sql + " and " + condition[i]
        logger.info(shop_sql)
        shop_data = pd.read_sql(shop_sql, conn_clm)
        logger.info(shop_data)
        logger.info("店铺数据读取完成")

        serach_phone = list(set(shop_data["phone"].to_list()))
        if "" in serach_phone:
            serach_phone.remove("")
        logger.info(serach_phone)
        if serach_phone:
            crm_sql = '''select unionid,if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) nickname,phone from crm_user where del_flag = 0 and phone is not null and phone != "" and phone in (%s)''' % ",".join(serach_phone)
            crm_data = pd.read_sql(crm_sql, conn_analyze)
            logger.info("用户数据完成")
        else:
            crm_data = [{"unionid":"","nickname":"","phone":""}]
            crm_data = pd.DataFrame(crm_data)

        user_data = shop_data.merge(crm_data,on="phone",how="left")
        if user_data is None:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        #订单情况
        order_sql = '''select id,shop_id,pay_money,pay_status,pay_types from luke_marketing.orders where is_del = 0'''
        if start_time and end_time:
            order_sql = order_sql + ''' and FROM_UNIXTIME(addtime,"%Y-%m-%d %H:%i:%s") >= "{}" and FROM_UNIXTIME(addtime,"%Y-%m-%d %H:%i:%s")<="{}"'''.format(start_time,end_time)
        logger.info(order_sql)
        order_data = pd.read_sql(order_sql,conn_clm)

        logger.info(order_data.iloc[0])

        #交易金额 交易数量
        tran_all_data = order_data.groupby(["shop_id"]).agg({"id":"count","pay_money":"sum"}).rename(columns={"id": "count", "pay_money": "pay_money"}).reset_index()

        #余额收款
        tran_packet_data = order_data[order_data["pay_types"] == 3].groupby(["shop_id"]).agg({"pay_money":"sum"}).rename(columns={"pay_money": "packet_pay_money"}).reset_index()
        logger.info(tran_packet_data)

        #支付宝收款
        tran_zfb_data = order_data[order_data["pay_types"] == 2].groupby(["shop_id"]).agg({"pay_money": "sum"}).rename(columns={"pay_money": "zfb_pay_money"}).reset_index()
        logger.info(tran_zfb_data)

        #微信收款
        tran_wx_data = order_data[order_data["pay_types"] == 1].groupby(["shop_id"]).agg({"pay_money": "sum"}).rename(columns={"pay_money": "wx_pay_money"}).reset_index()
        logger.info(tran_wx_data)


        #有效订单
        ok_order_data = order_data[order_data["pay_status"] == 2].groupby(["shop_id"]).agg({"id":"count","pay_money":"sum"}).rename(columns={"id": "ok_count", "pay_money": "ok_pay_money"}).reset_index()

        # 已退款订单
        refund_order_data = order_data[order_data["pay_status"] == 3].groupby(["shop_id"]).agg({"id":"count","pay_money":"sum"}).rename(columns={"id": "refund_count", "pay_money": "refund_pay_money"}).reset_index()

        # 待支付
        nopay_order_data = order_data[order_data["pay_status"] == 1].groupby(["shop_id"]).agg({"id":"count","pay_money":"sum"}).rename(columns={"id": "nopay_count", "pay_money": "no_pay_money"}).reset_index()


        #订单数据合并
        df_list = []
        df_list.append(tran_all_data)
        df_list.append(tran_packet_data)
        df_list.append(tran_zfb_data)
        df_list.append(tran_wx_data)
        df_list.append(ok_order_data)
        df_list.append(refund_order_data)
        df_list.append(nopay_order_data)
        form_order_data = reduce(lambda left, right: pd.merge(left, right, on='shop_id', how='left'), df_list)
        form_order_data.fillna(0,inplace=True)

        user_data.fillna("",inplace=True)
        form_data = user_data.merge(form_order_data,how="left",on="shop_id")
        form_data.fillna(0, inplace=True)

        # logger.info(form_data["name"])
        # logger.info(form_data["phone"])
        # logger.info(form_data["unionid"])
        form_data["unionid"] = form_data["unionid"].astype(str)
        #关键词
        if keyword:
            form_data = form_data[(form_data["name"].str.contains(keyword)) | (form_data["phone"].str.contains(keyword)) | (form_data["unionid"].str.contains(keyword))]
            # form_data = form_data[(form_data["name"].str.contains(keyword)) | (form_data["phone"].str.contains(keyword))]

        count = form_data.shape[0]

        all_data = {"shop_count":0,"ok_wechatstatus":0,"ok_alipaystatus":0,"count":0,"pay_money":0,"ok_count":0,"ok_pay_money":0,
                    "refund_count":0,"nopay_count":0,"no_pay_money":0}
        all_data["shop_count"] = int(form_data["shop_id"].count())
        all_data["ok_wechatstatus"] = form_data[form_data["wechatstatus"] == 6].shape[0]
        all_data["ok_alipaystatus"] = form_data[form_data["alipaystatus"] == 6].shape[0]
        all_data["count"] = int(form_data["count"].sum())
        all_data["pay_money"] = round(float(form_data["pay_money"].sum()),2)
        all_data["ok_count"] = int(form_data["ok_count"].sum())
        all_data["ok_pay_money"] = round(float(form_data["ok_pay_money"].sum()),2)
        all_data["refund_count"] = int(form_data["refund_count"].sum())
        all_data["refund_money"] = round(float(form_data["refund_pay_money"].sum()),2)
        all_data["nopay_count"] = int(form_data["nopay_count"].sum())
        all_data["no_pay_money"] = round(float(form_data["no_pay_money"].sum()),2)

        form_data.sort_values('count', ascending=False, inplace=True)
        if page and size:
            code_page = (page - 1) * size
            code_size = page * size
            form_data = form_data[code_page:code_size]
        form_data = form_data.to_dict("records")
        data = {
            "all_data":all_data,
            "data":form_data
        }

        return {"code":"0000","status":"success","msg":data,"count":count}
    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clm.close()
        conn_analyze.close()

# 用户数据统计
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

            user_info_df = pd.read_sql(user_info_sql, conn_analyze)
            unionid_list = [str(unionid) for unionid in user_info_df['unionid'].tolist()]
            user_order_sql += ''' and unionid in (%s)''' % ','.join(unionid_list)
        else:
            user_info_df = pd.read_sql(user_info_sql, conn_analyze)
            # pass
        logger.info(user_info_df.shape)
        if start_time and end_time:
            user_order_sql += ''' and from_unixtime(addtime, "%Y-%m-%d %H:%i:%S")>="{start_time}" and from_unixtime(addtime, "%Y-%m-%d %H:%i:%S")<="{end_time}"'''.format(start_time=start_time, end_time=end_time)

        df_list = []
        user_order_df = pd.read_sql(user_order_sql, conn_crm)
        df_list.append(user_info_df)

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

# 订单流水
@clmtranbp.route("/orderflow",methods=["POST"])
def clm_orderflow_all():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 12:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            # 订单号
            order_sn = request.json['order_sn']
            # 下单时间
            start_paytime = request.json['start_paytime']
            end_paytime = request.json['end_paytime']
            # 发起时间
            start_addtime = request.json['start_addtime']
            end_addtime = request.json['end_addtime']
            # 付款人信息
            buyer_info = request.json['buyer_info']
            # 归属商家
            shop_id = request.json['shop_id']
            # 支付方式
            pay_types = request.json['pay_types']
            # 订单状态
            order_status = request.json['order_status']
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

        # 用户数据
        user_info_sql = '''
            select unionid, phone, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) name from lh_analyze.crm_user where del_flag=0
        '''
        base_order_sql = '''
            select t1.order_sn, t2.shop_name, t1.shop_id, t1.unionid, t1.pay_types, t1.money, t1.pay_money, t1.order_status, t1.addtime, t1.paytime from
            (select `no` order_sn, shop_id, unionid, pay_types, money, pay_money, pay_status order_status, from_unixtime(addtime, "%Y-%m-%d %H:%i:%S") addtime, from_unixtime(paytime, "%Y-%m-%d %H:%i:%S") paytime, is_del from luke_marketing.orders) t1
            left join
            (select id, name shop_name from luke_marketing.shop) t2
            on t1.shop_id=t2.id
            where is_del=0
        '''
        if start_addtime and end_addtime:
            base_order_sql += ''' and ddtime>="{start_time}" and addtime<="{end_time}"'''.format(start_time=start_addtime, end_time=end_addtime)
        if start_paytime and end_paytime:
            base_order_sql += ''' and paytime>="{start_time}" and paytime<="{end_time}"'''.format(start_time=start_paytime, end_time=end_paytime)
        if order_sn != '':
            base_order_sql += ''' and order_sn=%s''' % order_sn
        if shop_id != '':
            base_order_sql += ''' and shop_id=%s''' % shop_id
        if pay_types != '':
            base_order_sql += ''' and pay_types=%s''' % pay_types
        if order_status != '':
            base_order_sql += ''' and order_status=%s''' % order_status
        # 购买人
        if buyer_info != '':
            buyer_resutl = get_phone_by_keyword(buyer_info)
            if not buyer_resutl[0]:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
            buyer_phone_list = buyer_resutl[1]
            user_info_sql += ''' and phone in (%s)''' % ','.join(buyer_phone_list)

            user_info_df = pd.read_sql(user_info_sql, conn_analyze)
            unionid_list = [str(unionid) for unionid in user_info_df['unionid'].tolist()]
            base_order_sql += ''' and unionid in (%s)''' % ','.join(unionid_list)
            user_order_df = pd.read_sql(base_order_sql,conn_crm)
        else:
            user_order_df = pd.read_sql(base_order_sql, conn_crm)
            unionid_list = [str(unionid) for unionid in set(user_order_df['unionid'].tolist())]
            if len(unionid_list) == 0:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
            user_info_sql += ''' and unionid in (%s)''' % ','.join(unionid_list)
            user_info_df = pd.read_sql(user_info_sql, conn_analyze)
        # 合并用户信息与订单信息
        fina_df = user_order_df.merge(user_info_df, how='left', on='unionid')
        fina_df.fillna('', inplace=True)
        fina_df.sort_values('addtime', ascending=False, inplace=True)

        # 关系映射
        map_order_status = {
            1: "待支付",
            2: "已支付",
            3: "已退款"
        }
        map_pay_types = {
            0: "未知",
            1: "微信支付",
            2: "支付宝支付",
            3: "余额支付"
        }
        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
            cut_data = fina_df[start_index:end_index]
        else:
            cut_data = fina_df.copy()
        cut_data['pay_types'] = cut_data['pay_types'].map(map_pay_types)
        cut_data['order_status'] = cut_data['order_status'].map(map_order_status)

        return {"code": "0000", "status": "success", "msg": cut_data.to_dict("records"), "count": fina_df.shape[0]}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
            conn_analyze.close()
        except:
            pass
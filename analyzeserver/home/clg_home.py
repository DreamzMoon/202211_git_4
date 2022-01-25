# -*- coding: utf-8 -*-

# @Time : 2022/1/25 15:05

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : clg_home.py


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

clghomebp = Blueprint('clghome', __name__, url_prefix='/clghome')

'''个人排行版'''
@clghomebp.route("person/top",methods=["GET"])
def person_top():
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

        sql = '''select phone,sum(pay_money) pay_total_money from trade_order_info where DATE_FORMAT(create_time,"%Y-%m_%d") = CURRENT_DATE and order_status in (4,5,6,10,15)
        and del_flag = 0
        group by phone order by pay_total_money desc limit 3'''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clg)
        # datas = datas.to_dict("records")
        # logger.info(len(datas))
        phone_lists = datas["phone"].tolist()

        logger.info(phone_lists)
        if not phone_lists:
            return {"code": "0000", "status": "success", "msg": []}
        sql = '''select phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from crm_user where phone in ({})'''.format(
            ",".join(phone_lists))
        logger.info(sql)
        user_data = pd.read_sql(sql, conn_analyze)
        datas = datas.merge(user_data, on="phone", how="left")
        logger.info(datas)
        datas["username"].fillna("", inplace=True)
        datas = datas.to_dict("records")

        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

'''交易中心'''
@clghomebp.route("datacenter", methods=["GET"])
def data_center():
    try:
        conn_clg = direct_get_conn(clg_mysql_conf)
        if not conn_clg:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
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
            select count(distinct t1.user_id) person_count, count(*) order_count, sum(t1.pay_money) total_money, sum(t2.buy_num) total_count from 
            (select order_sn, user_id, date_format(create_time, "%Y-%m-%d") create_time, pay_money from trade_order_info
            where date_format(create_time, "%Y-%m-%d")=current_date and order_status in (3,4,5,6,10,15) and del_flag=0) t1
            left join
            (select order_sn, sum(buy_num) buy_num from trade_order_item where date_format(create_time, "%Y-%m-%d")=current_date group by order_sn) t2
            on t1.order_sn=t2.order_sn
        '''

        data = (pd.read_sql(sql,conn_clg)).to_dict("records")
        return {"code":"0000","status":"success","msg":data}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
        except:
            pass

'''商品排行版'''
@clghomebp.route("product/top",methods=["GET"])
def product_top():
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



        sql = '''select od.goods_name,sum(o.pay_money) pay_total_money from trade_order_info o
        left join trade_order_item od on o.order_sn = od.order_sn
        where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0 
        group by od.goods_id
        order by pay_total_money desc limit 3'''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clg)
        datas = datas.to_dict("records")


        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

'''店铺排行版'''
@clghomebp.route("shop/top",methods=["GET"])
def shop_top():
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



        sql = '''select o.shop_name,sum(o.pay_money) pay_total_money from trade_order_info o
        where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0 
        group by o.shop_id
        order by pay_total_money desc limit 7'''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clg)
        datas = datas.to_dict("records")


        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

'''今日新增用户动态'''
@clghomebp.route('/today/dynamic/newuser', methods=["GET"])
def today_dynamic_newuser():
    try:
        try:
            logger.info(request.json)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_an = direct_get_conn(analyze_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        if not conn_clg or not conn_an:
            return {"code": "10002", "status": "failed", "message": message["10002"]}

        search_name_sql = '''
            select phone, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username from crm_user where phone = "%s"
        '''
        # 新注册用户
        new_user_sql = '''
            select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, phone from mall.member_user
            where del_flag=0 and (phone is not null or phone != '')
            and create_time is not null
            and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE
            order by create_time desc
            limit 3
        '''

        new_user_df = pd.read_sql(new_user_sql, conn_clg)
        if new_user_df.shape[0] > 0:
            new_user_df['sub_time'] = round(new_user_df['sub_time'], 0).astype(int)

            new_user_phone_list = new_user_df['phone'].to_list()
            new_user_df_list = []
            for phone in set(new_user_phone_list):
                new_user_df_list.append(pd.read_sql(search_name_sql % phone, conn_an))
            user_df = pd.concat(new_user_df_list, axis=0)
            new_user_fina_df = new_user_df.merge(user_df, how='left', on='phone')
            new_user_fina_df["username"].fillna("", inplace=True)
            new_user_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            new_user_list = new_user_fina_df.to_dict("records")
        else:
            new_user_list = []

        # for nl in new_user_list:
        #     if nl["phone"]:
        #         nl["phone"] = nl["phone"][0:4]+len(nl["phone"][4:])*"*"
        #     if nl["username"]:
        #         nl["username"] = nl["username"][0]+len(nl["username"][1:])*"*"

        return_data = {
            "new_user": new_user_list
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
            conn_an.close()
        except:
            pass

'''今日商品实时动态'''
@clghomebp.route('/today/dynamic/goods', methods=["GET"])
def today_dynamic_goods():
    try:
        try:
            logger.info(request.json)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_clg = direct_get_conn(clg_mysql_conf)
        if not conn_clg:
            return {"code": "10002", "status": "failed", "message": message["10002"]}

        shop_goods_sql = '''
            select t2.shop_name, t1.item_pay_money sell_money, t1.goods_name, t2.sub_time from
            (select order_sn,goods_name, item_pay_money from trade_order_item where order_sn in (select order_sn from trade_order_info where date_format(create_time, "%Y-%m-%d")=current_date and order_status in (3,4,5,6,10,15))) t1
            left join
            (select order_sn, shop_name, TIMESTAMPDIFF(second,create_time,now())/60 sub_time from trade_order_info where date_format(create_time, "%Y-%m-%d")=current_date and order_status in (3,4,5,6,10,15)) t2
            on t1.order_sn=t2.order_sn
            order by sub_time desc
            limit 3
        '''
        shop_goods_data = pd.read_sql(shop_goods_sql, conn_clg).to_dict("records")

        return_data = {
            "shop_goods": shop_goods_data
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
        except:
            pass

'''今日交易实时动态'''
@clghomebp.route("order/status",methods=["GET"])
def order_status():
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



        sql = '''select o.phone,o.pay_money,od.goods_name from trade_order_info o
        left join trade_order_item od on o.order_sn = od.order_sn
        where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0 
        order by o.create_time desc limit 3'''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clg)
        # datas = datas.to_dict("records")
        # logger.info(len(datas))
        phone_lists = datas["phone"].tolist()

        logger.info(phone_lists)
        if not phone_lists:
            return {"code": "0000", "status": "success", "msg": []}
        sql = '''select phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from crm_user where phone in ({})'''.format(
            ",".join(phone_lists))
        logger.info(sql)
        user_data = pd.read_sql(sql, conn_analyze)
        datas = datas.merge(user_data, on="phone", how="left")
        logger.info(datas)
        datas["username"].fillna("", inplace=True)
        datas = datas.to_dict("records")

        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

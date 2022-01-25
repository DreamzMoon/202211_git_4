# -*- coding: utf-8 -*-

# @Time : 2022/1/25 15:05

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : clg_home.py


import sys
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



        sql = '''select phone,sum(pay_money) total_money from trade_order_info where DATE_FORMAT(create_time,"%Y-%m_%d") = CURRENT_DATE and order_status in (4,5,6)
        group by phone order by total_money desc limit 3'''
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



        sql = '''select od.goods_name,sum(o.total_money) total_money from trade_order_info o
        left join trade_order_item od on o.order_sn = od.order_sn
        where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6)
        group by od.goods_id
        order by o.create_time desc limit 3'''
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



        sql = '''select o.shop_name,sum(o.total_money) total_money from trade_order_info o
        where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6)
        group by o.shop_id
        order by o.create_time desc limit 7'''
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
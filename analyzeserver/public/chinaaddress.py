# -*- coding: utf-8 -*-

# @Time : 2022/1/13 15:46

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : chinaaddress.py

import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from analyzeserver.common import *
from config import *
import traceback
from util.help_fun import *
import pandas as pd
from analyzeserver.user.sysuser import check_token
import base64
import oss2
import time

chaddrbp = Blueprint('chaddress', __name__, url_prefix='/chaddr')

@chaddrbp.route("pro",methods=["GET"])
def get_pro():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        logger.info(request.args)
        # token = request.headers["Token"]
        # user_id = request.args.get("user_id")
        #
        # if not user_id and not token:
        #     return {"code": "10001", "status": "failed", "msg": message["10001"]}
        #
        # check_token_result = check_token(token, user_id)
        # if check_token_result["code"] != "0000":
        #     return check_token_result

        sql = '''select code,name from province'''
        pro_data = pd.read_sql(sql,conn_analyze)
        pro_data = pro_data.to_dict("records")
        return {"code":"0000","status":"success","msg":pro_data}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


@chaddrbp.route("city",methods=["GET"])
def get_city():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()
        logger.info(request.json)
        # token = request.headers["Token"]
        # user_id = request.args.get("user_id")
        #
        # if not user_id and not token:
        #     return {"code": "10001", "status": "failed", "msg": message["10001"]}
        #
        # check_token_result = check_token(token, user_id)
        # if check_token_result["code"] != "0000":
        #     return check_token_result

        pro_code = request.args.get("pro_code")
        if pro_code:
            sql = '''select code,name from city where province_code = %s'''
            cursor_analyze.execute(sql,(pro_code))
            city_data = pd.DataFrame(cursor_analyze.fetchall())
            city_data.columns = ["code","name"]
            city_data = city_data.to_dict("records")
        else:
            city_data = []
        return {"code":"0000","status":"success","msg":city_data}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


@chaddrbp.route("region",methods=["GET"])
def get_region():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()
        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.args.get("user_id")

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        city_code = request.args.get("city_code")

        sql = '''select code,name from region where city_code = %s'''
        cursor_analyze.execute(sql,(city_code))
        region_data = pd.DataFrame(cursor_analyze.fetchall())
        region_data.columns = ["code","name"]
        region_data = region_data.to_dict("records")
        return {"code":"0000","status":"success","msg":region_data}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


@chaddrbp.route("town",methods=["GET"])
def get_town():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()
        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.args.get("user_id")
        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        region_code = request.args.get("region_code")

        sql = '''select code,name from town where region_code = %s'''
        cursor_analyze.execute(sql,(region_code))
        town_data = pd.DataFrame(cursor_analyze.fetchall())
        town_data.columns = ["code","name"]
        town_data = town_data.to_dict("records")
        return {"code":"0000","status":"success","msg":town_data}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


@chaddrbp.route("list",methods=["GET"])
def get_list():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()
        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.args.get("user_id")
        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        pro_sql = '''select province.code pro_code,province.name pro_name,"" children from province'''
        pro_datas = pd.read_sql(pro_sql,conn_analyze)
        pro_datas = pro_datas.to_dict("records")

        for p in pro_datas:
            city_dict = {}
            city_sql = '''select city.code city_code,city.name city_name from city where province_code = %s'''
            logger.info(city_sql)
            cursor_analyze.execute(city_sql,(p["pro_code"]))
            city_data = cursor_analyze.fetchall()
            city_dict["city_code"] = city_data[0]
            city_dict["city_name"] = city_data[1]
            city_dict["children"] = ""
            p["children"] = city_dict

            for c in city_data:
                region_dict = {}
                region_sql = '''select region.code region_code,region.name region_name from region where city_code = %s'''
                logger.info(region_dict)
                cursor_analyze.execute(region_sql, (c["city_code"]))
                region_data = cursor_analyze.fetchone()
                region_dict["city_code"] = region_data[0]
                region_dict["city_name"] = region_data[1]
                region_dict["children"] = ""
                c["children"] = region_dict

        # region_sql = '''select region.city_code city_code,region.code region_code,region.name region_name from region'''
        # town_sql = '''select town.region_code region_code,town.code town_code,town.name town_name from town'''


        return {"code":"0000","status":"success","msg":pro_datas}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
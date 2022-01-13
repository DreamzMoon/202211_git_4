# -*- coding: utf-8 -*-

# @Time : 2022/1/13 16:59

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : crm_user_tag.py
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


usertagbp = Blueprint('crm_user_tag', __name__, url_prefix='/user/tag/')

'''标签列表'''
@usertagbp.route("list",methods=["GET"])
def user_tag_list():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.args.get("user_id")
        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        sql = '''select id,tag_name from crm_tag'''
        tag_list = pd.read_sql(sql,conn_analyze)
        tag_list.columns=["id","tag_name"]
        tag_list = tag_list.to_dict("records")
        return {"code":"0000","msg":tag_list,"status":"success"}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


@usertagbp.route("uinsert",methods=["POST"])
def user_tag_uinsert():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()

        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json.get("user_id")
        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        logger.info("dddddd")
        tag_list = request.json.get("tag_list")
        unionid_list = request.json.get("unionid_list")
        params = []
        for tag_id in tag_list:
            for unionid in unionid_list:
                params.append((unionid,tag_id))
        logger.info(params)

        insert_sql = '''insert into crm_user_tag (unionid,tag_id) values (%s,%s)'''
        cursor_analyze.executemany(insert_sql,(params))
        conn_analyze.commit()
        return {"code":"0000","msg":"更新成功","status":"success"}
    except Exception as e:
        conn_analyze.rollback()
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


@usertagbp.route("insert",methods=["POST"])
def user_tag_insert():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()

        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json.get("user_id")
        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        tag_name = request.json.get("tag_name")
        params = []
        params.append(tag_name)

        insert_sql = '''INSERT INTO `lh_analyze`.`crm_tag` (`tag_name`) VALUES ("%s")'''
        cursor_analyze.execute(insert_sql,(params))
        conn_analyze.commit()
        return {"code":"0000","msg":"插入成功","status":"success"}
    except Exception as e:
        conn_analyze.rollback()
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
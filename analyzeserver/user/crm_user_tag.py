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


'''用户打标'''
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
                #判断标签是否存在 如果存在不在进行打标
                sql = '''select * from crm_user_tag where unionid = %s and tag_id = %s'''
                cursor_analyze.execute(sql,(unionid,tag_id))
                exist = cursor_analyze.fetchone()
                if not exist:
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


'''新增标签'''
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
        return {"code":"0000","msg":"标签新增成功","status":"success"}
    except Exception as e:
        conn_analyze.rollback()
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


'''标签的删除'''
@usertagbp.route("del",methods=["delete"])
def user_tag_delete():
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

        tag_id = request.args.get("tag_id")
        params = []
        params.append(tag_id)

        #删除标签列表
        delete_tag_sql = '''delete from crm_tag where id = %s'''
        cursor_analyze.execute(delete_tag_sql,(tag_id))

        delete_user_tag = '''delete from crm_user_tag where tag_id = %s'''
        cursor_analyze.execute(delete_user_tag,(tag_id))

        conn_analyze.commit()
        return {"code":"0000","msg":"删除标签成功","status":"success"}
    except Exception as e:
        conn_analyze.rollback()
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()


@usertagbp.route("list/distribute",methods=["POST"])
def user_tag_distribute():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        token = request.headers["Token"]
        user_id = request.json.get("user_id")
        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        tag_name = request.json.get("tag_name")
        page = request.json.get("page")
        size = request.json.get("size")

        sql = '''select id tag_id,tag_name from crm_tag'''
        tag_data = pd.read_sql(sql, conn_analyze)

        sql = '''select tag_id,unionid from crm_user_tag'''
        user_tag_data = pd.read_sql(sql, conn_analyze)
        tag_data["tag_id"] = tag_data["tag_id"].astype(str)

        data = pd.merge(tag_data, user_tag_data, how="left", on="tag_id")
        if tag_name:
            data = data[data["tag_name"].str.contains(tag_name)]

        data = data.groupby(["tag_id", "tag_name"]).agg({"unionid": "count"}).reset_index().rename(columns={"unionid": "count"})
        count = data.shape[0]

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size
            data = data[code_page:code_size]
        logger.info(data)
        data = data.to_dict("records")

        return {"code":"0000","msg":data,"count":count,"status":"success"}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
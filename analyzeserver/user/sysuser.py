# -*- coding: utf-8 -*-

# @Time : 2021/11/3 15:47

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : sysuser.py

import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from analyzeserver.common import *
from config import *
import traceback
from util.help_fun import *
import pandas as pd
import redis
from uuid import uuid1
import base64

sysuserbp = Blueprint('sysuser', __name__, url_prefix='/user')

r = get_redis()
# r=ssh_redis()

def check_token(token,user_id):
  logger.info(token)
  try:
    # conn = ssh_get_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
    conn = direct_get_conn(analyze_mysql_conf)
    with conn.cursor() as cursor:
      # 校验前端传来的用户id数据库是否存在
      sql = '''select * from lh_analyze.sys_user where id = %s'''
      cursor.execute(sql,(user_id))
      data = cursor.fetchone()
      logger.info(data)
      if not data:
        return {"code": "11026", "msg": message["11026"], "status": "failed"}

      if int(data[6]) == 1:
        r.delete(token)
        return {"code":"11022","msg":message["11022"], "status": "failed"}

      if not token:
        return {"code": "10020", "msg": message["10020"], "status": "failed"}

      token_result = r.get(token)
      logger.info(token_result)
      if not token_result:
        return {"code": "11012", "msg": message["11012"], "status": "failed"}

      redis_user_id = json.loads(token_result)["user_id"]
      if str(user_id) != str(redis_user_id):
        return {"code": "11024", "msg": message["11024"], "status": "failed"}

      logger.info("用户令牌正确")
      return {"code":"0000","msg":"用户令牌正常","status":"success"}
  except Exception as e:
    logger.exception(traceback.format_exc())
    return {"code":"10000","msg":message["10000"],"status":"failed"}

def create_token(user_id,nowtime):
    str_token = str(uuid1())+str(user_id)+str(nowtime)
    return str(base64.b64encode(str_token.encode("utf-8")), "utf-8")

@sysuserbp.route("/register",methods=['POST'])
def register():
    #获取参数

    request_data = request.json
    logger.info("requests_data:%s" %request_data)
    phone = request_data["phone"].strip()
    password = request_data["password"].strip()
    username = request_data["username"].strip()

    if not phone or not password or not username:
      return {"code": "10005", "msg": message["10005"], "status": "failed"}


    addtime = datetime.datetime.now()
    delflag = 0

    # 创建数据库连接
    # conn = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
    conn = direct_get_conn(analyze_mysql_conf)
    logger.info("conn:%s" %conn)
    if not conn:
        return {"code": 10001, "status": "failed", "msg": message["10001"]}
    try:
        with conn.cursor() as cursor:
            # 注册时候同时判断
            sql = '''select * from lh_analyze.sys_user where phone = %s or username = %s'''
            cursor.execute(sql,(phone,username))
            data = cursor.fetchone()
            if data:
                return {"code":"11011","status":"failed","msg":message["11011"]}
            else:

                insert_sql = '''insert into lh_analyze.sys_user (username,phone,password,delflag) values (%s,%s,%s,%s)'''
                cursor.execute(insert_sql,(username,phone,password,delflag))

                user_id = int(cursor.lastrowid)
                conn.commit()
                logger.info("注册成功")

                return {"code": "0000", "status": "success", "msg": {"user_id":user_id}}
    except Exception as e:
        logger.info(traceback.format_exc())
        logger.info(e)
        conn.rollback()
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn.close()

@sysuserbp.route("/login",methods=['POST'])
def login():
    # 获取参数
    try:
        request_data = request.json
        username = request_data["username"].strip()
        password = request_data["password"].strip()
    except:
        return {"code": "10000", "msg": message["10000"], "status": "failed"}

    if not username and not password:
      return {"code": "10005", "msg": message["10005"], "status": "failed"}

    #创建数据库连接
    # conn = ssh_get_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
    conn = direct_get_conn(analyze_mysql_conf)
    logger.info("conn:%s" % conn)
    if not conn:
        return {"code": 10001, "status": "failed", "msg": message["10001"]}
    try:
        with conn.cursor() as cursor:
            sql = '''select * from lh_analyze.sys_user where username = %s and delflag = 0'''
            cursor.execute(sql, (username))
            data = cursor.fetchone()
            logger.info(data)
            if data:
                if data[3] == password:
                    if data[7] == 0:
                        return {"code": "11010", "status": "success", "msg": message["11010"]}
                    user_id = data[0]
                    token = create_token(user_id, datetime.datetime.now())
                    # r.set(token, json.dumps({"user_id": user_id, "username": username}),ex=86400)
                    # 86400*30
                    r.set(token, json.dumps({"user_id": user_id, "username": username}),ex=2592000)
                    return_data = {"user_id": user_id, "token": token}
                    logger.info("return_data:%s" %return_data)
                    return {"code": "0000", "status": "success", "msg": return_data}
                else:
                    return {"code": "11027", "status": "failed", "msg": message["11027"]}
            else:
                return {"code": "11026", "status": "failed", "msg": message["11026"]}
    except Exception as e:
        logger.exception(traceback.format_exc())
        logger.exception(e)
        conn.rollback()
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn.close()

@sysuserbp.route("logout",methods=["GET"])
def logout():
    try:
        # conn = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
        conn = direct_get_conn(analyze_mysql_conf)
        if not conn:
            return {"code": 10001, "status": "failed", "msg": message["10001"]}

        request_headers = request.headers
        logger.info(request_headers)
        token = request_headers["Token"]

        user_id = request.args.get("user_id")

        if not user_id:
          return {"code": "10005", "msg": message["10005"], "status": "failed"}

        r.delete(token)
        return {"code":"0000","msg":"用户退出成功","status":"success"}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

@sysuserbp.route("check",methods=["GET"])
def check():
    try:
        # conn = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
        conn = direct_get_conn(analyze_mysql_conf)
        if not conn:
            return {"code": 10001, "status": "failed", "msg": message["10001"]}

        request_headers = request.headers
        logger.info(request_headers)
        token = request_headers["Token"]


        user_id = request.args.get("user_id")

        if not user_id:
          return {"code": "10005", "msg": message["10005"], "status": "failed"}



        check_token_result = check_token(token,user_id)
        if check_token_result["code"] == "0000":
          return {"code":"0000","data":"用户登录状态正常","status":"success"}
        else:
          return check_token_result
    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}


if __name__ == "__main__":
    result = check_token(111111,10)
    logger.info(result)
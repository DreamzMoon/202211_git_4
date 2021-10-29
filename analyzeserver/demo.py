# -*- coding: utf-8 -*-

# @Time : 2021/10/20 13:51

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : demo.py

import sys
sys.path.append(".")
sys.path.append("../")
from flask import *
from config import *

demobp = Blueprint('demo', __name__, url_prefix='/demo')

'''
get post put delete 四个请求常用的方法
'''

@demobp.route("/getdemo",methods=["get"])
def getdemo():
    try:
        logger.info("get请求来了")
        #获取参数可以用 如果参数用user_id request.args.get("user_id").strip()
        user_id = request.args.get("user_id")
        if user_id:
            logger.info("准备返回用户数据")
            return {"code": "0000", "status": "success", "msg": "get获取用户id成功，user_id:%s" %user_id}
        else:
            return {"code":"0000","status":"success","msg":"get请求成功了"}
    except:
        return {"code":"10000","status":"failed","msg":message["10000"]}



@demobp.route("/postdemo",methods=["post"])
def postdemo():
    try:
        logger.info("进入")
        logger.info("postdemo")
        #获取参数可以用 如果参数有user_id request.json["user_id"].strip()   application-json
        user_id = request.json["user_id"].strip()
        if user_id:
            return {"code": "0000", "status": "success", "msg": "post获取用户id成功，user_id:%s" % user_id}
        else:
            return {"code":"0000","status":"success","msg":"post请求成功了"}
    except:
        return {"code":"10000","status":"failed","msg":message["10000"]}


@demobp.route("/deletedemo",methods=["delete"])
def deletedemo():
    try:
        # 获取参数可以用 如果参数有user_id request.json["user_id"].strip()
        user_id = request.json["user_id"].strip()
        if user_id:
            return {"code": "0000", "status": "success", "msg": "delete获取用户id成功，user_id:%s" % user_id}
        else:
            return {"code":"0000","status":"success","msg":"delete请求成功了"}
    except:
        return {"code":"10000","status":"failed","msg":message["10000"]}


@demobp.route("/putdemo",methods=["put"])
def putdemo():
    try:
        logger.info("进入putdemo")
        # 获取参数可以用 如果参数有user_id request.json["user_id"].strip()
        user_id = request.json["user_id"].strip()
        if user_id:
            return {"code": "0000", "status": "success", "msg": "put获取用户id成功，user_id:%s" % user_id}
        else:
            return {"code":"0000","status":"success","msg":"put请求成功,user_id:%s" %user_id}
    except:
        return {"code":"10000","status":"failed","msg":message["10000"]}

@demobp.route("/ciyun",methods=["GET"])
def ciyun_render():
    return render_template("ciyun.html")




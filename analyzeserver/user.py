# -*- coding: utf-8 -*-

# @Time : 2021/10/27 16:00

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : user.py

import sys
sys.path.append(".")
sys.path.append("../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import datetime

userbp = Blueprint('user', __name__, url_prefix='/user')

@userbp.route("/lh",methods=["post"])
def lh_user_add():
    try:
        logger.info(request.json)

        try:
            lh_user_id = request.json["user_id"].strip()
            lh_status = request.json["status"].strip()
            phone = request.json["phone"].strip()
            addtime = request.json["create_time"].strip()
            updtime = request.json["update_time"].strip()
        except:
            return {"code":"10004","status":"failed","msg":message["10004"]}


        if not lh_user_id or not lh_status or not phone or not addtime or not updtime:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        # 获取crm的用户信息
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return {"code":"10002","status":"failed","msg":message["10002"]}
        crm_data = ""
        with conn_crm.cursor() as cursor:
            sql = '''
            select a.sex,a.id unionid,a.pid parentid,a.phone,a.nickname,a.`name`,b.types auto_type,b.status vertify_status,b.address,b.birth,b.nationality,
            c.grade vip_grade,FROM_UNIXTIME(c.starttime,'%%Y-%%m-%%d %%H:%%i:%%s') vip_starttime,FROM_UNIXTIME(c.endtime,'%%Y-%%m-%%d %%H:%%i:%%s') vip_endtime,
            d.grade serpro_grade,d.status serpro_status,FROM_UNIXTIME(d.starttime,'%%Y-%%m-%%d %%H:%%i:%%s') serpro_starttime
            from luke_sincerechat.user a
            left join luke_crm.authentication b on a.id = b.unionid
            left join luke_crm.user_vip c on b.unionid = c.unionid
            left join luke_crm.user_serpro d on c.unionid = d.unionid
            where a.phone = %s'''
            cursor.execute(sql,(phone))
            crm_data = cursor.fetchone()
            if not crm_data:
                return {"code":10003,"status":"failed","msg":message["10003"]}
        conn_crm.close()
        param = [crm_data["unionid"],crm_data["parentid"],lh_user_id,lh_status,phone,addtime,updtime,crm_data["sex"],
                 crm_data["nickname"],crm_data["name"],crm_data["auto_type"],crm_data["vertify_status"],crm_data["address"],
                 crm_data["birth"],crm_data["nationality"],crm_data["vip_grade"],crm_data["vip_starttime"],crm_data["vip_endtime"],
                 crm_data["serpro_grade"],crm_data["serpro_status"],crm_data["serpro_starttime"],0,datetime.datetime.now()]

        logger.info(param)

        #准备插入分析库
        conn_rw = ssh_get_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
        with conn_rw.cursor() as cursor:
            insert_sql = '''insert into lh_user values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cursor.execute(insert_sql,param)
            conn_rw.commit()
        conn_rw.close()
        return {"code":0,"status":"success","msg":u"靓号数据同步成功"}

    except:
        logger.exception(traceback.format_exc())
        return {"code":"10000","status":"failed","msg":message["10000"]}

@userbp.route("crm",methods=["POST"])
def crm_user_add():
    try:
        logger.info(request.json)

        try:
            unionid = request.json["unionid"].strip()
            phone = request.json["phone"].strip()
        except:
            return {"code":"10004","status":"failed","msg":message["10004"]}

        if not unionid or not phone:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        # 获取crm的用户信息
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        crm_data = ""
        with conn_crm.cursor() as cursor:
            sql = '''
            select a.sex,a.id unionid,a.pid parentid,a.phone,a.nickname,a.`name`,a.`status`,b.types auto_type,b.status vertify_status,b.address,b.birth,b.nationality,
            c.grade vip_grade,FROM_UNIXTIME(c.starttime,'%%Y-%%m-%%d %%H:%%i:%%s') vip_starttime,FROM_UNIXTIME(c.endtime,'%%Y-%%m-%%d %%H:%%i:%%s') vip_endtime,
            d.grade serpro_grade,d.status serpro_status,FROM_UNIXTIME(d.starttime,'%%Y-%%m-%%d %%H:%%i:%%s') serpro_starttime
            from luke_sincerechat.user a
            left join luke_crm.authentication b on a.id = b.unionid
            left join luke_crm.user_vip c on b.unionid = c.unionid
            left join luke_crm.user_serpro d on c.unionid = d.unionid
            where a.id = %s'''

            cursor.execute(sql,(unionid))
            crm_data = cursor.fetchone()
        conn_crm.close()

        param = [crm_data["unionid"], crm_data["parentid"], phone, crm_data["status"],crm_data["nickname"], crm_data["name"],crm_data["sex"],
                 crm_data["birth"],crm_data["address"],crm_data["nationality"],crm_data["auto_type"], crm_data["vertify_status"],
                 crm_data["vip_grade"], crm_data["vip_starttime"],crm_data["vip_endtime"],
                 crm_data["serpro_grade"], crm_data["serpro_status"], crm_data["serpro_starttime"], 0,datetime.datetime.now()]

        logger.info(param)

        # 准备插入分析库
        conn_rw = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
        with conn_rw.cursor() as cursor:
            insert_sql = '''insert into crm_user values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cursor.execute(insert_sql, param)
            conn_rw.commit()
        conn_rw.close()
        return {"code": 0, "status": "success", "msg": u"crm数据同步成功"}


    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
# -*- coding: utf-8 -*-

# @Time : 2021/10/27 16:00

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : user.py

import sys
sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
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
        conn_rw = ssh_get_conn(lianghao_ssh_conf,analyze_mysql_conf)
        with conn_rw.cursor() as cursor:
            insert_sql = '''insert into lh_user values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cursor.execute(insert_sql,param)
            conn_rw.commit()
        conn_rw.close()
        return {"code":"0000","status":"success","msg":u"靓号数据同步成功"}

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
            where a.id = %s and a.phone = %s'''

            cursor.execute(sql,(unionid,phone))
            crm_data = cursor.fetchone()
        conn_crm.close()

        if not crm_data:
            return {"code":"10003","status":"failed","msg":message["10003"]}

        param = [crm_data["unionid"], crm_data["parentid"], phone, crm_data["status"],crm_data["nickname"], crm_data["name"],crm_data["sex"],
                 crm_data["birth"],crm_data["address"],crm_data["nationality"],crm_data["auto_type"], crm_data["vertify_status"],
                 crm_data["vip_grade"], crm_data["vip_starttime"],crm_data["vip_endtime"],
                 crm_data["serpro_grade"], crm_data["serpro_status"], crm_data["serpro_starttime"], 0,datetime.datetime.now()]

        logger.info(param)

        # 准备插入分析库
        conn_rw = ssh_get_conn(lianghao_ssh_conf, analyze_mysql_conf)
        with conn_rw.cursor() as cursor:
            insert_sql = '''insert into crm_user values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            cursor.execute(insert_sql, param)
            conn_rw.commit()
        conn_rw.close()
        return {"code": "0000", "status": "success", "msg": u"crm数据同步成功"}


    except:
        logger.exception(traceback.format_exc())
        return {"code":"10000","status":"failed","msg":message["10000"]}

@userbp.route('/bus', methods=["POST"])
def bus_user_add():
    try:
        # 接收参数
        logger.info("新增用户")
        logger.info(request.json)
        phone = request.json.get('phone', "").strip()
        # unionid = request.json.get('unionid', "").strip()
        if not phone:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        # 建立连接
        crm_mysql_conf['db'] = 'luke_lukebus'
        conn_bus = direct_get_conn(crm_mysql_conf)
        if not conn_bus:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        # 获取禄可商务用户信息sql
        bus_user_find_sql = '''
            select
                t1.unionid, t1.punionid parentid, t1.phone, t1.u_nickname nickname, t1.capacity, t1.`exclusive`, t1.`status` bus_status,
                FROM_UNIXTIME(t1.addtime,'%%Y-%%m-%%d %%H:%%i:%%s') addtime, FROM_UNIXTIME(t1.udptime,'%%Y-%%m-%%d %%H:%%i:%%s') udptime,
                FROM_UNIXTIME(t1.invtime,'%%Y-%%m-%%d %%H:%%i:%%s') invtime, t2.identity, t2.types auto_type, t2.`status` vertify_status,
                t2.sex, t2.`name`, t2.address, t2.birth, t2.nationality, FROM_UNIXTIME(t2.extime,'%%Y-%%m-%%d %%H:%%i:%%s') extime,
                t3.grade provider_garde, t3.`status` provider_status, FROM_UNIXTIME(t3.starttime,'%%Y-%%m-%%d %%H:%%i:%%s') provider_starttime,
                t4.grade vip_grade, FROM_UNIXTIME(t4.starttime,'%%Y-%%m-%%d %%H:%%i:%%s')  vip_starttime, FROM_UNIXTIME(t4.endtime,'%%Y-%%m-%%d %%H:%%i:%%s')  vip_endtime
            from
                    luke_lukebus.`user` t1
            left join
                luke_crm.authentication t2
            on
                t1.unionid = t2.unionid
            left join
                luke_crm.user_serpro t3
            on
                t2.unionid = t3.unionid
            left join
                luke_crm.user_vip t4
            on
                t3.unionid = t4.unionid
            where
                t1.phone = %s
        '''
        with conn_bus.cursor() as cursor:
            cursor.execute(bus_user_find_sql, (phone,))
            bus_user_data = cursor.fetchone()
        conn_bus.close()
        if not bus_user_data:
            return {"code": "10005", "status": "failed", "msg": message["10005"]}
        logger.info(bus_user_data)

        # 禄可商户用户插入列
        bus_user_param = [bus_user_data['unionid'], bus_user_data['parentid'], bus_user_data['name'], bus_user_data['nickname'], bus_user_data['sex'], bus_user_data['phone'],
                     bus_user_data['identity'], bus_user_data['auto_type'], bus_user_data['vertify_status'], bus_user_data['address'], bus_user_data['birth'], bus_user_data['nationality'], bus_user_data['capacity'],
                     bus_user_data['exclusive'], bus_user_data['bus_status'], bus_user_data['provider_garde'], bus_user_data['provider_status'], bus_user_data['provider_starttime'], bus_user_data['vip_grade'], bus_user_data['vip_starttime'],
                     bus_user_data['vip_endtime'], bus_user_data['addtime'], bus_user_data['udptime'], bus_user_data['invtime'], bus_user_data['extime'], 0, time.strftime("%Y-%m-%d %H:%M:%S")]
        logger.info(bus_user_param)

        # 数据同步
        conn_bus_rw = ssh_get_conn(lianghao_ssh_conf, analyze_mysql_conf)
        with conn_bus_rw.cursor() as cursor:
            bus_user_insert_sql = '''insert into bus_user values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            cursor.execute(bus_user_insert_sql, bus_user_param)
        conn_bus_rw.commit()
        conn_bus_rw.close()
        return {"code": "0", "status": "success", "msg": message["0"]}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}


@userbp.route('/operationcenter', methods=["POST"])
def bus_operationcenter_add():
    try:
        logger.info('新增运营中心')
        logger.info(request.json)
        #　接收参数
        unionid = request.json.get('unionid', "").strip()
        if not unionid:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        # 建立连接
        crm_mysql_conf['db'] = 'luke_lukebus'
        conn_bus_center = direct_get_conn(crm_mysql_conf)
        if not conn_bus_center:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # 获取禄可商务运营中心
        center_find_sql = '''
            select
                unionid, punionid parentid, capacity, `name`, telephone phone, operatename, is_factory, status, crm, from_unixtime(addtime, '%%Y-%%m-%%d %%H:%%i:%%s') addtime
            from
                luke_lukebus.operationcenter
            where
                unionid = %s
            '''
        with conn_bus_center.cursor() as cursor:
            cursor.execute(center_find_sql, (unionid,))
            bus_center_data = cursor.fetchone()
        conn_bus_center.close()
        if not bus_center_data:
            return {"code": "10005", "status": "failed", "msg": message["10005"]}

        # 禄可商务运营中心插入列
        bus_center_param = [bus_center_data['unionid'], bus_center_data['parentid'], bus_center_data['capacity'], bus_center_data['name'],
                            bus_center_data['phone'], bus_center_data['operatename'], bus_center_data['is_factory'], bus_center_data['status'],
                            bus_center_data['crm'], bus_center_data['addtime'], time.strftime("%Y-%m-%d %H:%M:%S")]

        # 数据同步
        conn_bus_rw = ssh_get_conn(lianghao_ssh_conf, analyze_mysql_conf)
        with conn_bus_rw.cursor() as cursor:
            bus_center_insert_sql = '''insert into bus_operationcenter values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            cursor.execute(bus_center_insert_sql, bus_center_param)
        conn_bus_rw.commit()
        conn_bus_rw.close()
        return {"code": "0", "status": "success", "msg": message["0"]}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

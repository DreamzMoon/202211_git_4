# -*- coding: utf-8 -*-

# @Time : 2021/11/30 9:28

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : crmuserrelate.py

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


userrelatebp = Blueprint('userrelate', __name__, url_prefix='/user/relate')

@userrelatebp.route("mes",methods=["POST"])
def user_relate_mes():
    try:
        conn = direct_get_conn(analyze_mysql_conf)

        logger.info(request.json)
        # 参数个数错误
        if len(request.json) != 10:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        keyword = request.json["keyword"]
        bus_id = request.json["bus_id"]
        parent = request.json["parent"]
        serpro_grade = request.json["serpro_grade"]
        capacity = request.json["capacity"]
        bus_parent = request.json["bus_parent"]
        page = request.json["page"]
        size = request.json["size"]

        phone_lists = request.json["phone_lists"]

        code_page = ""
        code_size = ""
        if page and size:
            code_page = (page - 1) * size
            code_size = size


        #先默认查全部
        cursor = conn.cursor()
        sql = '''select nickname,phone,unionid,parent_phone,operatename,bus_phone,parentid,capacity,bus_parentid,operatenamedirect,direct_bus_phone, vip_grade,vip_starttime,vip_endtime,
        serpro_grade,serpro_status
        from crm_user_%s where del_flag = 0''' %current_time
        count_sql = '''select count(*) count
        from crm_user_%s where del_flag = 0''' %current_time

        if phone_lists:
            phone_lists = ",".join(phone_lists)
            phone_lists_sql = ''' and phone in (%s)''' %phone_lists
            sql = sql + phone_lists_sql
            count_sql = count_sql +phone_lists_sql
        else:
            if keyword:
                keyword_sql = ''' and (nickname like "%s" or phone like "%s" or unionid like "%s")''' %("%"+keyword+"%","%"+keyword+"%","%"+keyword+"%")
                sql = sql + keyword_sql
                count_sql = count_sql + keyword_sql


            if bus_id:
                bus_sql = ''' and operate_id = %s''' %(bus_id)
                sql = sql + bus_sql
                count_sql = count_sql + bus_sql

            if parent:
                parent_sql = ''' and (parentid = %s or parent_phone=%s)''' %(parent,parent)
                sql = sql + parent_sql
                count_sql = count_sql + parent_sql

            if serpro_grade:
                serpro_grade_sql = ''' and serpro_grade = %s''' %serpro_grade
                sql = sql + serpro_grade_sql
                count_sql = count_sql + serpro_grade_sql

            if capacity:
                capacity_sql = ''' and capacity = %s''' %capacity
                sql = sql + capacity_sql
                count_sql = count_sql + capacity_sql

            if bus_parent:
                bus_parentid_sql = ''' and (bus_parentid = %s or bus_parent_phone = %s)''' %(bus_parent,bus_parent)
                sql = sql + bus_parentid_sql
                count_sql = count_sql + bus_parentid_sql

        logger.info("code_page:%s" % code_page)
        logger.info("code_size:%s" % code_size)
        if page and size:
            limit_sql = ''' limit %s,%s''' %(code_page,code_size)
            sql = sql + limit_sql


        cursor.execute(count_sql)
        count = cursor.fetchone()[0]

        logger.info(sql)
        cursor.execute(sql)
        datas = cursor.fetchall()
        last_datas = []
        for data in datas:
            data_dict = {}
            data_dict["nickname"] = data[0]
            data_dict["phone"] = data[1]
            data_dict["unionid"] = data[2]
            data_dict["parent_phone"] = data[3]
            data_dict["operatename"] = data[4]
            data_dict["bus_phone"] = data[5]
            data_dict["parentid"] = data[6]
            data_dict["capacity"] = data[7]
            data_dict["bus_parentid"] = data[8]
            data_dict["operatenamedirect"] = data[9]
            data_dict["direct_bus_phone"] = data[10]
            data_dict["vip_grade"] = data[11]
            data_dict["vip_starttime"] = data[12].strftime("%Y-%m-%d %H:%M:%S") if data[11] else ""
            data_dict["vip_endtime"] = data[13].strftime("%Y-%m-%d %H:%M:%S") if data[12] else ""
            data_dict["serpro_grade"] = data[14]
            data_dict["serpro_status"] = data[15]
            last_datas.append(data_dict)


        return {"code":"0000","msg":last_datas,"count":count,"status":"success"}

    except Exception as e:
        logger.error(e)
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}


@userrelatebp.route("basicmes",methods=["POST"])
def user_relate_basicmes():
    try:
        conn = direct_get_conn(analyze_mysql_conf)

        logger.info(request.json)
        # 参数个数错误
        if len(request.json) != 9:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        keyword = request.json["keyword"]
        status = request.json["status"]
        vertify_status = request.json["vertify_status"]
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]
        page = request.json["page"]
        size = request.json["size"]
        phone_lists = request.json["phone_lists"]

        code_page = ""
        code_size = ""
        if page and size:
            code_page = (page - 1) * size
            code_size = size

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        # 时间判断
        if start_time and end_time:

            if start_time >= end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}

        logger.info("current_time:%s" %current_time)
        sql = '''select phone,unionid,nickname,`name`,sex,birth,nationality,vertify_status,huoti_status,addtime,`status` from crm_user_%s where del_flag = 0 ''' %current_time
        count_sql = '''select count(*) count from crm_user_%s where del_flag = 0 ''' %current_time


        if phone_lists:
            phone_lists = ",".join(phone_lists)
            phone_lists_sql = ''' and phone in (%s)''' %phone_lists
            sql = sql + phone_lists_sql
            count_sql = count_sql +phone_lists_sql
        else:

            if keyword:
                # keyword_sql = ''' and (nickname like "%s" or phone like "%s" or unionid like "%s" or name like "%s")''' % ("%" + keyword + "%", "%" + keyword + "%", "%" + keyword + "%", "%" + keyword + "%")
                keyword_sql = ''' and (nickname like "%s" or phone like "%s" or unionid like "%s")''' % ("%" + keyword + "%", "%" + keyword + "%", "%" + keyword + "%")
                sql = sql + keyword_sql
                count_sql = count_sql + keyword_sql

            if status:
                status_sql = ''' and status = %s''' %status
                sql = sql + status_sql
                count_sql = count_sql + status_sql

            if vertify_status:
                vertify_sql = ''' and vertify_status = %s''' %vertify_status
                sql = sql + vertify_sql
                count_sql = count_sql + vertify_sql

            if start_time and end_time:
                time_sql = ''' and addtime >= "%s" and addtime <= "%s"''' %(start_time,end_time)
                sql = sql + time_sql
                count_sql = count_sql + time_sql

            if page and size:
                limit_sql = ''' limit %s,%s''' %(code_page,code_size)
                sql = sql + limit_sql

        logger.info(sql)
        logger.info(count_sql)

        cursor = conn.cursor()

        cursor.execute(count_sql)
        count = cursor.fetchone()[0]

        cursor.execute(sql)
        datas = cursor.fetchall()
        last_datas = []
        for data in datas:
            data_dict = {}
            data_dict["phone"] = data[0]
            data_dict["unionid"] = data[1]
            data_dict["nickname"] = data[2]
            data_dict["name"] = data[3]
            data_dict["sex"] = data[4]
            data_dict["birth"] = data[5].strftime("%Y-%m-%d %H:%M:%S") if data[5] else ""
            data_dict["nationality"] = data[6]
            data_dict["vertify_status"] = data[7]
            data_dict["huoti_status"] = data[8]
            data_dict["addtime"] = data[9].strftime("%Y-%m-%d %H:%M:%S") if data[9] else ""
            data_dict["status"] = data[10]
            last_datas.append(data_dict)

        return {"code": "0000", "msg": last_datas, "count": count, "status": "success"}
    except Exception as e:
        logger.error(e)
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}


@userrelatebp.route("detailmes",methods=["POST"])
def user_relate_detail():
    try:
        conn = direct_get_conn(analyze_mysql_conf)

        # 参数个数错误
        if len(request.json) != 2:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        unionid = request.json["unionid"]


        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        sql = '''select * from crm_user_%s where del_flag = 0 and unionid = %s''' %(current_time,unionid)
        data = pd.read_sql(sql,conn)

        data = data.to_dict("records")
        data = data[0]
        logger.info(data)
        data["addtime"] = data["addtime"].strftime("%Y-%m-%d %H:%M:%S") if data["addtime"] else ""
        data["birth"] = data["birth"].strftime("%Y-%m-%d %H:%M:%S") if data["birth"] else ""
        data["serpro_starttime"] = data["serpro_starttime"].strftime("%Y-%m-%d %H:%M:%S") if data["serpro_starttime"] else ""
        data["addtime"] = data["vip_starttime"].strftime("%Y-%m-%d %H:%M:%S") if data["vip_starttime"] else ""
        data["vip_endtime"] = data["vip_endtime"].strftime("%Y-%m-%d %H:%M:%S") if data["vip_endtime"] else ""
        data["vip_starttime"] = data["vip_starttime"].strftime("%Y-%m-%d %H:%M:%S") if data["vip_starttime"] else ""

        return {"code": "0000", "msg": data, "status": "success"}
    except Exception as e:
        logger.error(e)
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}



@userrelatebp.route("update/user/ascription",methods=["POST"])
def update_user_ascriptions():
    try:
        conn = direct_get_conn(analyze_mysql_conf)
        cursor = conn.cursor()
        conn_crm = direct_get_conn(crm_mysql_conf)
        cursor_crm = conn_crm.cursor()
        try:
            token = request.headers["Token"]
            user_id = request.json.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        #直属运营中心修改 传直属运营中心id
        operate_direct_id = request.json.get("operate_direct_id")

        #支持crm的运营中心修改 传crm运营中心的id
        operate_id = request.json.get("operate_id")

        #上级修改传手机号码
        parent_phone = request.json.get("parent_phone")

        #运营中心的上级修改 传手机号码
        bus_parent_phone = request.json.get("bus_parent_phone")

        unionid = request.json.get("unionid")

        # 更新crm
        update_crm = '''update crm_user_{} set'''.format(current_time)
        crm_where = ''' where unionid = %s''' %unionid
        crm_condition = []

        #更新统计表
        update_daily_order_sql = '''update user_daily_order_data set'''
        update_store_vas_sql = '''update user_storage_value set'''
        update_store_vasto_sql = '''update user_storage_value_today set'''
        update_where = ''' where unionid = %s''' %unionid

        statistic_condition = []
        logger.info("unionid:%s" %unionid)
        # 如果要改上级的话 需要看有没有递归 判断要修改的手机号码是不是在下级里面
        if parent_phone:
            sql = '''
            select a.id,a.phone,a.pid from 
            (WITH RECURSIVE temp as (
            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE id = %s
            UNION ALL
            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id
            )
            SELECT * FROM temp
            )a left join luke_lukebus.operationcenter b
            on a.id = b.unionid 
            where a.phone != "" and a.phone is not null
            ''' %unionid
            logger.info(sql)
            cursor_crm.execute(sql)
            datas = cursor_crm.fetchall()
            logger.info(datas)
            pid = datas[0]["id"]
            below_phone = []
            for data in datas:
                if data["pid"] == pid:
                    continue
                else:
                    below_phone.append(data["phone"])

            if parent_phone in below_phone:
                return {"code":"11028","msg":message["11028"],"status":"failed"}


        if bus_parent_phone:
            sql = '''
            select a.id,a.phone,a.pid from 
            (WITH RECURSIVE temp as (
            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE id = %s
            UNION ALL
            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id
            )
            SELECT * FROM temp
            )a left join luke_lukebus.operationcenter b
            on a.id = b.unionid 
            where a.phone != "" and a.phone is not null
            ''' %unionid
            cursor_crm.execute(sql)
            datas = cursor_crm.fetchall()
            pid = datas[0]["id"]
            below_phone = []
            for data in datas:
                if data["pid"] == pid:
                    continue
                else:
                    below_phone.append(data["phone"])

            if bus_parent_phone in below_phone:
                return {"code":"11028","msg":message["11028"],"status":"failed"}



        if operate_direct_id:
            #先去禄可商务拿运营中心的信息
            operate_sql = '''select * from luke_lukebus.operationcenter where id = %s''' %operate_direct_id
            bus_data = pd.read_sql(operate_sql,conn_crm).to_dict("records")[0]
            crm_condition.append(''' operatenamedirect = "%s",direct_bus_phone= "%s",direct_leader = "%s",direct_leader_unionid = "%s",operate_direct_id = %s ''' %(bus_data["operatename"],bus_data["telephone"],bus_data["name"],bus_data["unionid"],operate_direct_id))

        if operate_id:
            # 先去禄可商务拿运营中心的信息
            operate_sql = '''select * from luke_lukebus.operationcenter where id = %s''' %operate_id
            bus_data = pd.read_sql(operate_sql, conn_crm).to_dict("records")[0]
            crm_condition.append(''' operatename = "%s",bus_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s ''' % (bus_data["operatename"], bus_data["telephone"], bus_data["name"], bus_data["unionid"], operate_id))

            statistic_condition.append(''' operate_id="%s",operatename="%s",leader_unionid="%s",leader="%s",leader_phone="%s" ''' %(operate_id,bus_data["operatename"],bus_data["unionid"], bus_data["name"],bus_data["telephone"]))

        if parent_phone:
            user_sql = '''select * from luke_sincerechat.user where phone = %s''' %(parent_phone)
            user_data = pd.read_sql(user_sql,conn_crm).to_dict("records")[0]
            if not user_data:
                return {"code":"11029","msg":message["11029"],"status":"failed"}
            crm_condition.append(''' parent_phone="%s",parent_nickname="%s",parent_name="%s",parentid="%s" ''' %(parent_phone,user_data["nickname"],user_data["name"],user_data["id"]))
            statistic_condition.append(''' parentid="%s",parent_phone="%s" ''' %(user_data["id"],parent_phone))

        if bus_parent_phone:
            user_sql = '''select * from luke_sincerechat.user where phone = %s''' %(parent_phone)
            user_data = pd.read_sql(user_sql,conn_crm).to_dict("records")[0]
            if not user_data:
                return {"code":"11029","msg":message["11029"],"status":"failed"}
            crm_condition.append(''' bus_parent_phone="%s",bus_parent_nickname="%s",bus_parent_name="%s",bus_parentid="%s" ''' %(parent_phone,user_data["nickname"],user_data["name"],user_data["id"]))

        crm_sql_condition = ",".join(crm_condition)
        statistic_condition_sql = ",".join(statistic_condition)

        logger.info("crm_sql_condition:%s" %crm_sql_condition)
        logger.info("statistic_condition_sql:%s" %statistic_condition_sql)

        update_daily_order_sql = update_daily_order_sql + statistic_condition_sql + update_where
        update_store_vas_sql = update_store_vas_sql + statistic_condition_sql + update_where
        update_store_vasto_sql = update_store_vasto_sql + statistic_condition_sql + update_where

        update_crm = update_crm + crm_sql_condition + crm_where
        logger.info("update_crm_sql:%s" %update_crm)
        logger.info("update_daily_order_sql:%s" %update_daily_order_sql)
        logger.info("update_store_vas_sql:%s" %update_store_vas_sql)
        logger.info("update_store_vasto_sql:%s" %update_store_vasto_sql)

        cursor.execute(update_crm)
        logger.info("执行成功")
        cursor.execute(update_daily_order_sql)
        logger.info("执行成功")
        cursor.execute(update_store_vas_sql)
        logger.info("执行成功")
        cursor.execute(update_store_vasto_sql)
        logger.info("执行成功")
        conn.commit()

        return {"code": "0000", "msg": "更新成功", "status": "success"}
    except Exception as e:
        conn.rollback()
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn.close()
        conn_crm.close()
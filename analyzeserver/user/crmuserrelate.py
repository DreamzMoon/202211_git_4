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
        t1 = int(time.time())
        conn = direct_get_conn(analyze_mysql_conf)

        logger.info(request.json)


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
        tag_id = request.json.get("tag_id")

        code_page = ""
        code_size = ""
        if page and size:
            code_page = (page - 1) * size
            code_size = size


        #先默认查全部
        cursor = conn.cursor()
        # sql = '''select nickname,phone,unionid,parent_phone,operatename,bus_phone,parentid,capacity,bus_parentid,operatenamedirect,direct_bus_phone, vip_grade,vip_starttime,vip_endtime,
        # serpro_grade,serpro_status
        # from crm_user where del_flag = 0'''
        sql = '''
        select nickname,phone,crm_user.unionid,parent_phone,operatename,bus_phone,parentid,capacity,bus_parentid,operatenamedirect,direct_bus_phone, vip_grade,vip_starttime,vip_endtime,
        serpro_grade,serpro_status,GROUP_CONCAT(crm_tag.tag_name) tag_name
        from crm_user
        left join crm_user_tag on crm_user.unionid = crm_user_tag.unionid
        left join crm_tag on crm_user_tag.tag_id = crm_tag.id
        where crm_user.del_flag = 0
        '''

        group_sql = ''' group by crm_user.unionid '''

        if not tag_id:
            logger.info("走这个")
            # count_sql = '''
            # select count(*) count
            # from crm_user
            # left join crm_user_tag on crm_user.unionid = crm_user_tag.unionid
            # left join crm_tag on crm_user_tag.tag_id = crm_tag.id
            # where crm_user.del_flag = 0
            # '''

            count_sql = '''
                        select count(*) count
                        from crm_user
                        where crm_user.del_flag = 0
                        '''

        else:
            # 因为分组的关系 所以不适合上面那种
            count_sql = '''
            select crm_user.unionid,GROUP_CONCAT(crm_tag.tag_name) tag_name
            from crm_user
            left join crm_user_tag on crm_user.unionid = crm_user_tag.unionid
            left join crm_tag on crm_user_tag.tag_id = crm_tag.id
            where crm_user.del_flag = 0
            '''


        if phone_lists:
            phone_lists = ",".join(phone_lists)
            phone_lists_sql = ''' and phone in (%s)''' %phone_lists
            sql = sql + phone_lists_sql
            count_sql = count_sql +phone_lists_sql
        else:
            if keyword:
                keyword_sql = ''' and (nickname like "%s" or phone like "%s" or crm_user.unionid like "%s")''' %("%"+keyword+"%","%"+keyword+"%","%"+keyword+"%")
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

        sql = sql + group_sql


        if tag_id:
            count_sql = count_sql + group_sql
            logger.info("tag_id:%s" %tag_id)
            tag_sql = '''select tag_name from crm_tag where id = %s''' %tag_id
            cursor.execute(tag_sql)
            tag_name = cursor.fetchone()[0]
            having_sql = ''' having tag_name like "%s"''' %("%"+tag_name+"%")
            sql = sql + having_sql
            count_sql = count_sql + having_sql
            logger.info(sql)
            logger.info(count_sql)

        if page and size:
            limit_sql = ''' limit %s,%s''' %(code_page,code_size)
            sql = sql + limit_sql

        cursor.execute(sql)
        datas = cursor.fetchall()

        if tag_id:
            count = cursor.execute(count_sql)
        else:
            cursor.execute(count_sql)
            count = cursor.fetchone()[0]



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
            data_dict["tag_name"] = data[16]
            last_datas.append(data_dict)


        return {"code":"0000","msg":last_datas,"count":count,"status":"success"}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
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
        sql = '''select phone,unionid,nickname,`name`,sex,birth,nationality,vertify_status,huoti_status,addtime,`status` from crm_user where del_flag = 0 '''
        count_sql = '''select count(*) count from crm_user where del_flag = 0 '''


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

        sql = '''select crm_user.*,crm_user_info.identity,crm_user_info.identify_front,crm_user_info.identify_back,crm_user_info.face_pic,crm_user_info.usericon,crm_user_info.issue,crm_user_info.province_code,crm_user_info.city_code,crm_user_info.region_code,crm_user_info.town_code,crm_user_info.address_detail,GROUP_CONCAT(crm_tag.tag_name) tag_name from crm_user 
                left join crm_user_info on crm_user.unionid = crm_user_info.unionid
                left join crm_user_tag on crm_user.unionid = crm_user_tag.unionid
                left join crm_tag on crm_tag.id = crm_user_tag.tag_id
                where crm_user.del_flag = 0 and crm_user.unionid = %s
                group by crm_user.unionid
                ''' % unionid
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

# @userrelatebp.route("update/user/ascription",methods=["POST"])
# def update_user_ascriptions():
#     try:
#         conn = direct_get_conn(analyze_mysql_conf)
#         cursor = conn.cursor()
#         conn_crm = direct_get_conn(crm_mysql_conf)
#         cursor_crm = conn_crm.cursor()
#         try:
#             token = request.headers["Token"]
#             user_id = request.json.get("user_id")
#
#             if not user_id and not token:
#                 return {"code": "10001", "status": "failed", "msg": message["10001"]}
#
#             check_token_result = check_token(token, user_id)
#             if check_token_result["code"] != "0000":
#                 return check_token_result
#         except:
#             return {"code": "10004", "status": "failed", "msg": message["10004"]}
#
#         #直属运营中心修改 传直属运营中心id
#         operate_direct_id = request.json.get("operate_direct_id")
#
#         #支持crm的运营中心修改 传crm运营中心的id
#         operate_id = request.json.get("operate_id")
#
#         #上级修改传手机号码
#         parent_phone = request.json.get("parent_phone")
#
#         #运营中心的上级修改 传手机号码
#         bus_parent_phone = request.json.get("bus_parent_phone")
#
#         unionid_lists = request.json.get("unionid_lists")
#
#         # 如果要改上级的话 需要看有没有递归 判断要修改的手机号码是不是在下级里面
#         if parent_phone:
#             for unionid in unionid_lists:
#                 logger.info(unionid)
#                 # sql = '''
#                 #             select * from (
#                 #                 select a.*, if (crm =0, Null, b.operatename) operatename, b.id operateid from
#                 #                 (WITH RECURSIVE temp as (
#                 #                 SELECT t.unionid id,t.parentid pid,t.phone,t.nickname,t.name FROM crm_user t WHERE unionid = %s
#                 #                 UNION ALL
#                 #                 SELECT t1.unionid id,t1.parentid pid,t1.phone, t1.nickname,t1.name FROM crm_user t1 INNER JOIN temp ON t1.parentid = temp.id
#                 #                 )
#                 #                 SELECT * FROM temp
#                 #                 )a left join operationcenter b
#                 #                 on a.id = b.unionid
#                 #                 ) t where pid != %s and id != %s and phone is not null and phone !=""
#                 #             ''' % (unionid, unionid, unionid)
#                 sql = '''
#                                             select * from (
#                                                 select a.*, if (crm =0, Null, b.operatename) operatename, b.id operateid from
#                                                 (WITH RECURSIVE temp as (
#                                                 SELECT t.unionid id,t.parentid pid,t.phone,t.nickname,t.name FROM crm_user t WHERE unionid = %s
#                                                 UNION ALL
#                                                 SELECT t1.unionid id,t1.parentid pid,t1.phone, t1.nickname,t1.name FROM crm_user t1 INNER JOIN temp ON t1.parentid = temp.id
#                                                 )
#                                                 SELECT * FROM temp
#                                                 )a left join operationcenter b
#                                                 on a.id = b.unionid
#                                                 ) t where  phone is not null and phone !=""
#                                             ''' % (unionid)
#                 logger.info(sql)
#                 cursor.execute(sql)
#                 datas = cursor.fetchall()
#                 logger.info(datas)
#                 below_phone = [data[2] for data in datas]
#                 logger.info(below_phone)
#                 if parent_phone in below_phone:
#                     return {"code": "11028", "msg": "用户："+str(unionid)+":"+message["11028"], "status": "failed"}
#
#
#         if bus_parent_phone:
#             for unionid in unionid_lists:
#                 sql = '''
#                 select * from (
#                     select a.*, if (crm =0, Null, b.operatename) operatenamedirect, b.id operate_direct_id from
#                     (WITH RECURSIVE temp as (
#                     SELECT t.unionid id,t.bus_parentid pid,t.phone,t.nickname,t.name FROM crm_user t WHERE unionid = %s
#                     UNION ALL
#                     SELECT t1.unionid id,t1.bus_parentid pid,t1.phone, t1.nickname,t1.name FROM crm_user t1 INNER JOIN temp ON t1.bus_parentid = temp.id
#                     )
#                     SELECT * FROM temp
#                     )a left join operationcenter b
#                     on a.id = b.unionid
#                     ) t where  phone is not null and phone !=""
#                 ''' %(unionid)
#
#                 cursor.execute(sql)
#                 datas = cursor.fetchall()
#                 below_phone = [data[2] for data in datas]
#
#                 if bus_parent_phone in below_phone:
#                     return {"code": "11028", "msg": "用户："+str(unionid)+":"+message["11028"], "status": "failed"}
#         all_compare = []
#         #原用户数据 用户对比旧数据
#         for unionid in unionid_lists:
#             # 更新crm
#             update_crm = '''update crm_user '''
#             crm_where = ''' where unionid = %s''' %unionid
#             crm_condition = []
#
#             # 更新统计表
#             update_daily_order_sql = '''update user_daily_order_data '''
#             update_store_vas_sql = '''update user_storage_value '''
#             update_store_vasto_sql = '''update user_storage_value_today '''
#             update_where = ''' where unionid = %s''' %unionid
#
#             statistic_condition = []
#
#             select_sql = '''select * from crm_user where unionid = %s and del_flag = 0''' %(unionid)
#             old_data = pd.read_sql(select_sql,conn)
#             logger.info(old_data)
#             old_data = old_data.to_dict("records")[0]
#             old_operate_id = old_data["operate_id"]
#             old_operate_direct_id = old_data["operate_direct_id"]
#             old_parent_phone = old_data["parent_phone"]
#             old_bus_parent_phone = old_data["bus_parent_phone"]
#
#
#
#             if operate_direct_id:
#                 #先去禄可商务拿运营中心的信息
#                 operate_sql = '''select * from operationcenter where id = %s''' %operate_direct_id
#                 bus_data = pd.read_sql(operate_sql,conn).to_dict("records")[0]
#                 crm_condition.append(''' operatenamedirect = "%s",direct_bus_phone= "%s",direct_leader = "%s",direct_leader_unionid = "%s",operate_direct_id = %s ''' %(bus_data["operatename"],bus_data["telephone"],bus_data["name"],bus_data["unionid"],operate_direct_id))
#
#             if operate_id:
#                 # 先去禄可商务拿运营中心的信息
#                 operate_sql = '''select * from operationcenter where id = %s''' %operate_id
#                 bus_data = pd.read_sql(operate_sql, conn).to_dict("records")[0]
#                 crm_condition.append(''' operatename = "%s",bus_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s ''' % (bus_data["operatename"], bus_data["telephone"], bus_data["name"], bus_data["unionid"], operate_id))
#
#                 statistic_condition.append(''' operate_id="%s",operatename="%s",leader_unionid="%s",leader="%s",leader_phone="%s" ''' %(operate_id,bus_data["operatename"],bus_data["unionid"], bus_data["name"],bus_data["telephone"]))
#
#             if parent_phone:
#                 user_sql = '''select * from crm_user where phone = %s''' %(parent_phone)
#                 user_data = pd.read_sql(user_sql,conn)
#                 if user_data.shape[0] == 0:
#                     return {"code":"11029","msg":message["11029"],"status":"failed"}
#                 user_data = user_data.to_dict("records")[0]
#                 crm_condition.append(''' parent_phone="%s",parent_nickname="%s",parent_name="%s",parentid="%s" ''' %(parent_phone,user_data["nickname"],user_data["name"],user_data["unionid"]))
#                 statistic_condition.append(''' parentid="%s",parent_phone="%s" ''' %(user_data["unionid"],parent_phone))
#
#             if bus_parent_phone:
#                 user_sql = '''select * from crm_user where phone = %s''' %(bus_parent_phone)
#                 user_data = pd.read_sql(user_sql, conn)
#                 if user_data.shape[0] == 0:
#                     return {"code": "11029", "msg": message["11029"], "status": "failed"}
#                 user_data = user_data.to_dict("records")[0]
#                 crm_condition.append(''' bus_parent_phone="%s",bus_parent_nickname="%s",bus_parent_name="%s",bus_parentid="%s" ''' %(bus_parent_phone,user_data["nickname"],user_data["name"],user_data["unionid"]))
#
#             crm_sql_condition = ",".join(crm_condition)
#             statistic_condition_sql = ",".join(statistic_condition)
#
#             logger.info("crm_sql_condition:%s" %crm_sql_condition)
#             logger.info("statistic_condition_sql:%s" %statistic_condition_sql)
#
#             if statistic_condition_sql:
#                 update_daily_order_sql = update_daily_order_sql + " set " + statistic_condition_sql + update_where
#                 update_store_vas_sql = update_store_vas_sql + " set " + statistic_condition_sql + update_where
#                 update_store_vasto_sql = update_store_vasto_sql + " set " + statistic_condition_sql + update_where
#
#                 logger.info(update_daily_order_sql)
#
#                 cursor.execute(update_daily_order_sql)
#                 logger.info("执行成功")
#                 cursor.execute(update_store_vas_sql)
#                 logger.info("执行成功")
#                 cursor.execute(update_store_vasto_sql)
#                 logger.info("执行成功")
#
#             if crm_sql_condition:
#                 update_crm = update_crm + " set " + crm_sql_condition + crm_where
#                 cursor.execute(update_crm)
#                 logger.info("执行成功")
#
#             logger.info("update_crm_sql:%s" % update_crm)
#             logger.info("update_daily_order_sql:%s" % update_daily_order_sql)
#             logger.info("update_store_vas_sql:%s" % update_store_vas_sql)
#             logger.info("update_store_vasto_sql:%s" % update_store_vasto_sql)
#
#             # conn.commit()
#
#             # 日志接入
#             compare = []
#             if operate_direct_id:
#                 if not old_operate_direct_id:
#                     sql = '''select operatename from operationcenter where id = %s''' % (operate_direct_id)
#                     cursor.execute(sql)
#                     operatena = cursor.fetchall()
#                     operate_direct_operatena = operatena[0][0]
#                     compare.append("运营中心由 %s 变更为 %s" % ("-", operate_direct_operatena))
#                 elif int(operate_direct_id) != int(old_operate_direct_id):
#                     sql = '''select operatename from operationcenter where id in (%s,%s)''' %(old_operate_direct_id,operate_direct_id)
#                     cursor.execute(sql)
#                     operatena = cursor.fetchall()
#                     logger.info(operatena)
#                     old_operate_direct_operatena = operatena[0][0]
#                     operate_direct_operatena = operatena[1][0]
#                     compare.append("运营中心由 %s 变更为 %s" %(old_operate_direct_operatena,operate_direct_operatena))
#             if operate_id:
#                 if not old_operate_id:
#                     sql = '''select operatename from operationcenter where id = %s''' % (operate_id)
#                     cursor.execute(sql)
#                     operatena = cursor.fetchall()
#                     operate_operatena = operatena[0][0]
#                     compare.append("运营中心由 %s 变更为 %s" % ("-", operate_operatena))
#                 elif int(operate_id) != int(old_operate_id):
#                     sql = '''select operatename from operationcenter where id in (%s,%s)''' % (old_operate_id, operate_id)
#                     cursor.execute(sql)
#                     operatena = cursor.fetchall()
#                     old_operate_operatena = operatena[0][0]
#                     operate_operatena = operatena[1][0]
#                     compare.append("支持crm运营中心由 %s 变更为 %s" % (old_operate_operatena, operate_operatena))
#             if parent_phone:
#                 if not old_parent_phone:
#                     compare.append("上级手机号码由 %s 变更为 %s" % ("-", parent_phone))
#                 elif int(parent_phone) != int(old_parent_phone):
#                     compare.append("上级手机号码由 %s 变更为 %s" %(old_parent_phone,parent_phone))
#             if bus_parent_phone:
#                 if not old_bus_parent_phone:
#                     compare.append("禄可商务上级的手机号码由 %s 变更为 %s" % ("-", old_bus_parent_phone))
#                 elif int(bus_parent_phone) != int(old_bus_parent_phone):
#                     compare.append("禄可商务上级的手机号码由 %s 变更为 %s" %(old_bus_parent_phone,bus_parent_phone))
#
#             if compare:
#                 compare.insert(0,"该用户的unionid为:%s" %unionid)
#                 all_compare.append(compare)
#
#
#         logger.info(all_compare)
#         if all_compare:
#             last_compare = []
#             for c in all_compare:
#                 last_compare.append("<br>".join(c))
#             insert_sql = '''insert into sys_log (user_id,log_url,log_req,log_action,remark) values (%s,%s,%s,%s,%s)'''
#             params = []
#             params.append(user_id)
#             params.append("/user/relate/update/user/ascription")
#             params.append(json.dumps(request.json))
#             params.append("修改用户数据")
#             params.append("<br>".join(last_compare))
#             logger.info(params)
#             cursor.execute(insert_sql, params)
#
#             conn.commit()
#
#         return {"code": "0000", "msg": "更新成功", "status": "success"}
#     except Exception as e:
#         conn.rollback()
#         logger.exception(traceback.format_exc())
#         # 参数名错误
#         return {"code": "10000", "status": "failed", "msg": message["10000"]}
#     finally:
#         conn.close()
#         conn_crm.close()


@userrelatebp.route("update/user/ascription",methods=["POST"])
def update_user_ascriptions():
    try:
        conn = direct_get_conn(analyze_mysql_conf)
        cursor = conn.cursor()

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

        #上级修改传手机号码
        parent_phone = request.json.get("parent_phone")
        if not parent_phone:
            return {"code":"11033","msg":message["11033"],"status":"failed"}

        unionid_lists = request.json.get("unionid_lists")

        # 如果要改上级的话 需要看有没有递归 判断要修改的手机号码是不是在下级里面

        all_below_unionid = []
        user_data = []
        parent_unionid = ""
        bus_data = ""

        if parent_phone:
            user_sql = '''select * from crm_user where phone = %s''' % (parent_phone)
            user_data = pd.read_sql(user_sql, conn)
            if user_data.shape[0] == 0:
                return {"code": "11029", "msg": message["11029"], "status": "failed"}
            user_data = user_data.to_dict("records")[0]
            parent_unionid = user_data["unionid"]

            # 被改的上级归属运营中心
            sql = '''select * from crm_user where unionid = %s''' % parent_unionid
            logger.info(sql)
            bus_data = pd.read_sql(sql, conn).to_dict("records")[0]

            logger.info(parent_unionid)
            for unionid in unionid_lists:
                logger.info(unionid)
                sql = '''
                    select * from (						
                        select a.*, if (crm =0, Null, b.operatename) operatename, b.id operateid from
                        (WITH RECURSIVE temp as (
                        SELECT t.unionid id,t.parentid pid,t.phone,t.nickname,t.name FROM crm_user t WHERE unionid = %s
                        UNION ALL
                        SELECT t1.unionid id,t1.parentid pid,t1.phone, t1.nickname,t1.name FROM crm_user t1 INNER JOIN temp ON t1.parentid = temp.id
                        )
                        SELECT * FROM temp
                        )a left join operationcenter b
                        on a.id = b.unionid
                        ) t where  phone is not null and phone !=""
                    ''' % (unionid)
                cursor.execute(sql)
                datas = cursor.fetchall()
                # logger.info(datas)
                below_unionid = [str(data[0]) for data in datas]
                below_unionid = list(set(below_unionid))
                if "None" in below_unionid:
                    below_unionid.remove("None")
                logger.info(below_unionid)
                if str(parent_unionid) in below_unionid:
                    return {"code": "11028", "msg": "用户："+str(unionid)+":"+message["11028"], "status": "failed"}
                all_below_unionid.append(below_unionid)
        elif str(parent_phone) == "":
            parent_phone = 0


        all_compare = []
        #原用户数据 用户对比旧数据
        for i,unionid in enumerate(unionid_lists):
            compare = []

            flag = 0
            select_sql = '''select * from crm_user where unionid = %s and del_flag = 0''' % (unionid)
            old_data = pd.read_sql(select_sql, conn)

            old_data = old_data.to_dict("records")[0]
            logger.info(old_data)
            old_parent_phone = old_data["parent_phone"]
            old_operate_id = old_data["operate_id"]


            #判断是否要更新运营中心 如果unionid本身是运营中心的 就改推荐关系 如果本身不是运营中心的本身和下级原本归属运营中心的要批量修改
            bus_sql = '''select * from operationcenter where unionid = %s''' %unionid
            flag = cursor.execute(bus_sql)
            logger.info(flag)

            update_operate_sql = ""
            update_operate = ""

            update_user_daily_order_82_sql = ""
            update_user_daily_order_data_sql = ""
            update_user_daily_order_p8_sql = ""
            update_user_storage_eight_sql = ""
            update_user_storage_eight_hour_sql = ""
            update_user_storage_eight_today_sql = ""
            update_user_storage_value_sql = ""
            update_user_storage_value_hour_sql = ""
            update_user_storage_value_today_sql = ""


            # 如果没有找到 crm_user关系修改
            if parent_phone and not flag:
                # 把被改的unionid列出来
                if old_operate_id:
                    logger.info(all_below_unionid[i])
                    update_unionid_sql = '''select unionid from crm_user where operate_id = %s and unionid in (%s)''' % (old_operate_id, ",".join(all_below_unionid[i]))
                    update_unionid = pd.read_sql(update_unionid_sql, conn)["unionid"].to_list()
                    logger.info(update_unionid)
                    if update_unionid:
                        sql = '''select operatename from crm_user where unionid in (%s,%s)''' % (unionid, parent_unionid)
                        cursor.execute(sql)
                        operatena = cursor.fetchall()
                        old_operate_operatena = operatena[0][0]
                        operate_operatena = operatena[1][0]
                        logger.info(operate_operatena)
                        compare.append("本级和下级部分用户 支持crm运营中心由原先的运营中心： %s 变更为： %s" % (old_operate_operatena, operate_operatena))
                        # compare.append("以下用户:%s 支持crm运营中心由原先的运营中心： %s 变更为： %s" % (str(update_unionid)[1:-1], old_operate_operatena, operate_operatena))
                        update_operate_sql = '''update crm_user set operatename = "%s",bus_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (
                        bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],
                        bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        logger.info(update_operate_sql)
                        # cursor.execute(update_operate_sql)
                else:
                    logger.info("该unionis没有运营中心 元数据")
                    update_unionid_sql = '''select unionid from crm_user where (operate_id is null or operate_id = "") and unionid in (%s)''' % (",".join(all_below_unionid[i]))
                    update_unionid = pd.read_sql(update_unionid_sql, conn)["unionid"].to_list()

                    if update_unionid:
                        logger.info("有要改的")
                        sql = '''select operatename from crm_user where unionid in (%s)''' % (parent_unionid)
                        cursor.execute(sql)
                        operatena = cursor.fetchall()
                        operate_operatena = operatena[0][0]

                        # compare.append("以下用户:%s 支持crm运营中心由原先的运营中心： %s 变更为： %s" % (str(update_unionid)[1:-1], "-", operate_operatena))

                        compare.append("本级和下级部分用户支持crm运营中心由原先的运营中心： %s 变更为： %s" % ( "-", operate_operatena))
                        update_operate_sql = '''update crm_user set operatename = "%s",bus_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (
                        bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],
                        bus_data["leader_unionid"], bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        logger.info(update_operate_sql)

                        # cursor.execute(update_operate_sql)


            #统计表的运营关系修改
            if parent_phone and not flag:
                # 把被改的unionid列出来
                if old_operate_id:
                    logger.info(all_below_unionid[i])
                    update_unionid_sql = '''select unionid from crm_user where operate_id = %s and unionid in (%s)''' % (old_operate_id, ",".join(all_below_unionid[i]))
                    update_unionid = pd.read_sql(update_unionid_sql, conn)["unionid"].to_list()
                    if update_unionid:
                        update_user_daily_order_82_sql = '''update user_daily_order_82 set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_daily_order_data_sql = '''update user_daily_order_data set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_daily_order_p8_sql = '''update user_daily_order_p8 set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_storage_eight_sql = '''update user_storage_eight set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_storage_eight_hour_sql = '''update user_storage_eight_hour set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_storage_eight_today_sql = '''update user_storage_eight_today set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_storage_value_sql = '''update user_storage_value set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_storage_value_hour_sql = '''update user_storage_value_hour set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                        update_user_storage_value_today_sql = '''update user_storage_value_hour set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and operate_id = %s''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]), old_operate_id)
                else:
                    logger.info("该unionis没有运营中心 元数据")
                    update_unionid_sql = '''select unionid from crm_user where (operate_id is null or operate_id = "") and unionid in (%s)''' % ( ",".join(all_below_unionid[i]))
                    update_unionid = pd.read_sql(update_unionid_sql, conn)["unionid"].to_list()
                    if update_unionid:
                        # update_operate_sql = '''update crm_user set operatename = "%s",bus_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"],bus_data["leader_unionid"], bus_data["operate_id"],",".join(all_below_unionid[i]))
                        update_user_daily_order_82_sql = '''update user_daily_order_82 set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_daily_order_data_sql = '''update user_daily_order_data set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_daily_order_p8_sql = '''update user_daily_order_p8 set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_storage_eight_sql = '''update user_storage_eight set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_storage_eight_hour_sql = '''update user_storage_eight_hour set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_storage_eight_today_sql = '''update user_storage_eight_today set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_storage_value_sql = '''update user_storage_value set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_storage_value_hour_sql = '''update user_storage_value_hour set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))
                        update_user_storage_value_today_sql = '''update user_storage_value_hour set operatename = "%s",leader_phone= "%s",leader = "%s",leader_unionid = "%s",operate_id = %s  where unionid in (%s) and (operate_id is null or operate_id = "")''' % (bus_data["operatename"], bus_data["bus_phone"], bus_data["leader"], bus_data["leader_unionid"],bus_data["operate_id"], ",".join(all_below_unionid[i]))

            # 更新crm
            update_crm = '''update crm_user '''
            crm_where = ''' where unionid = %s''' %unionid
            crm_condition = []

            # 更新统计表 7
            update_daily_order_sql = '''update user_daily_order_data '''
            update_store_vas_sql = '''update user_storage_value '''
            update_store_vasto_sql = '''update user_storage_value_today '''
            update_store_vasho_sql = '''update user_storage_value_hour '''

            #8
            update_daily_82_sql = '''update user_daily_order_82 '''
            update_daily_p2_sql = '''update user_daily_order_p8 '''
            update_daily_eight_sql = '''update user_storage_eight '''
            update_daily_eighth_sql = '''update user_storage_eight_hour '''
            update_daily_eightt_sql = '''update user_storage_eight_today '''

            update_where = ''' where unionid = %s''' %unionid

            statistic_condition = []

            if parent_phone:
                crm_condition.append(''' parent_phone="%s",parent_nickname="%s",parent_name="%s",parentid="%s" ''' %(parent_phone,user_data["nickname"],user_data["name"],user_data["unionid"]))
                statistic_condition.append(''' parentid="%s",parent_phone="%s" ''' %(user_data["unionid"],parent_phone))


            crm_sql_condition = ",".join(crm_condition)
            statistic_condition_sql = ",".join(statistic_condition)

            logger.info("crm_sql_condition:%s" %crm_sql_condition)
            logger.info("statistic_condition_sql:%s" %statistic_condition_sql)

            # 批量修改统计表
            if statistic_condition_sql:
                update_daily_order_sql = update_daily_order_sql + " set " + statistic_condition_sql + update_where
                update_store_vas_sql = update_store_vas_sql + " set " + statistic_condition_sql + update_where
                update_store_vasto_sql = update_store_vasto_sql + " set " + statistic_condition_sql + update_where
                update_store_vasho_sql = update_store_vasho_sql + " set " + statistic_condition_sql + update_where

                update_daily_82_sql = update_daily_82_sql + " set " + statistic_condition_sql + update_where
                update_daily_p2_sql = update_daily_p2_sql + " set " + statistic_condition_sql + update_where
                update_daily_eight_sql = update_daily_eight_sql + " set " + statistic_condition_sql + update_where
                update_daily_eighth_sql = update_daily_eighth_sql + " set " + statistic_condition_sql + update_where
                update_daily_eightt_sql = update_daily_eightt_sql + " set " + statistic_condition_sql + update_where

                logger.info(update_store_vas_sql)

                cursor.execute(update_daily_order_sql)
                cursor.execute(update_store_vas_sql)
                cursor.execute(update_store_vasto_sql)
                cursor.execute(update_store_vasho_sql)
                logger.info(update_daily_eighth_sql)
                cursor.execute(update_daily_82_sql)
                cursor.execute(update_daily_p2_sql)
                cursor.execute(update_daily_eight_sql)
                cursor.execute(update_daily_eighth_sql)
                cursor.execute(update_daily_eightt_sql)

            if crm_sql_condition:
                update_crm = update_crm + " set " + crm_sql_condition + crm_where
                cursor.execute(update_crm)
                logger.info("执行成功")


            # 修改运营中心
            if update_operate:
                cursor.execute(update_operate)
            if update_operate_sql:
                cursor.execute(update_operate_sql)
            if update_user_daily_order_82_sql:
                cursor.execute(update_user_daily_order_82_sql)
                logger.info("update_user_daily_order_82_sql执行成功")
            if update_user_daily_order_data_sql:
                cursor.execute(update_user_daily_order_data_sql)
                logger.info("update_user_daily_order_data_sql执行成功")
            if update_user_daily_order_p8_sql:
                cursor.execute(update_user_daily_order_p8_sql)
                logger.info("update_user_daily_order_p8_sql执行成功")
            if update_user_storage_eight_sql:
                cursor.execute(update_user_storage_eight_sql)
                logger.info("update_user_storage_eight_sql执行成功")
            if update_user_storage_eight_hour_sql:
                cursor.execute(update_user_storage_eight_hour_sql)
                logger.info("update_user_storage_eight_hour_sql执行成功")
            if update_user_storage_eight_today_sql:
                cursor.execute(update_user_storage_eight_today_sql)
                logger.info("update_user_storage_eight_today_sql执行成功")
            if update_user_storage_value_sql:
                logger.info(update_user_storage_value_sql)
                cursor.execute(update_user_storage_value_sql)
                logger.info("update_user_storage_value_sql执行成功")
            if update_user_storage_value_hour_sql:
                cursor.execute(update_user_storage_value_hour_sql)
                logger.info("update_user_storage_value_hour_sql执行成功")
            if update_user_storage_value_today_sql:
                cursor.execute(update_user_storage_value_today_sql)



            # 日志接入
            # compare = []
            if parent_phone:
                if not old_parent_phone:
                    compare.append("上级手机号码由 %s 变更为 %s" % ("-", parent_phone))
                elif int(parent_phone) != int(old_parent_phone):
                    compare.append("上级手机号码由 %s 变更为 %s" %(old_parent_phone,parent_phone))


            if compare:
                logger.info(compare)
                compare.insert(0,"该用户的unionid为:%s" %unionid)
                all_compare.append(compare)


        logger.info(all_compare)
        if all_compare:
            last_compare = []
            for c in all_compare:
                last_compare.append("<br>".join(c))
            insert_sql = '''insert into sys_log (user_id,log_url,log_req,log_action,remark) values (%s,%s,%s,%s,%s)'''
            params = []
            params.append(user_id)
            params.append("/user/relate/update/user/ascription")
            params.append(json.dumps(request.json))
            params.append("修改用户数据")
            params.append("<br>".join(last_compare))
            logger.info(params)
            cursor.execute(insert_sql, params)

            conn.commit()

        return {"code": "0000", "msg": "更新成功", "status": "success"}
    except Exception as e:
        logger.info(e)
        if e:
            try:
                error = str(e)
                logger.info(error)
                if error.find("Try increasing @@cte_max_recursion_depth to a larger value"):
                    return {"code": "11035", "status": "failed", "msg":message["11035"]}
            except:
                pass
        conn.rollback()
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn.close()


# 查看用户基础信息详情
@userrelatebp.route("/check/baseinfo",methods=["POST"])
def check_base_info():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 2:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
            unionid = request.json['unionid']
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # 获取用户基础信息
        user_base_info_sql = '''
            select unionid, nickname, phone, date_format(addtime, "%Y-%m-%d %H:%i:%S") create_time, address, vertify_status, huoti_status, date_format(birth, "%Y-%m-%d") birth, nationality, sex, name, status from lh_analyze.crm_user where unionid={}
        '''.format(unionid)
        # 用户图片信息
        user_img_info_sql = '''
            select unionid, usericon, identify_front, identify_back, face_pic, identity, province_code, city_code, region_code, town_code, address_detail from lh_analyze.crm_user_info where unionid=%s
        ''' % unionid

        user_base_info = pd.read_sql(user_base_info_sql, conn_analyze)
        user_img_info = pd.read_sql(user_img_info_sql, conn_analyze)
        # 判断省市区是否有空，如果有空，所有都置为空
        for index, user_img in user_img_info.iterrows():
            if not user_img['province_code'] or not user_img['city_code'] or not user_img['region_code']:
                user_img_info.loc[index, ['province_code', 'city_code', 'region_code', 'town_code']] = None

        user_info = user_base_info.merge(user_img_info, how='left', on='unionid')
        user_info.fillna('', inplace=True)

        return {"code": "0000", "status": "success", "msg": user_info.to_dict("records")[0]}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass

@userrelatebp.route("/edit/baseinfo",methods=["POST"])
def edit_base_info():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) < 2:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
            unionid = request.json['unionid']
            request_dict = request.json
            del request_dict['unionid']
            del request_dict['user_id']
            del request_dict['create_time']
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        # 旧数据，用于日志
        user_base_info_sql = '''
            select unionid, nickname, phone, date_format(addtime, "%Y-%m-%d %H:%i:%S") create_time, address, vertify_status, huoti_status, date_format(birth, "%Y-%m-%d") birth, nationality, sex, name, status from lh_analyze.crm_user where unionid={}
        '''.format(unionid)
        # 用户图片信息
        user_img_info_sql = '''
            select unionid, usericon, identify_front, identify_back, face_pic, identity, province_code, city_code, region_code, town_code, address_detail from lh_analyze.crm_user_info where unionid=%s
        ''' % unionid

        user_base_info = pd.read_sql(user_base_info_sql, conn_analyze)
        user_img_info = pd.read_sql(user_img_info_sql, conn_analyze)

        user_info = user_base_info.merge(user_img_info, how='left', on='unionid')
        user_info.fillna('', inplace=True)


        # 划分字段
        crm_user_info_table_columns = ['unionid', 'usericon', 'identify_front', 'identify_back', 'face_pic', 'identity', 'province_code',
                                       'city_code', 'region_code', 'town_code', 'address_detail']
        other_table_columns = ['name', 'nickname', 'phone'] # 如果包含这几个字段，则需要修改另外几个表中的数据

        crm_user_info_table_edit = {}
        crm_user_table_edit = {}
        other_table_edit = {}
        not_edit_columns = []
        # 判断字段是否修改，有修改字段按对应表分类，未修改字段添加到not_edit_columns，后续进行剔除
        for k,v in request_dict.items():
            if user_info[k].values[0] != v:
                # 修改crm_user_info_表
                if k in crm_user_info_table_columns:
                    crm_user_info_table_edit[k] = v
                # 修改crm_user表
                else:
                    crm_user_table_edit[k] = v
                    # 判断是否需要修改其它表格
                    if k in other_table_columns:
                        other_table_edit[k] = v
            else:
                not_edit_columns.append(k)
                continue
        logger.info(crm_user_info_table_edit)
        logger.info(crm_user_table_edit)
        logger.info(other_table_edit)
        execute_list = []
        if len(crm_user_info_table_edit) != 0:
            update_crm_user_info_base_sql = '''update lh_analyze.crm_user_info set'''
            num = 1
            for k,v in crm_user_info_table_edit.items():
                if num == 1:
                    if v == '':
                        update_crm_user_info_base_sql += ''' %s=NULL''' % k
                    else:
                        update_crm_user_info_base_sql += ''' %s="%s"''' % (k, v)
                else:
                    if v == '':
                        update_crm_user_info_base_sql = update_crm_user_info_base_sql + ',' + ''' %s=NULL''' % k
                    else:
                        update_crm_user_info_base_sql = update_crm_user_info_base_sql + ',' + ''' %s="%s"''' % (k, v)
                num += 1
            update_crm_user_info_base_sql += ''' where unionid=%s''' % unionid
            execute_list.append(update_crm_user_info_base_sql)

        if len(crm_user_table_edit) != 0:
            update_crm_user_base_sql = '''update lh_analyze.crm_user set'''
            num = 1
            for k, v in crm_user_table_edit.items():
                if num == 1:
                    if v == '':
                        update_crm_user_base_sql += ''' %s=NULL''' % k
                    else:
                        update_crm_user_base_sql += ''' %s="%s"''' % (k, v)
                else:
                    if v == '':
                        update_crm_user_base_sql = update_crm_user_base_sql + ',' + ''' %s=NULL''' % k
                    else:
                        update_crm_user_base_sql = update_crm_user_base_sql + ',' + ''' %s="%s"''' % (k, v)
                num += 1
            update_crm_user_base_sql += ''' where unionid=%s''' % unionid
            execute_list.append(update_crm_user_base_sql)

        if len(other_table_edit) != 0:
            # crm_user的sql
            update_crm_parent_sql = '''update lh_analyze.crm_user set''' # crm上级
            update_bus_parent_sql = '''update lh_analyze.crm_user set''' # 禄可商务上级
            update_crm_operate_sql = '''update lh_analyze.crm_user set''' # crm运营中心
            update_bus_operate_sql = '''update lh_analyze.crm_user set''' # 禄可商务运营中心
            # operationcenter的sql
            update_operate_sql = '''update lh_analyze.operationcenter set''' # 运营中心
            # # user_daily_order_data的sql
            # update_daily_user_sql = '''update lh_analyze.user_daily_order_data set''' # 用户
            # update_daily_parent_sql = '''update lh_analyze.user_daily_order_data set''' # 上级
            # update_daily_bus_sql = '''update lh_analyze.user_daily_order_data set''' # 运营中心
            # # user_storage_value的sql
            # update_storage_user_sql = '''update lh_analyze.user_storage_value set''' # 用户
            # update_storage_parent_sql = '''update lh_analyze.user_storage_value set'''  # 上级
            # update_storage_bus_sql = '''update lh_analyze.user_storage_value set'''  # 运营中心
            # # user_storage_value_today的sql
            # update_today_user_sql = '''update lh_analyze.user_storage_value_today set'''  # 用户
            # update_today_parent_sql = '''update lh_analyze.user_storage_value_today set'''  # 上级
            # update_today_bus_sql = '''update lh_analyze.user_storage_value_today set'''  # 运营中心
            # for k, v in other_table_edit.items():
            #     if num == 1:
            #         if k == 'name':
            #             update_crm_parent_sql += ''' parent_name="%s"''' % v
            #             update_bus_parent_sql += ''' bus_parent_name="%s"''' % v
            #             update_crm_operate_sql += ''' leader="%s"''' % v
            #             update_bus_operate_sql += ''' direct_leader="%s"''' % v
            #             update_operate_sql += ''' name="%s"''' % v
            #             update_daily_user_sql += ''' name="%s"''' % v
            #             # update_daily_parent_sql += ''' name="%s"''' % v
            #             update_daily_bus_sql += ''' leader="%s"''' % v
            #             update_storage_user_sql += ''' name="%s"''' % v
            #             # update_storage_parent_sql += ''' name="%s"''' % v
            #             update_storage_bus_sql += ''' leader="%s"''' % v
            #             update_today_user_sql += ''' name="%s"''' % v
            #             # update_today_parent_sql += ''' name="%s"''' % v
            #             update_today_bus_sql += ''' leader="%s"''' % v
            #         if k == 'nickname':
            #             update_crm_parent_sql += ''' parent_nickname="%s"''' % v
            #             update_bus_parent_sql += ''' bus_parent_nickname="%s"''' % v
            #             update_operate_sql += ''' nickname="%s"''' % v
            #             update_daily_user_sql += ''' nickname="%s"''' % v
            #             update_storage_user_sql += ''' nickname="%s"''' % v
            #             update_today_user_sql += ''' nickname="%s"''' % v
            #         if k == 'phone':
            #             update_crm_parent_sql += ''' parent_phone="%s"''' % v
            #             update_bus_parent_sql += ''' bus_parent_phone="%s"''' % v
            #             update_crm_operate_sql += ''' bus_phone="%s"''' % v
            #             update_bus_operate_sql += ''' direct_bus_phone="%s"''' % v
            #             update_operate_sql += ''' telephone="%s"''' % v
            #             update_daily_user_sql += ''' phone="%s"''' % v
            #             update_daily_parent_sql += ''' parent_phone="%s"''' % v
            #             update_daily_bus_sql += ''' leader_phone="%s"''' % v
            #             update_storage_user_sql += ''' hold_phone="%s"''' % v
            #             update_storage_parent_sql += ''' parent_phone="%s"''' % v
            #             update_storage_bus_sql += ''' leader_phone="%s"''' % v
            #             update_today_user_sql += ''' hold_phone="%s"''' % v
            #             update_today_parent_sql += ''' parent_phone="%s"''' % v
            #             update_today_bus_sql += ''' leader_phone="%s"''' % v
            #
            #             update_daily_parent_sql += ''' where parentid=%s''' % unionid  # 上级
            #             execute_list.append(update_daily_parent_sql)
            #             update_storage_parent_sql += ''' where parentid=%s''' % unionid  # 上级
            #             execute_list.append(update_storage_parent_sql)
            #             update_today_parent_sql += ''' where parentid=%s''' % unionid  # 上级
            #             execute_list.append(update_today_parent_sql)
            #     else:
            #         if k == 'name':
            #             update_crm_parent_sql = update_crm_parent_sql + ',' + '''' parent_name="%s"''' % v
            #             update_bus_parent_sql = update_bus_parent_sql + ',' + '''' bus_parent_name="%s"''' % v
            #             update_crm_operate_sql = update_crm_operate_sql + ',' + '''' leader="%s"''' % v
            #             update_bus_operate_sql = update_bus_operate_sql + ',' + '''' direct_leader="%s"''' % v
            #             update_operate_sql = update_operate_sql + ',' + '''' name="%s"''' % v
            #             update_daily_user_sql = update_daily_user_sql + ',' + '''' name="%s"''' % v
            #             update_daily_bus_sql = update_daily_bus_sql + ',' + '''' leader="%s"''' % v
            #             update_storage_user_sql = update_storage_user_sql + ',' + '''' name="%s"''' % v
            #             update_storage_bus_sql = update_storage_bus_sql + ',' + '''' leader="%s"''' % v
            #             update_today_user_sql = update_today_user_sql + ',' + '''' name="%s"''' % v
            #             update_today_bus_sql = update_today_bus_sql + ',' + '''' leader="%s"''' % v
            #         if k == 'nickname':
            #             update_crm_parent_sql = update_crm_parent_sql + ',' + '''' parent_nickname="%s"''' % v
            #             update_bus_parent_sql = update_bus_parent_sql + ',' + '''' bus_parent_nickname="%s"''' % v
            #             update_operate_sql = update_operate_sql + ',' + '''' nickname="%s"''' % v
            #             update_daily_user_sql = update_daily_user_sql + ',' + '''' nickname="%s"''' % v
            #             update_storage_user_sql = update_storage_user_sql + ',' + '''' nickname="%s"''' % v
            #             update_today_user_sql = update_today_user_sql + ',' + '''' nickname="%s"''' % v
            #         if k == 'phone':
            #             update_crm_parent_sql = update_crm_parent_sql + ',' + '''' parent_phone="%s"''' % v
            #             update_bus_parent_sql = update_bus_parent_sql + ',' + '''' bus_parent_phone="%s"''' % v
            #             update_crm_operate_sql = update_crm_operate_sql + ',' + '''' bus_phone="%s"''' % v
            #             update_bus_operate_sql = update_bus_operate_sql + ',' + '''' direct_bus_phone="%s"''' % v
            #             update_operate_sql = update_operate_sql + ',' + '''' telephone="%s"''' % v
            #             update_daily_user_sql = update_daily_user_sql + ',' + '''' phone="%s"''' % v
            #             update_daily_parent_sql = update_daily_parent_sql + ',' + '''' parent_phone="%s"''' % v
            #             update_daily_bus_sql = update_daily_bus_sql + ',' + '''' leader_phone="%s"''' % v
            #             update_storage_user_sql = update_storage_user_sql + ',' + '''' hold_phone="%s"''' % v
            #             update_storage_parent_sql = update_storage_parent_sql + ',' + '''' parent_phone="%s"''' % v
            #             update_storage_bus_sql = update_storage_bus_sql + ',' + '''' leader_phone="%s"''' % v
            #             update_today_user_sql = update_today_user_sql + ',' + '''' hold_phone="%s"''' % v
            #             update_today_parent_sql = update_today_parent_sql + ',' + '''' parent_phone="%s"''' % v
            #             update_today_bus_sql = update_today_bus_sql + ',' + '''' leader_phone="%s"''' % v
            #
            #             update_daily_parent_sql += ''' where parentid=%s''' % unionid  # 上级
            #             execute_list.append(update_daily_parent_sql)
            #             update_storage_parent_sql += ''' where parentid=%s''' % unionid  # 上级
            #             execute_list.append(update_storage_parent_sql)
            #             update_today_parent_sql += ''' where parentid=%s''' % unionid  # 上级
            #             execute_list.append(update_today_parent_sql)
            #     num += 1
            num = 1
            for k, v in other_table_edit.items():
                if num == 1:
                    if k == 'name':
                        update_crm_parent_sql += ''' parent_name="%s"''' % v
                        update_bus_parent_sql += ''' bus_parent_name="%s"''' % v
                        update_crm_operate_sql += ''' leader="%s"''' % v
                        update_bus_operate_sql += ''' direct_leader="%s"''' % v
                        update_operate_sql += ''' name="%s"''' % v
                    if k == 'nickname':
                        update_crm_parent_sql += ''' parent_nickname="%s"''' % v
                        update_bus_parent_sql += ''' bus_parent_nickname="%s"''' % v
                        update_operate_sql += ''' nickname="%s"''' % v
                    if k == 'phone':
                        #这里查询一下 如果当前系统存在这个手机号 就不插入
                        check_sql = '''select * from crm_user where phone = %s'''
                        if cursor.execute(check_sql,(v)):
                            return {"code":"11032","msg":message["11032"],"status":"failed"}

                        update_crm_parent_sql += ''' parent_phone="%s"''' % v
                        update_bus_parent_sql += ''' bus_parent_phone="%s"''' % v
                        update_crm_operate_sql += ''' bus_phone="%s"''' % v
                        update_bus_operate_sql += ''' direct_bus_phone="%s"''' % v
                        update_operate_sql += ''' telephone="%s"''' % v
                else:
                    if k == 'name':
                        update_crm_parent_sql = update_crm_parent_sql + ',' + ''' parent_name="%s"''' % v
                        update_bus_parent_sql = update_bus_parent_sql + ',' + ''' bus_parent_name="%s"''' % v
                        update_crm_operate_sql = update_crm_operate_sql + ',' + ''' leader="%s"''' % v
                        update_bus_operate_sql = update_bus_operate_sql + ',' + ''' direct_leader="%s"''' % v
                        update_operate_sql = update_operate_sql + ',' + ''' name="%s"''' % v
                    if k == 'nickname':
                        update_crm_parent_sql = update_crm_parent_sql + ',' + ''' parent_nickname="%s"''' % v
                        update_bus_parent_sql = update_bus_parent_sql + ',' + ''' bus_parent_nickname="%s"''' % v
                        update_operate_sql = update_operate_sql + ',' + ''' nickname="%s"''' % v
                    if k == 'phone':
                        check_sql = '''select * from crm_user where phone = %s'''
                        if cursor.execute(check_sql, (v)):
                            return {"code": "11032", "msg": message["11032"], "status": "failed"}

                        update_crm_parent_sql = update_crm_parent_sql + ',' + ''' parent_phone="%s"''' % v
                        update_bus_parent_sql = update_bus_parent_sql + ',' + ''' bus_parent_phone="%s"''' % v
                        update_crm_operate_sql = update_crm_operate_sql + ',' + ''' bus_phone="%s"''' % v
                        update_bus_operate_sql = update_bus_operate_sql + ',' + ''' direct_bus_phone="%s"''' % v
                        update_operate_sql = update_operate_sql + ',' + ''' telephone="%s"''' % v
                num += 1
            update_crm_parent_sql += ''' where parentid=%s''' % unionid  # crm上级
            execute_list.append(update_crm_parent_sql)
            update_bus_parent_sql += ''' where bus_parentid=%s''' % unionid  # 禄可商务上级
            execute_list.append(update_bus_parent_sql)
            update_crm_operate_sql += ''' where leader_unionid=%s''' % unionid  # crm运营中心
            execute_list.append(update_crm_operate_sql)
            update_bus_operate_sql += ''' where direct_leader_unionid=%s''' % unionid  # 禄可商务运营中心
            execute_list.append(update_bus_operate_sql)
            update_operate_sql += ''' where unionid=%s''' % unionid  # 运营中心
            execute_list.append(update_operate_sql)
        for execute_sql in execute_list:
            logger.info(execute_sql)
            cursor.execute(execute_sql)

        ##################日志##########################
        map_column_dict = {
            "address": "户籍地址",
            "address_detail": "所在地详情",
            "birth": "生日",
            "city_code": "城市",
            "face_pic": "人脸照片",
            "huoti_status": "实人状态",
            "identify_back": "身份证反面",
            "identify_front": "身份证正面",
            "identity": "身份证号",
            "name": "姓名",
            "nationality": "民族",
            "nickname": "昵称",
            "phone": "手机号",
            "province_code": "省",
            "region_code": "区",
            "sex": "性别",
            "status": "账户状态",
            "town_code": "镇",
            "usericon": "用户头像",
            "vertify_status": "实名状态"
        }
        map_vertify_huoti_dict = {
            0: "待认证",
            1: "待审核",
            2: "认证中",
            3: "失败",
            4: "成功"
        }
        map_sex_dict = {
            0: "未知",
            1: "男",
            2: "女"
        }
        map_status_dict = {
            1: "正常",
            2: "禁用"
        }
        # 剔除未修改的字段
        logger.info(not_edit_columns)
        for not_edit in not_edit_columns:
            del request_dict[not_edit]
        logger.info(request_dict)
        compare = []
        for k, v in request_dict.items():
            old_v = user_info[k].values[0]
            if k == 'vertify_status' or k == 'huoti_status':
                old_v = map_vertify_huoti_dict.get(old_v, '')
                v = map_vertify_huoti_dict.get(v, '')
            elif k == 'sex':
                old_v = map_sex_dict.get(old_v, '')
                v = map_sex_dict.get(v, '')
            elif k == 'status':
                old_v = map_status_dict.get(old_v, '')
                v = map_status_dict.get(v, '')
            elif k in ['province_code', 'city_code', 'region_code', 'town_code']:
                area_sql = '''select name from lh_analyze.{area_name} where code=%s'''.format(area_name=k.split('_')[0])
                # 原本存在code
                if len(old_v) != 0:
                    logger.info(area_sql % old_v)
                    old_area_df = pd.read_sql(area_sql % old_v, conn_analyze)
                    old_v = old_area_df['name'].values[0] if old_area_df.shape[0] != 0 else ''
                    logger.info(area_sql % v)
                    new_area_df = pd.read_sql(area_sql % v, conn_analyze)
                    v = new_area_df['name'].values[0] if new_area_df.shape[0] != 0 else ''
                # 原本不存在code
                else:
                    logger.info(area_sql % v)
                    new_area_df = pd.read_sql(area_sql % v, conn_analyze)
                    v = new_area_df['name'].values[0] if new_area_df.shape[0] != 0 else ''
            # 图片
            if k in ['identify_front', 'identify_back', 'face_pic', 'usericon']:
                compare.append("%s修改了" % map_column_dict.get(k))
            else:
                compare.append("%s由 %s 变更为 %s" % (map_column_dict.get(k), old_v, v))

        if compare:
            compare.insert(0, "该用户的unionid为:%s" % unionid)
            insert_sql = '''insert into sys_log (user_id,log_url,log_req,log_action,remark) values (%s,%s,%s,%s,%s)'''
            params = []
            params.append(user_id)
            params.append("/user/relate/edit/baseinfo")
            params.append(json.dumps(request.json))
            params.append("修改用户数据")
            params.append("<br>".join(compare))
            logger.info(params)
            cursor.execute(insert_sql, params)
        conn_analyze.commit()
        return {"code": "0000", "status": "success", "msg": "更新成功"}
    except:
        logger.info(traceback.format_exc())
        conn_analyze.rollback()
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass




# 单用户修改时，查看用户信息
@userrelatebp.route("/edit/detail",methods=["POST"])
def edit_check_base_info():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 2:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
            unionid = request.json['unionid']
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # 获取用户基础信息
        user_base_info_sql = '''
            select operate_direct_id, parent_phone, operate_id, bus_parent_phone from lh_analyze.crm_user where unionid={}
        '''.format(unionid)
        user_info = pd.read_sql(user_base_info_sql, conn_analyze)
        if user_info.shape[0] > 0:
            user_info.fillna('', inplace=True)
            user_info_dict = user_info.to_dict("records")[0]
        else:
            user_info_dict = {
                "bus_phone": "",
                "direct_bus_phone": "",
                "operate_direct_id": "",
                "operate_id": ""
            }
        return {"code": "0000", "status": "success", "msg": user_info_dict}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass

#旧的修改用户信息偏慢 新的在上面 旧的暂时保留
# @userrelatebp.route("mes",methods=["POST"])
# def user_relate_mes():
#     try:
#         conn = direct_get_conn(analyze_mysql_conf)
#
#         logger.info(request.json)
#
#
#         token = request.headers["Token"]
#         user_id = request.json["user_id"]
#
#         if not user_id and not token:
#             return {"code": "10001", "status": "failed", "msg": message["10001"]}
#
#         check_token_result = check_token(token, user_id)
#         if check_token_result["code"] != "0000":
#             return check_token_result
#
#         keyword = request.json["keyword"]
#         bus_id = request.json["bus_id"]
#         parent = request.json["parent"]
#         serpro_grade = request.json["serpro_grade"]
#         capacity = request.json["capacity"]
#         bus_parent = request.json["bus_parent"]
#         page = request.json["page"]
#         size = request.json["size"]
#
#         phone_lists = request.json["phone_lists"]
#         tag_id = request.json.get("tag_id")
#
#         code_page = ""
#         code_size = ""
#         if page and size:
#             code_page = (page - 1) * size
#             code_size = size
#
#
#         #先默认查全部
#         cursor = conn.cursor()
#         # sql = '''select nickname,phone,unionid,parent_phone,operatename,bus_phone,parentid,capacity,bus_parentid,operatenamedirect,direct_bus_phone, vip_grade,vip_starttime,vip_endtime,
#         # serpro_grade,serpro_status
#         # from crm_user where del_flag = 0'''
#         sql = '''
#         select nickname,phone,crm_user.unionid,parent_phone,operatename,bus_phone,parentid,capacity,bus_parentid,operatenamedirect,direct_bus_phone, vip_grade,vip_starttime,vip_endtime,
#         serpro_grade,serpro_status,GROUP_CONCAT(crm_tag.tag_name) tag_name
#         from crm_user
#         left join crm_user_tag on crm_user.unionid = crm_user_tag.unionid
#         left join crm_tag on crm_user_tag.tag_id = crm_tag.id
#         where crm_user.del_flag = 0
#         '''
#
#         group_sql = ''' group by crm_user.unionid '''
#
#         if not tag_id:
#             logger.info("走这个")
#             count_sql = '''
#             select count(*) count
#             from crm_user
#             left join crm_user_tag on crm_user.unionid = crm_user_tag.unionid
#             left join crm_tag on crm_user_tag.tag_id = crm_tag.id
#             where crm_user.del_flag = 0
#             '''
#         else:
#             # 因为分组的关系 所以不适合上面那种
#             count_sql = '''
#             select crm_user.unionid,GROUP_CONCAT(crm_tag.tag_name) tag_name
#             from crm_user
#             left join crm_user_tag on crm_user.unionid = crm_user_tag.unionid
#             left join crm_tag on crm_user_tag.tag_id = crm_tag.id
#             where crm_user.del_flag = 0
#             '''
#
#
#         if phone_lists:
#             phone_lists = ",".join(phone_lists)
#             phone_lists_sql = ''' and phone in (%s)''' %phone_lists
#             sql = sql + phone_lists_sql
#             count_sql = count_sql +phone_lists_sql
#         else:
#             if keyword:
#                 keyword_sql = ''' and (nickname like "%s" or phone like "%s" or crm_user.unionid like "%s")''' %("%"+keyword+"%","%"+keyword+"%","%"+keyword+"%")
#                 sql = sql + keyword_sql
#                 count_sql = count_sql + keyword_sql
#             if bus_id:
#                 bus_sql = ''' and operate_id = %s''' %(bus_id)
#                 sql = sql + bus_sql
#                 count_sql = count_sql + bus_sql
#
#             if parent:
#                 parent_sql = ''' and (parentid = %s or parent_phone=%s)''' %(parent,parent)
#                 sql = sql + parent_sql
#                 count_sql = count_sql + parent_sql
#
#             if serpro_grade:
#                 serpro_grade_sql = ''' and serpro_grade = %s''' %serpro_grade
#                 sql = sql + serpro_grade_sql
#                 count_sql = count_sql + serpro_grade_sql
#
#             if capacity:
#                 capacity_sql = ''' and capacity = %s''' %capacity
#                 sql = sql + capacity_sql
#                 count_sql = count_sql + capacity_sql
#
#             if bus_parent:
#                 bus_parentid_sql = ''' and (bus_parentid = %s or bus_parent_phone = %s)''' %(bus_parent,bus_parent)
#                 sql = sql + bus_parentid_sql
#                 count_sql = count_sql + bus_parentid_sql
#
#         sql = sql + group_sql
#         count_sql = count_sql + group_sql
#
#         if tag_id:
#             logger.info("tag_id:%s" %tag_id)
#             tag_sql = '''select tag_name from crm_tag where id = %s''' %tag_id
#             cursor.execute(tag_sql)
#             tag_name = cursor.fetchone()[0]
#             having_sql = ''' having tag_name like "%s"''' %("%"+tag_name+"%")
#             sql = sql + having_sql
#             count_sql = count_sql + having_sql
#             logger.info(sql)
#             logger.info(count_sql)
#
#
#         logger.info("code_page:%s" % code_page)
#         logger.info("code_size:%s" % code_size)
#         if page and size:
#             limit_sql = ''' limit %s,%s''' %(code_page,code_size)
#             sql = sql + limit_sql
#
#
#
#         logger.info(sql)
#         cursor.execute(sql)
#         datas = cursor.fetchall()
#         logger.info(datas)
#
#         logger.info(count_sql)
#         count = cursor.execute(count_sql)
#
#         last_datas = []
#         for data in datas:
#             data_dict = {}
#             data_dict["nickname"] = data[0]
#             data_dict["phone"] = data[1]
#             data_dict["unionid"] = data[2]
#             data_dict["parent_phone"] = data[3]
#             data_dict["operatename"] = data[4]
#             data_dict["bus_phone"] = data[5]
#             data_dict["parentid"] = data[6]
#             data_dict["capacity"] = data[7]
#             data_dict["bus_parentid"] = data[8]
#             data_dict["operatenamedirect"] = data[9]
#             data_dict["direct_bus_phone"] = data[10]
#             data_dict["vip_grade"] = data[11]
#             data_dict["vip_starttime"] = data[12].strftime("%Y-%m-%d %H:%M:%S") if data[11] else ""
#             data_dict["vip_endtime"] = data[13].strftime("%Y-%m-%d %H:%M:%S") if data[12] else ""
#             data_dict["serpro_grade"] = data[14]
#             data_dict["serpro_status"] = data[15]
#             data_dict["tag_name"] = data[16]
#             last_datas.append(data_dict)
#
#
#         return {"code":"0000","msg":last_datas,"count":count,"status":"success"}
#
#     except Exception as e:
#         logger.error(e)
#         logger.exception(traceback.format_exc())
#         # 参数名错误
#         return {"code": "10000", "status": "failed", "msg": message["10000"]}

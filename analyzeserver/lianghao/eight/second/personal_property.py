# -*- coding: utf-8 -*-
# @Time : 2021/12/13  10:00
# @Author : shihong
# @File : .py
# --------------------------------------
# 个人名片网资产数据分析
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from config import *
from analyzeserver.common import *
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime
from datetime import timedelta
from functools import reduce
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
import numpy as np

secondppbp = Blueprint('property2', __name__, url_prefix='/le/second/property')

# 平台数据总览
@secondppbp.route('/platform/all', methods=['POST'])
def platform_data():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        cursor_analyze = conn_analyze.cursor()
        cursor_lh = conn_lh.cursor()
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

        unioinid_lists = request.json.get("unioinid_lists")
        phone_lists = request.json.get("phone_lists")
        bus_lists = request.json.get("bus_lists")
        tag_id = request.json.get("tag_id")

        args_phone_lists = []
        if phone_lists:
            args_phone_lists = phone_lists.copy()
        elif unioinid_lists:
            try:
                sql = '''select phone from crm_user where find_in_set (unionid,%s)'''
                ags_list = ",".join(unioinid_lists)
                logger.info(ags_list)
                cursor_analyze.execute(sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for p in phone_lists:
                    args_phone_lists.append(p[0])
                # args_phone_lists = ",".join(args_phone_lists)
            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}
        elif bus_lists:
            str_bus_lists = ",".join(bus_lists)
            sql = '''select not_contains from operate_relationship_crm where find_in_set (id,%s) and crm = 1 and del_flag = 0'''
            cursor_analyze.execute(sql, str_bus_lists)
            phone_lists = cursor_analyze.fetchall()
            for p in phone_lists:
                ok_p = json.loads(p[0])
                for op in ok_p:
                    args_phone_lists.append(op)
            # args_phone_lists = ",".join(args_phone_lists)

        tag_phone_list = []
        if tag_id:
            phone_result = find_tag_user_phone(tag_id)
            if phone_result[0]:
                tag_phone_list = phone_result[1]
            else:
                return {"code": phone_result[1], "message": message[phone_result[1]], "status": "failed"}

        # 查tag_phone_list
        select_phone = []
        lh_phone = []
        if tag_phone_list:
            for tp in tag_phone_list:
                if tp in args_phone_lists:
                    continue
                else:
                    select_phone.append(tp)
        else:
            lh_user_sql = '''select distinct(phone) phone from lh_user where del_flag = 0 and phone != "" and phone is not null'''
            lh_user_phone = pd.read_sql(lh_user_sql, conn_lh)
            lh_phone = lh_user_phone["phone"].to_list()

            select_phone = list(set(lh_phone) - set(args_phone_lists))

        flag = 0
        if len(lh_phone) == len(select_phone):
            flag = 1
        else:
            flag = 0

        logger.info(flag)

        select_phone = ",".join(select_phone)

        #算总数 因为不变
        # 算出今日的总持有
        below_sql = '''select sum(hold_count) hold_total_count from user_storage_eight_today '''
        below_group_sql = ''' group by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") order by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") desc limit 1'''
        below_sql = below_sql + below_group_sql
        today_hold_count = pd.read_sql(below_sql, conn_analyze)
        today_hold_count = int(today_hold_count["hold_total_count"].values[0])
        logger.info(today_hold_count)
        all_data = {}
        today_data = {}
        below_sql = ""

        condition = []
        if select_phone:
            if not flag:
                condition.append(''' hold_phone in (%s)''' %select_phone)

                #发布的单独取
                all_public_sql = '''
                select sum(public_lh_count) from (
select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,sell_phone hold_phone,sum(count) public_lh_count from le_second_hand_sell where del_flag = 0  and `status` != 1 group by statistic_time,hold_phone ) t                        '''


                condition_sql = ""
                for i in range(0,len(condition)):
                     condition_sql = " where " + condition[i] if i == 0 else " and " + condition[i]

                if condition_sql:
                    all_public_sql = all_public_sql + condition_sql
                logger.info(all_public_sql)
                cursor_lh.execute(all_public_sql)
                all_public_count = cursor_lh.fetchone()[0]

                # 按照统计表倒序排序按照时间分组取出最新的

                below_sql = '''select sum(public_count_2) public_total_count,sum(public_price_2) publish_total_price,sum(transferred_count) traned_total_count,sum(transferred_price) traned_total_price,sum(use_count) used_total_count,sum(use_total_price) used_total_price,sum(tran_count) tran_total_count,sum(tran_price) tran_total_price,sum(no_tran_count) no_tran_total_count,sum(no_tran_price) no_tran_total_price,sum(hold_count) hold_total_count,sum(hold_price) hold_total_price from user_storage_eight_today '''
                below_group_sql = ''' group by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") order by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") desc limit 1'''

                below_sql = below_sql + condition_sql + below_group_sql if condition_sql else below_sql+below_group_sql

                below_data = (pd.read_sql(below_sql,conn_analyze)).to_dict("records")[0]
                today_data = {
                    "public_count": below_data["public_total_count"], "public_price": below_data["publish_total_price"], "traned_count": below_data["traned_total_count"], "traned_price": below_data["traned_total_price"],
                    "used_count": below_data["used_total_count"],"used_price": below_data["used_total_price"],
                    "tran_count": below_data["tran_total_count"], "tran_price": below_data["tran_total_price"], "no_tran_count": below_data["no_tran_total_count"],
                    "no_tran_price": below_data["no_tran_total_price"], "hold_count": below_data["hold_total_count"], "hold_price": below_data["hold_total_price"]
                }

                use_public_sql = '''select sum(use_count) use_count,sum(public_count) public_count from (
                            select sum(use_count) use_count,sum(public_count_2) public_count from user_storage_eight ''' + condition_sql +'''union all
                            (select sum(use_count) use_count,sum(public_count_2) public_count from user_storage_eight_today''' + condition_sql + '''group by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") order by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") desc 
                            limit 1)
                            ) user_storage'''

                logger.info(use_public_sql)

                cursor_analyze.execute(use_public_sql)
                use_public = cursor_analyze.fetchone()
                logger.info(use_public)
                try:
                    r = get_redis()
                    plat_lh_total_count_8 = int(r.get("plat_lh_total_count_eight"))
                    logger.info(plat_lh_total_count_8)
                except:
                    plat_lh_total_count_8 = plat_le_total_count_eight
                logger.info(plat_lh_total_count_8)

                all_data["plat_count"] = plat_lh_total_count_8
                all_data["plat_hold_count"] = today_data["hold_count"]
                # all_data["plat_surplus_count"] = all_data["plat_count"] - all_data["plat_hold_count"]
                all_data["plat_surplus_count"] = all_data["plat_count"] - today_hold_count
                all_data["plat_tran_count"] = today_data["tran_count"]
                all_data["plat_no_tran_count"] = today_data["no_tran_count"]
                all_data["plat_used_count"] = use_public[0]
                all_data["plat_public_count"] = all_public_count
                msg_data = {"all_data":all_data,"today_data":today_data}

                return {"code":"0000","status":"success","msg":msg_data}
            else:
                # condition.append(''' hold_phone in (%s)''' % select_phone)

                # 发布的单独取
                all_public_sql = '''
                                select sum(public_lh_count) from (
select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,sell_phone hold_phone,sum(count) public_lh_count from le_second_hand_sell where del_flag = 0  and `status` != 1 group by statistic_time,hold_phone ) t                                        '''

                condition_sql = ""
                for i in range(0, len(condition)):
                    condition_sql = " where " + condition[i] if i == 0 else " and " + condition[i]

                if condition_sql:
                    all_public_sql = all_public_sql + condition_sql

                cursor_lh.execute(all_public_sql)
                all_public_count = cursor_lh.fetchone()[0]

                # 按照统计表倒序排序按照时间分组取出最新的

                below_sql = '''select sum(public_count_2) public_total_count,sum(public_price_2) publish_total_price,sum(transferred_count) traned_total_count,sum(transferred_price) traned_total_price,sum(use_count) used_total_count,sum(use_total_price) used_total_price,sum(tran_count) tran_total_count,sum(tran_price) tran_total_price,sum(no_tran_count) no_tran_total_count,sum(no_tran_price) no_tran_total_price,sum(hold_count) hold_total_count,sum(hold_price) hold_total_price from user_storage_eight_today '''
                below_group_sql = ''' group by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") order by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") desc limit 1'''

                below_sql = below_sql + condition_sql + below_group_sql if condition_sql else below_sql + below_group_sql

                below_data = (pd.read_sql(below_sql, conn_analyze)).to_dict("records")[0]
                today_data = {
                    "public_count": below_data["public_total_count"], "public_price": below_data["publish_total_price"],
                    "traned_count": below_data["traned_total_count"], "traned_price": below_data["traned_total_price"],
                    "used_count": below_data["used_total_count"], "used_price": below_data["used_total_price"],
                    "tran_count": below_data["tran_total_count"], "tran_price": below_data["tran_total_price"],
                    "no_tran_count": below_data["no_tran_total_count"],
                    "no_tran_price": below_data["no_tran_total_price"], "hold_count": below_data["hold_total_count"],
                    "hold_price": below_data["hold_total_price"]
                }

                use_public_sql = '''select sum(use_count) use_count,sum(public_count) public_count from (
                                        select sum(use_count) use_count,sum(public_count_2) public_count from user_storage_eight_today ''' + condition_sql + '''union all
                                        (select sum(use_count) use_count,sum(public_count_2) public_count from user_storage_eight_today ''' + condition_sql + '''group by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") order by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") desc 
                                        limit 1)
                                        ) user_storage'''
                logger.info(use_public_sql)
                cursor_analyze.execute(use_public_sql)
                use_public = cursor_analyze.fetchone()

                try:
                    r = get_redis()
                    plat_lh_total_count_8 = int(r.get("plat_lh_total_count_eight"))
                    logger.info(plat_lh_total_count_8)
                except:
                    plat_lh_total_count_8 = plat_le_total_count_eight
                logger.info(plat_lh_total_count_8)

                all_data["plat_count"] = plat_lh_total_count_8
                all_data["plat_hold_count"] = today_data["hold_count"]
                # all_data["plat_surplus_count"] = all_data["plat_count"] - all_data["plat_hold_count"]
                all_data["plat_surplus_count"] = all_data["plat_count"] - today_hold_count
                all_data["plat_tran_count"] = today_data["tran_count"]
                all_data["plat_no_tran_count"] = today_data["no_tran_count"]
                all_data["plat_used_count"] = use_public[0]
                all_data["plat_public_count"] = all_public_count
                msg_data = {"all_data": all_data, "today_data": today_data}

                return {"code": "0000", "status": "success", "msg": msg_data}
        else:

            today_data = {
                "public_count": 0, "public_price": 0,
                "traned_count": 0, "traned_price": 0,
                "used_count": 0, "used_price": 0,
                "tran_count": 0, "tran_price": 0,
                "no_tran_count": 0,
                "no_tran_price": 0, "hold_count": 0,
                "hold_price": 0
            }

            try:
                r = get_redis()
                plat_lh_total_count_8 = int(r.get("plat_lh_total_count_eight"))
                logger.info(plat_lh_total_count_8)
            except:
                plat_lh_total_count_8 = plat_le_total_count_eight
            logger.info(plat_lh_total_count_8)

            all_data["plat_count"] = plat_lh_total_count_8
            all_data["plat_hold_count"] = 0
            all_data["plat_surplus_count"] = all_data["plat_count"] - today_hold_count
            all_data["plat_tran_count"] = 0
            all_data["plat_no_tran_count"] = 0
            all_data["plat_used_count"] = 0
            all_data["plat_public_count"] = 0
            msg_data = {"all_data": all_data, "today_data": today_data}

            return {"code": "0000", "status": "success", "msg": msg_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
        conn_lh.close()

# 平台名片网数据统计
@secondppbp.route("plat/statis",methods=['POST'])
def plat_statis():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        cursor_analyze = conn_analyze.cursor()
        cursor_lh = conn_lh.cursor()
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

        # 1今天 2 本周 3本月 如果自定义就到年月日
        time_type = request.json.get("time_type")
        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")

        unioinid_lists = request.json.get("unioinid_lists")
        phone_lists = request.json.get("phone_lists")
        bus_lists = request.json.get("bus_lists")
        tag_id = request.json.get("tag_id")

        args_phone_lists = []
        if phone_lists:
            args_phone_lists = phone_lists.copy()
        elif unioinid_lists:
            try:
                sql = '''select phone from crm_user where find_in_set (unionid,%s)'''
                ags_list = ",".join(unioinid_lists)
                logger.info(ags_list)
                cursor_analyze.execute(sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for p in phone_lists:
                    args_phone_lists.append(p[0])
                # args_phone_lists = ",".join(args_phone_lists)
            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}
        elif bus_lists:
            str_bus_lists = ",".join(bus_lists)
            sql = '''select not_contains from operate_relationship_crm where find_in_set (id,%s) and crm = 1 and del_flag = 0'''
            cursor_analyze.execute(sql, str_bus_lists)
            phone_lists = cursor_analyze.fetchall()
            for p in phone_lists:
                ok_p = json.loads(p[0])
                for op in ok_p:
                    args_phone_lists.append(op)
            # args_phone_lists = ",".join(args_phone_lists)

        tag_phone_list = []
        if tag_id:
            phone_result = find_tag_user_phone(tag_id)
            if phone_result[0]:
                tag_phone_list = phone_result[1]
            else:
                return {"code": phone_result[1], message: message[phone_result[1]], "status": "failed"}



        # 查tag_phone_list
        select_phone = []
        lh_phone = []
        if tag_phone_list:
            for tp in tag_phone_list:
                if tp in args_phone_lists:
                    continue
                else:
                    select_phone.append(tp)
        else:
            lh_user_sql = '''select distinct(phone) phone from lh_user where del_flag = 0 and phone != "" and phone is not null'''
            lh_user_phone = pd.read_sql(lh_user_sql, conn_lh)
            lh_phone = lh_user_phone["phone"].to_list()

            select_phone = list(set(lh_phone) - set(args_phone_lists))

        flag = 0
        if len(lh_phone) == len(select_phone):
            flag = 1
        else:
            flag = 0

        logger.info(flag)

        select_phone = ",".join(select_phone)

        if time_type == 4:
            if not start_time or not end_time:
                return {"code": "11009", "status": "failed", "msg": message["11009"]}
            if start_time >= end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d")
            datetime_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d")
            daysss = datetime_end_time - datetime_start_time
            if daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0) > 30:
                return {"code": "11018", "status": "failed", "msg": message["11018"]}

        if time_type == 1:
            if select_phone:
                if not flag:
                    form_sql = '''
                        select day_time, sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price, sum(no_tran_count) no_tran_count,
                        sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price, sum(hold_count) hold_count,sum(hold_price)  hold_price
                        from user_storage_eight_hour where hold_phone in ({}) and date_format(day_time, "%Y-%m-%d") = current_date
                        group by day_time
                        order by day_time asc
                    '''.format(select_phone)
                    circle_sql = '''
                        (select "current" statistic_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                        sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                        sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_eight_hour where hold_phone in (%s) group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc limit 1)
                        union all 
                        (select "yesterday" statistic_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                        sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                        sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_eight_today where hold_phone in (%s) order by day_time desc limit 1)
                    '''%(select_phone,select_phone)


                    form_data = pd.read_sql(form_sql, conn_analyze)
                    form_data['day_time'] = form_data['day_time'].apply(lambda x: x.strftime('%Y-%m-%d %H'))
                    form_data = form_data.to_dict("records")

                    circle_data = pd.read_sql(circle_sql, conn_analyze)
                    circle_data = circle_data.to_dict('records')
                    msg_data = {"form_data": form_data, "circle_data": circle_data}
                    return {"code": "0000", "status": "sucesss", "msg": msg_data}
                else:
                    form_sql = '''
                                        select day_time, sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price, sum(no_tran_count) no_tran_count,
                                        sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price, sum(hold_count) hold_count,sum(hold_price)  hold_price
                                        from user_storage_eight_hour where date_format(day_time, "%Y-%m-%d") = current_date
                                        group by day_time
                                        order by day_time asc
                                    '''
                    circle_sql = '''
                                        (select "current" statistic_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                        sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                        sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_eight_today group by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") order by DATE_FORMAT(addtime,"%Y-%m-%d %H-%i") desc limit 1)
                                        union all 
                                        (select "yesterday" statistic_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                        sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                        sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_eight_today order by day_time desc limit 1)
                                    '''

                    form_data = pd.read_sql(form_sql, conn_analyze)
                    form_data['day_time'] = form_data['day_time'].apply(lambda x: x.strftime('%Y-%m-%d %H'))
                    form_data = form_data.to_dict("records")

                    circle_data = pd.read_sql(circle_sql, conn_analyze)
                    circle_data = circle_data.to_dict('records')
                    msg_data = {"form_data": form_data, "circle_data": circle_data}
                    return {"code": "0000", "status": "sucesss", "msg": msg_data}
            else:
                circle_data = [
                  {
                    "no_tran_count": 0,
                    "public_count": 0,
                    "statistic_time": "current",
                    "tran_count": 0,
                    "transferred_count": 0
                  },
                  {
                    "no_tran_count": 0 ,
                    "public_count": 0,
                    "statistic_time": "yesterday",
                    "tran_count": 0,
                    "transferred_count": 0
                  }
                ]
                msg_data = {"form_data": [], "circle_data": circle_data}
                return {"code": "0000", "status": "sucesss", "msg": msg_data}

        elif time_type == 2 or time_type == 3:
            if select_phone:
                if not flag:
                    limit_list = [0,6,7,7] if time_type == 2 else [0,29,30,30]

                    form_sql = '''
                                    (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                    sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                    sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                    from user_storage_eight where hold_phone in (%s) group by day_time order by day_time desc limit %s)
                                    union all
                                    (
                                    select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                    sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                    sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                     from user_storage_eight_today where hold_phone in (%s) group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                    limit 1)
                                    order by day_time asc
                                    ''' % (select_phone,limit_list[1],select_phone)

                    circle_sql = '''
                                                select "current" statistic_time,
                                    sum(public_count) public_count,sum(tran_count) tran_count,
                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                    (select sum(public_count_2) public_count,sum(tran_count) tran_count,
                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                    from user_storage_eight where hold_phone in (%s) group by day_time order by day_time desc limit %s,%s)
                                    union all
                                    (
                                    select sum(public_count_2) public_count,sum(tran_count) tran_count,
                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                     from user_storage_eight_today group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                    limit 1)
                                    ) t1  
                                    union all 
                                    select "yesterday" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                    (select sum(public_count_2) public_count,sum(tran_count) tran_count,
                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                    from user_storage_eight where hold_phone in (%s) group by day_time order by day_time desc limit %s,%s)
                                    ) t2
                                    ''' % (select_phone,limit_list[0], limit_list[1],select_phone, limit_list[2], limit_list[3])



                    form_data = pd.read_sql(form_sql,conn_analyze)
                    form_data = form_data.to_dict("records")

                    circle_data = pd.read_sql(circle_sql,conn_analyze)
                    circle_data = circle_data.to_dict('records')
                    msg_data = {"form_data":form_data,"circle_data":circle_data}
                    return {"code":"0000","status":"sucesss","msg":msg_data}
                else:
                    limit_list = [0, 6, 7, 7] if time_type == 2 else [0, 29, 30, 30]

                    form_sql = '''
                                                        (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                                        sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                                        sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                                        from user_storage_eight group by day_time order by day_time desc limit %s)
                                                        union all
                                                        (
                                                        select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                                        sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                                        sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                                         from user_storage_eight_today  group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                                        limit 1)
                                                        order by day_time asc
                                                        ''' % (limit_list[1])

                    circle_sql = '''
                                                                    select "current" statistic_time,
                                                        sum(public_count) public_count,sum(tran_count) tran_count,
                                                        sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                                        (select sum(public_count_2) public_count,sum(tran_count) tran_count,
                                                        sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                                        from user_storage_eight group by day_time order by day_time desc limit %s,%s)
                                                        union all
                                                        (
                                                        select sum(public_count_2) public_count,sum(tran_count) tran_count,
                                                        sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                                         from user_storage_eight_today group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                                        limit 1)
                                                        ) t1  
                                                        union all 
                                                        select "yesterday" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                                                        sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                                        (select sum(public_count_2) public_count,sum(tran_count) tran_count,
                                                        sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                                        from user_storage_eight  group by day_time order by day_time desc limit %s,%s)
                                                        ) t2
                                                        ''' % (
                     limit_list[0], limit_list[1], limit_list[2], limit_list[3])

                    form_data = pd.read_sql(form_sql, conn_analyze)
                    form_data = form_data.to_dict("records")

                    circle_data = pd.read_sql(circle_sql, conn_analyze)
                    circle_data = circle_data.to_dict('records')
                    msg_data = {"form_data": form_data, "circle_data": circle_data}
                    return {"code": "0000", "status": "sucesss", "msg": msg_data}
            else:
                circle_data = [
                    {
                        "no_tran_count": 0,
                        "public_count": 0,
                        "statistic_time": "current",
                        "tran_count": 0,
                        "transferred_count": 0
                    },
                    {
                        "no_tran_count": 0,
                        "public_count": 0,
                        "statistic_time": "yesterday",
                        "tran_count": 0,
                        "transferred_count": 0
                    }
                ]
                msg_data = {"form_data": [], "circle_data": circle_data}
                return {"code": "0000", "status": "sucesss", "msg": msg_data}
        elif time_type == 4:
            if select_phone:
                if not flag:
                    sub_day = int(daysss.days + 1)
                    before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")
                    before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")


                    form_sql = '''
                                            select * from (
                                (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                from user_storage_eight where hold_phone not (%s) group by day_time order by day_time desc )
                                union all
                                (
                                select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                from user_storage_eight_today where hold_phone not (%s) group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                limit 1)
                                ) t where day_time >= "%s" and day_time <= "%s"
                                            ''' % (select_phone,select_phone,start_time, end_time)

                    circle_sql = '''(select "curent" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                (select day_time,sum(public_count_2) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                from user_storage_eight where hold_phone in (%s) group by day_time order by day_time desc )
                                union all
                                (
                                select day_time,sum(public_count_2) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                from user_storage_eight_today where hold_phone not in (%s) group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                limit 1)
                                ) t1 where day_time >="%s" and day_time <= "%s")
                                union all 
                                (
                                select "yeaterday" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                (select day_time, sum(public_count_2) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                from user_storage_eight where hold_phone in (%s) group by day_time order by day_time desc )
                                ) t2 where day_time >= "%s" and day_time <="%s")''' % (select_phone,select_phone,
                    start_time, end_time,args_phone_lists, before_start_time, before_end_time)


                    logger.info(form_sql)
                    logger.info(circle_sql)

                    form_data = pd.read_sql(form_sql, conn_analyze)
                    form_data = form_data.to_dict("records")

                    circle_data = pd.read_sql(circle_sql, conn_analyze)
                    circle_data = circle_data.to_dict('records')
                    msg_data = {"form_data": form_data, "circle_data": circle_data}
                    return {"code": "0000", "status": "sucesss", "msg": msg_data}
                else:
                    sub_day = int(daysss.days + 1)
                    before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")
                    before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")

                    form_sql = '''
                                                                select * from (
                                                    (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                                    sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                                    sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                                    from user_storage_eight  group by day_time order by day_time desc )
                                                    union all
                                                    (
                                                    select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count_2) public_count,sum(public_price_2) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                                    sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                                    sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                                    from user_storage_eight_today group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                                    limit 1)
                                                    ) t where day_time >= "%s" and day_time <= "%s"
                                                                ''' % (start_time, end_time)

                    circle_sql = '''(select "curent" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                                    (select day_time,sum(public_count_2) public_count,sum(tran_count) tran_count,
                                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                                    from user_storage_eight where hold_phone in (%s) group by day_time order by day_time desc )
                                                    union all
                                                    (
                                                    select day_time,sum(public_count_2) public_count,sum(tran_count) tran_count,
                                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                                    from user_storage_eight_today group by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") order by DATE_FORMAT(addtime,"%%Y-%%m-%%d %%H-%%i") desc 
                                                    limit 1)
                                                    ) t1 where day_time >="%s" and day_time <= "%s")
                                                    union all 
                                                    (
                                                    select "yeaterday" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                                    (select day_time, sum(public_count_2) public_count,sum(tran_count) tran_count,
                                                    sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                                    from user_storage_eight group by day_time order by day_time desc )
                                                    ) t2 where day_time >= "%s" and day_time <="%s")''' % (
                    start_time, end_time, args_phone_lists, before_start_time, before_end_time)

                    logger.info(form_sql)
                    logger.info(circle_sql)

                    form_data = pd.read_sql(form_sql, conn_analyze)
                    form_data = form_data.to_dict("records")

                    circle_data = pd.read_sql(circle_sql, conn_analyze)
                    circle_data = circle_data.to_dict('records')
                    msg_data = {"form_data": form_data, "circle_data": circle_data}
                    return {"code": "0000", "status": "sucesss", "msg": msg_data}
            else:
                circle_data = [
                    {
                        "no_tran_count": 0,
                        "public_count": 0,
                        "statistic_time": "current",
                        "tran_count": 0,
                        "transferred_count": 0
                    },
                    {
                        "no_tran_count": 0,
                        "public_count": 0,
                        "statistic_time": "yesterday",
                        "tran_count": 0,
                        "transferred_count": 0
                    }
                ]
                msg_data = {"form_data": [], "circle_data": circle_data}
                return {"code": "0000", "status": "sucesss", "msg": msg_data}

        else:
            return {"code":"10014","status":"failed","msg":message["10014"]}
    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
        conn_lh.close()

# 个人持有名片网估值统计详情
@secondppbp.route("person/detail",methods=['POST'])
def person_detail():
    try:
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


        hold_phone = request.json.get("hold_phone")
        if not hold_phone:
            return {"code":"10001","message":message["10001"],"status":"failed"}



        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh or not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        cursor_lh = conn_lh.cursor()

        data = {"user":{},"up":{},"middle":""}

        #根据手机号差个人信息
        user_sql = '''select if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) nickname,phone,unionid,operatename,parentid,parent_phone from crm_user where phone = %s''' %hold_phone
        user_data = pd.read_sql(user_sql,conn_analyze)
        user_data = user_data.to_dict("records")
        data["user"] = user_data

        # 先算投入价值
        sql = '''select sum(total_price) total_price from le_order where type = 4 and del_flag = 0 and `status` = 1 and phone = %s''' %hold_phone
        cursor_lh.execute(sql)
        investment = cursor_lh.fetchone()
        logger.info(investment)
        try:
            investment = round(float(investment[0]),2)
        except:
            investment = 0

        sql = '''select public_count_2 public_count,public_price_2 public_price,transferred_count,transferred_price,use_count,use_total_price,tran_count ,tran_price,no_tran_count,no_tran_price,hold_count,hold_price,transferred_count,transferred_price from user_storage_eight_today where hold_phone = %s
group by addtime order by addtime desc limit 1''' %hold_phone


        sum_data = (pd.read_sql(sql, conn_analyze)).to_dict("records")[0]
        logger.info(sum_data)

        #去user表拿id
        user_sql = '''select id from lh_user where phone = %s'''
        cursor_lh.execute(user_sql,(hold_phone))
        lh_user_id = cursor_lh.fetchone()
        if not lh_user_id:
            return {"code":"11014","status":"failed","msg":message["11014"]}
        lh_user_id = lh_user_id[0]

        # 持有
        hold_sql = '''select pretty_type_id,count(*) hold_count from le_pretty_hold_%s where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and hold_phone = %s group by pretty_type_id''' %(lh_user_id[0],hold_phone)
        hold_data = pd.read_sql(hold_sql,conn_lh)

        # 可转让
        tran_sql = '''select pretty_type_id,count(*) tran_count from le_pretty_hold_%s where del_flag=0 and `status` = 0 and is_open_vip = 0 and is_sell = 1 and pay_type !=0  and thaw_time<=now() and hold_phone = %s group by pretty_type_id''' %(lh_user_id[0],hold_phone)
        tran_data = pd.read_sql(tran_sql,conn_lh)

        # traning_sql = '''
        #     select day_time,hold_phone, pretty_type_name, sum(public_count) public_count,sum(public_price) public_price from (select DATE_FORMAT(create_time,"%Y-%m-%d") day_time,sell_phone hold_phone, pretty_type_name, sum(count) public_count,sum(total_price) public_price from lh_sell where del_flag = 0 and `status` != 1 group by day_time, hold_phone, pretty_type_name
        #     union all
        #     select DATE_FORMAT(lsrd.update_time,"%Y-%m-%d") day_time,lsr.retail_user_phone hold_phone, lsrd.pretty_type_name, count(*) public_count,sum(lsrd.unit_price) public_price from lh_sell_retail lsr left join lh_sell_retail_detail lsrd
        #     on lsr.id = lsrd.retail_id where lsr.del_flag = 0 and lsrd.retail_status != 1
        #     group by day_time,hold_phone, pretty_type_name) t group by day_time,hold_phone, pretty_type_name having hold_phone = {} order by day_time desc
        # '''.format(hold_phone)

        traning_sql = '''
        select pretty_type_name, sum(public_count) public_count,sum(public_price) public_price from (select DATE_FORMAT(create_time,"%Y-%m-%d") day_time,sell_phone hold_phone, pretty_type_name, sum(count) public_count,sum(total_price) public_price from le_second_hand_sell where del_flag = 0 and `status` != 1 and sell_phone = {} group by day_time, hold_phone, pretty_type_name
) t group by pretty_type_name 
        '''.format(hold_phone)

        traning_data = pd.read_sql(traning_sql, conn_lh)

        # 不可转让
        no_tran_sql = '''SELECT pretty_type_id,count(*) no_tran_count FROM le_pretty_hold_%s WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM le_pretty_hold_%s WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=now() AND del_flag=0 AND is_sell=1 AND pay_type !=0) and hold_phone = %s group by pretty_type_id''' %(lh_user_id[0],lh_user_id[0],hold_phone)
        no_tran_data = pd.read_sql(no_tran_sql,conn_lh)

        # 已转让
        # traned_sql = '''select pretty_type_id,count(*) traned_count from lh_pretty_hold_%s where hold_phone = %s and del_flag=0 and `status`= 3 group by pretty_type_id''' %(lh_user_id[0],hold_phone)
        traned_sql = '''
        select pretty_type_name,count(*) traned_count,sum(unit_price) traned_sum_price from (
        select pretty_hold.pretty_type_name,le_order.unit_price from le_pretty_hold_%s pretty_hold
        left join le_order on pretty_hold.order_sn = le_order.order_sn
        where pretty_hold.hold_phone = %s and pretty_hold.del_flag=0 and pretty_hold.`status`= 3 
        ) t
        group by pretty_type_name
        '''%(lh_user_id[0],hold_phone)
        traned_data = pd.read_sql(traned_sql,conn_lh)

        # 价格表
        price_sql = '''select le_config_guide.pretty_type_id,le_pretty_type.name pretty_type_name,max(price) guide_price from le_config_guide
        left join le_pretty_type on le_config_guide.pretty_type_id = le_pretty_type.id where le_config_guide.del_flag = 0  and "%s">=le_config_guide.date group by pretty_type_id ''' % current_time
        price_data = pd.read_sql(price_sql, conn_lh)

        hold_data = hold_data.merge(price_data,on="pretty_type_id")
        tran_data = tran_data.merge(price_data,on="pretty_type_id")
        no_tran_data = no_tran_data.merge(price_data,on="pretty_type_id")
        logger.info(hold_data)

        hold_data["hold_sum_price"] = hold_data["hold_count"]*hold_data["guide_price"]
        tran_data["tran_sum_price"] = tran_data["tran_count"]*tran_data["guide_price"]
        no_tran_data["no_tran_sum_price"] = no_tran_data["no_tran_count"]*no_tran_data["guide_price"]

        # 已使用的现查
        use_sql = '''select count(*) use_count from (
        select pretty_id,count(*) use_count from le_pretty_bind_records where hold_phone = %s and del_flag = 0 group by pretty_id
        ) t''' %hold_phone
        cursor_lh.execute(use_sql)
        use_count = cursor_lh.fetchone()[0]

        # data["up"]["use_count"] = use_count

        # 已转让的数据去现在查询的
        traned_count = int(traned_data["traned_count"].sum())
        traned_price = round(float(traned_data["traned_sum_price"].sum()),2)

        public_count = int(traning_data["public_count"].sum())
        public_price = round(float(traning_data["public_price"].sum()), 2)

        hold_count = int(hold_data["hold_count"].sum())
        hold_price = round(float(hold_data["hold_sum_price"].sum()),2)

        tran_count = int(tran_data["tran_count"].sum())
        tran_price = round(float(tran_data["tran_sum_price"].sum()), 2)

        no_tran_count = int(no_tran_data["no_tran_count"].sum())
        no_tran_price = round(float(no_tran_data["no_tran_sum_price"].sum()), 2)

        # 靓号总数=靓号当前+已转让
        logger.info(sum_data)

        sum_count = hold_count + traned_count
        sum_price = hold_price + traned_price

        data["up"] = {
            "public_count": public_count, "public_price": public_price,
            "traned_count": traned_count, "traned_price": traned_price,
            "tran_count": tran_count, "tran_price": tran_price,
            "no_tran_count": no_tran_count,"no_tran_price": no_tran_price,
            "hold_count": hold_count, "hold_price": hold_price,
            "investment_money": investment, "use_count": use_count,
            "sum_count": sum_count, "sum_price": sum_price
        }
        logger.info(data["up"])


        hold_data = hold_data.to_dict("records")
        tran_data = tran_data.to_dict("records")
        traning_data = traning_data.to_dict("records")
        no_tran_data = no_tran_data.to_dict("records")
        traned_data = traned_data.to_dict("records")

        hold_list = []
        for hl in hold_data:
            hl_dict = {}
            hl_dict["pretty_type_name"] = hl["pretty_type_name"]
            hl_dict["hold_count"] = hl["hold_count"]
            hl_dict["hold_sum_price"] = hl["hold_sum_price"]
            hold_list.append(hl_dict)

        tran_list = []
        for tn in tran_data:
            tn_dict = {}
            tn_dict["pretty_type_name"] = tn["pretty_type_name"]
            tn_dict["tran_count"] = tn["tran_count"]
            tn_dict["tran_sum_price"] = tn["tran_sum_price"]
            tran_list.append(tn_dict)

        traning_list = []
        for tn in traning_data:
            logger.info(tn)
            tn_dict = {}
            tn_dict["pretty_type_name"] = tn["pretty_type_name"]
            tn_dict["traning_count"] = tn["public_count"]
            tn_dict["traning_sum_price"] = tn["public_price"]
            traning_list.append(tn_dict)

        no_tran_list = []
        for ntn in no_tran_data:
            ntn_dict = {}
            ntn_dict["pretty_type_name"] = ntn["pretty_type_name"]
            ntn_dict["no_tran_count"] = ntn["no_tran_count"]
            ntn_dict["no_tran_sum_price"] = ntn["no_tran_sum_price"]
            no_tran_list.append(ntn_dict)

        traned_list = []
        for tn in traned_data:
            tn_dict = {}
            tn_dict["pretty_type_name"] = tn["pretty_type_name"]
            tn_dict["traned_count"] = tn["traned_count"]
            tn_dict["traned_sum_price"] = tn["traned_sum_price"]
            traned_list.append(tn_dict)

        data["middle"] = {
            "hold":hold_list,
            "tran":tran_list,
            "traning":traning_list,
            "no_tran":no_tran_list,
            "traned":traned_list

        }

        return {"code":"0000","status":"success","msg":data}
    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
            conn_lh.close()
        except:
            pass

@secondppbp.route("person/chart",methods=["POST"])
def person_charts():
    try:
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

        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)

        time_type = request.json.get("time_type")
        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")

        hold_phone = request.json.get("hold_phone")
        if not hold_phone:
            return {"code": "10001", "message": message["10001"], "status": "failed"}

        if (time_type != 4 and start_time and end_time) or time_type not in range(1, 5) or (
                time_type == 4 and (not start_time or not end_time)):
            return {"code": "10014", "status": "failed", "msg": message["10014"]}
        if time_type == 4:
            if not start_time or not end_time:
                return {"code": "11009", "status": "failed", "msg": message["11009"]}
            if start_time >= end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            datetime_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            daysss = datetime_end_time - datetime_start_time
            if daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0) > 30:
                return {"code": "11018", "status": "failed", "msg": message["11018"]}

        # 数据分析
        # 价格表
        # if time_type == 1 or (time_type == 4 and daysss and daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0)<1):
        if time_type == 1:
            today_sql = '''
                select day_time, public_count_2 public_count,public_price_2 public_price, tran_count, tran_price, no_tran_count, no_tran_price, transferred_count, transferred_price,
                 hold_count, hold_price
                 from user_storage_eight_hour where hold_phone={} and date_format(day_time, '%Y-%m-%d') = current_date
            '''.format(hold_phone)
            ration_sql = '''
                select tran_price, transferred_price, hold_price
                from user_storage_eight
                where day_time=date_sub(current_date, interval 1 day)
                and hold_phone=%s
            ''' % hold_phone

            today_df = pd.read_sql(today_sql, conn_analyze)
            ration_df = pd.read_sql(ration_sql, conn_analyze)
            today_df.sort_values('day_time', inplace=True)
            today_df['day_time'] = today_df['day_time'].apply(lambda x: x.strftime('%Y-%m-%d %H'))
            ration_data = {
                "hold_price": today_df['hold_price'].values[-1],
                "ration_hold_price": ration_df['hold_price'].sum(),
                "tran_price": today_df['tran_price'].values[-1],
                "ration_tran_price": ration_df['tran_price'].sum(),
                "transferred_price": today_df['transferred_price'].values[-1],
                "ration_transferred_price": ration_df['transferred_price'].sum()
            }
            analyze_data = {
                "day_data": today_df.to_dict("records"),
                "ration_data": ration_data
            }

        else:
            today_new_sql = '''
                        select day_time, a.hold_phone, a.public_count_2 public_count, a.public_price_2 public_price, a.transferred_count, a.transferred_price,
                        a.no_tran_count, a.no_tran_price, a.hold_count, a.hold_price, a.tran_count, a.tran_price from lh_analyze.user_storage_eight_today a
                        join (select hold_phone, max(addtime) time from lh_analyze.user_storage_eight_today group by hold_phone) b
                        on a.hold_phone = b.hold_phone
                        and a.addtime = b.time
                        where a.hold_phone=%s
                    ''' % hold_phone
            today_new_df = pd.read_sql(today_new_sql, conn_analyze)
            if time_type == 2:  # 本周
                week_start = datetime.datetime.now() - timedelta(days=6)
                week_end = (datetime.datetime.now()).strftime('%Y-%m-%d')

                ration_week_start = (week_start - timedelta(weeks=1)).strftime('%Y-%m-%d')
                current_sql = '''
                            select day_time, hold_phone,public_count_2 public_count,public_price_2 public_price, transferred_count, transferred_price,
                            no_tran_count, no_tran_price, hold_count, hold_price, tran_count, tran_price from lh_analyze.user_storage_eight
                            where hold_phone=%s
                            and day_time >="%s" and day_time <="%s"
                        ''' % (hold_phone, week_start.strftime('%Y-%m-%d'), week_end)
                ration_sql = '''
                            select sum(transferred_price) transferred_price, sum(hold_price) hold_price, sum(tran_price) tran_price from lh_analyze.user_storage_eight
                            where hold_phone=%s
                            and day_time >="%s" and day_time < "%s"
                        ''' % (hold_phone, ration_week_start, week_start.strftime('%Y-%m-%d'))
                current_df = pd.read_sql(current_sql, conn_analyze)
                current_df = pd.concat([today_new_df, current_df], axis=0, ignore_index=True)
            elif time_type == 3:  # 本月
                month_start = datetime.datetime.now() - timedelta(days=29)
                month_end = (datetime.datetime.now()).strftime('%Y-%m-%d')

                ration_month_start = (month_start - timedelta(days=30)).strftime('%Y-%m-%d')
                current_sql = '''
                                select day_time, hold_phone,public_count_2 public_count,public_price_2 public_price, transferred_count, transferred_price,
                                no_tran_count, no_tran_price, hold_count, hold_price, tran_count, tran_price from lh_analyze.user_storage_eight
                                where hold_phone=%s
                                and day_time >="%s" and day_time <="%s"
                            ''' % (hold_phone, month_start.strftime('%Y-%m-%d'), month_end)
                ration_sql = '''
                                select sum(transferred_price) transferred_price, sum(hold_price) hold_price, sum(tran_price) tran_price from lh_analyze.user_storage_eight
                                where hold_phone=%s
                                and day_time >="%s" and day_time < "%s"
                            ''' % (hold_phone, ration_month_start, month_start.strftime('%Y-%m-%d'))
                current_df = pd.read_sql(current_sql, conn_analyze)
                current_df = pd.concat([today_new_df, current_df], axis=0, ignore_index=True)
            else:
                sub_day = int(daysss.days + 1)
                before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")
                before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")
                current_sql = '''
                            select day_time, hold_phone,public_count_2 public_count,public_price_2 public_price, transferred_count, transferred_price,
                            no_tran_count, no_tran_price, hold_count, hold_price, tran_count, tran_price from lh_analyze.user_storage_eight
                            where hold_phone=%s
                            and day_time >="%s" and day_time <="%s"
                        ''' % (hold_phone, start_time, end_time)
                ration_sql = '''
                            select sum(transferred_price) transferred_price, sum(hold_price) hold_price, sum(tran_price) tran_price from lh_analyze.user_storage_eight
                            where hold_phone=%s
                            and day_time >="%s" and day_time <= "%s"
                        ''' % (hold_phone, before_start_time, before_end_time)
                current_df = pd.read_sql(current_sql, conn_analyze)
                if end_time == datetime.datetime.now().strftime("%Y-%m-%d"):
                    current_df = pd.concat([today_new_df, current_df], axis=0, ignore_index=True)
            current_df['day_time'] = current_df['day_time'].apply(lambda x: x.strftime('%Y-%m-%d'))
            current_df.sort_values('day_time', inplace=True)
            ration_df = pd.read_sql(ration_sql, conn_analyze)
            ration_data = {
                "hold_price": round(float(current_df['hold_price'].sum()), 2),
                "ration_hold_price": ration_df['hold_price'].sum(),
                "tran_price": round(float(current_df['tran_price'].sum()), 2),
                "ration_tran_price": ration_df['tran_price'].sum(),
                "transferred_price": round(float(current_df['hold_price'].sum()), 2),
                "ration_transferred_price": ration_df['transferred_price'].sum()
            }
            analyze_data = {
                "day_data": current_df.to_dict("records"),
                "ration_data": ration_data
            }

        return {"code":"0000","msg":analyze_data,"status":"success"}
    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
            conn_lh.close()
        except:
            pass

# 名片网归属数据表
@secondppbp.route('/belong', methods=['POST'])
def bus_card_belong():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 13:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            # 持有人信息
            hold_user_info = request.json['hold_user_info'].strip()
            # 持有时间
            time_type = request.json['time_type']
            hold_start_time = request.json['hold_start_time'].strip()
            hold_end_time = request.json['hold_end_time'].strip()
            # 转让类型
            transfer_type = request.json['transfer_type']
            # 使用状态
            use_type = request.json['use_type']
            # 使用人信息
            use_user_info = request.json['use_user_info'].strip()
            # 购买来源
            source_type = request.json['source_type']
            # 靓号位数
            pretty_length = request.json['pretty_length']
            # 靓号类型
            pretty_type = request.json['pretty_type']

            # 每页显示条数
            size = request.json['size']
            # 页码
            page = request.json['page']

            # if hold_start_time or hold_end_time:
            #     order_time_result = judge_start_and_end_time(hold_start_time, hold_end_time)
            #     if not order_time_result[0]:
            #         return {"code": order_time_result[1], "status": "failed", "msg": message[order_time_result[1]]}
            #     request.json['hold_start_time'] = order_time_result[0]
            #     request.json['hold_end_time'] = order_time_result[1]
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_lh or not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        pretty_hold_sql = '''
            select pretty_id, pretty_type_name, pretty_type_length, hold_phone, order_type, `status`, is_sell, pay_type, is_open_vip, create_time, thaw_time
            from lh_pretty_client.le_pretty_hold_{table_name}
            where del_flag=0
        '''
        # 拼接sql
        # 持有时间 0持有时间，1可转让时间，2使用时间-> 为2的在后面进行筛选
        if time_type == 0:
            hold_time_sql = ''' and create_time >= "%s" and create_time <= "%s"''' % (hold_start_time, hold_end_time)
        elif time_type == 1:
            hold_time_sql = ''' and thaw_time >= "%s" and thaw_time <= "%s"''' % (hold_start_time, hold_end_time)
        else:
            hold_time_sql = ''
        pretty_hold_sql_1 = pretty_hold_sql + hold_time_sql
        # 转让类型 # 0可转让 1不可转让 2转让中 3已转让
        if transfer_type == 0:
            transfer_sql = ''' and thaw_time <= now() and `status`=0 and is_open_vip=0 and is_sell=1'''
        elif transfer_type == 1:
            transfer_sql = ''' and is_open_vip=0 AND STATUS=0 and pretty_id not in (select pretty_id from lh_pretty_hold_{table_name} where `status`=0 and is_open_vip=0 and thaw_time<=now() and del_flag=0 and is_sell=1 and pay_type !=0)'''
        elif transfer_type == 2:
            transfer_sql = ''' and `status`=2'''
        elif transfer_type == 3:
            transfer_sql = ''' and `status`=3'''
        else:
            transfer_sql = ''
        pretty_hold_sql_1 += transfer_sql
        # 使用状态，0已使用，1未使用
        if use_type == 0:
            use_sql = ''' and (`status`=1 or is_open_vip=1)'''
        else:
            use_sql = ''
        pretty_hold_sql_1 += use_sql
        # 购买来源 0官方，1交易市场，2预订单，3零售订单，4二手市场订单
        if source_type != '':
            source_sql = ''' and order_type=%s''' % source_type
        else:
            source_sql = ''
        pretty_hold_sql_1 += source_sql
        # 靓号位数
        if pretty_length:
            pretty_length_sql = ''' and pretty_type_length=%s''' % pretty_length
        else:
            pretty_length_sql = ''
        pretty_hold_sql_1 += pretty_length_sql
        # 靓号类型
        if pretty_type:
            pretty_type_sql = ''' and pretty_type_id=%s''' % pretty_type
        else:
            pretty_type_sql = ''
        pretty_hold_sql_1 += pretty_type_sql
        # 读取用户信息
        user_info_sql = '''select unionid, phone, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) nickname from crm_user where phone is not null and phone != ""'''
        user_info_df = pd.read_sql(user_info_sql, conn_analyze)
        user_info_df['unionid'] = user_info_df['unionid'].astype(str)
        logger.info('读取用户信息表')
        user_id_sql = '''select phone, left(id, 1) id from lh_pretty_client.lh_user where del_flag=0'''
        user_id_df = pd.read_sql(user_id_sql, conn_lh)
        logger.info('读取靓号用户表')
        hold_user_info_df = user_info_df.merge(user_id_df, how='left', on='phone')
        # # 如果存在用户信息查找，则先查找用信息，再根据用户信息查找持有表
        hold_table_type_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        hold_user_phone_list = []
        flag = False
        if hold_user_info:
            flag = True
            hold_user_phone_list = (hold_user_info_df.loc[(hold_user_info_df['unionid'].str.contains(hold_user_info)) |
                                            (hold_user_info_df['phone'].str.contains(hold_user_info)) |
                                            (hold_user_info_df['nickname'].str.contains(hold_user_info)), :]['phone'].unique().tolist())
        if use_user_info:
            flag = True
            bind_log_sql = '''select hold_phone, phone from lh_pretty_client.le_pretty_bind_records'''
            bind_log_df = pd.read_sql(bind_log_sql, conn_lh)
            bind_user_info_df = user_info_df.merge(bind_log_df, how='left', on='phone')
            hold_user_phone_list = set(hold_user_phone_list) & set(bind_user_info_df.loc[(bind_user_info_df['unionid'].str.contains(use_user_info)) |
                                                     (bind_user_info_df['phone'].str.contains(use_user_info)) |
                                                     (bind_user_info_df['nickname'].str.contains(use_user_info)), :]['hold_phone'].unique().tolist())
        if flag and len(hold_user_phone_list) == 0:
            return {"code": "0000", "status": "success", "msg": []}
        if hold_user_phone_list:
            logger.info(hold_user_phone_list)
            hold_user_info_df.dropna(subset=['id'], inplace=True)
            hold_table_type_list = hold_user_info_df.loc[hold_user_info_df['phone'].isin(hold_user_phone_list), :]['id'].unique().tolist()



        hold_df_list = []
        cursor = conn_lh.cursor()
        # 没有任何筛选条件时：sql查询加上limit 10
        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
        else:
            start_index = 0
            end_index = 0
        count = 0
        # if pretty_hold_sql_1 == pretty_hold_sql and not hold_user_info and not use_user_info and not time_type:
        #     if start_index == 0 and end_index ==0:
        #         return {'code': "0000", "status": "success", "msg": "数据量过大,暂不支持导出"}
        #
        #    # 获取统计次数
        #     count_list = []
        #     # sql列表
        #     sql_list = []
        #     for hold_table_type in hold_table_type_list:
        #         sql_list.append(pretty_hold_sql_1.format(table_name=hold_table_type))
        #         count_sql = '''
        #         select count(*) count
        #         from lh_pretty_client.lh_pretty_hold_%s
        #         where del_flag=0
        #         ''' % hold_table_type
        #         cursor.execute(count_sql)
        #         count_list.append(int(cursor.fetchone()[0]))
        #     count = sum(count_list)
        #     logger.info(count)
        #     pretty_hold_sql_1 = ''' union all '''.join(sql_list)
        #     pretty_hold_sql_1 = '''select * from (''' + pretty_hold_sql_1 + ''')t limit %s, %s''' % (start_index, size)
        #     hold_all_df = pd.read_sql(pretty_hold_sql_1, conn_lh)
        # else:
        #     for hold_table_type in hold_table_type_list:
        #         hold_df = pd.read_sql(pretty_hold_sql_1.format(table_name=hold_table_type), conn_lh)
        #         hold_df_list.append(hold_df)
        #     hold_all_df = pd.concat(hold_df_list, axis=0)

        # 转让类型
        # 正常

        if pretty_hold_sql_1 == pretty_hold_sql and not hold_user_info and not use_user_info and not time_type:
            if start_index == 0 and end_index == 0:
                return {'code': "10017", "status": "failed", "msg": "数据量过大,暂不支持导出"}

            # 获取统计次数
            count_list = []
            # sql列表
            sql_list = []
            for hold_table_type in hold_table_type_list:
                sql_list.append(pretty_hold_sql_1.format(table_name=hold_table_type))
                count_sql = '''
                select count(*) count
                from lh_pretty_client.le_pretty_hold_%s
                where del_flag=0
                ''' % hold_table_type
                cursor.execute(count_sql)
                count_list.append(int(cursor.fetchone()[0]))
            count = sum(count_list)
            logger.info(count)
            # 如果 page * size > count，则end_index=count，不然会导致sum(count_list) 小于end_index,无法正确判断应该读取哪张表
            if end_index > count:
                end_index = count
            # 跨表维度
            indexs = 0
            count_sum = 0
            # 是否跨表标志位
            span_table_flag = False
            for index, count_s in enumerate(count_list):
                count_sum += count_s
                if count_sum >= end_index:
                    # 是否跨表判断
                    if index != 0 and start_index <= sum(count_list[:index]):
                        span_table_flag = True
                    indexs = index
                    break
                else:
                    continue
            # 跨表
            if span_table_flag:
                sql_list = sql_list[indexs-1:indexs+1]
                start_index = start_index - sum(count_list[:indexs])
                end_index = end_index - sum(count_list[:indexs])
                pretty_hold_sql_1 = ''' union all '''.join(sql_list)
                pretty_hold_sql_1 = '''select * from (''' + pretty_hold_sql_1 + ''')t limit %s, %s''' % (start_index, size)
            else:
                pretty_hold_sql_1 = sql_list[indexs]
                if indexs !=0:
                    start_index = start_index - sum(count_list[:indexs])
                    end_index = end_index - sum(count_list[:indexs])

                pretty_hold_sql_1 = pretty_hold_sql_1 + '''limit %s, %s''' % (start_index, size)
                logger.info(pretty_hold_sql_1)
            hold_all_df = pd.read_sql(pretty_hold_sql_1, conn_lh)
        else:
            logging.info(hold_table_type_list)
            for hold_table_type in hold_table_type_list:
                if hold_user_phone_list:
                    pretty_hold_sql_1 = pretty_hold_sql_1 + ''' and hold_phone in (%s)''' % ','.join(hold_user_phone_list)
                else:
                    pretty_hold_sql_1 = pretty_hold_sql_1
                logger.info(pretty_hold_sql_1)
                hold_df = pd.read_sql(pretty_hold_sql_1.format(table_name=hold_table_type), conn_lh)
                hold_df_list.append(hold_df)
            hold_all_df = pd.concat(hold_df_list, axis=0)


        normal_df = hold_all_df[(hold_all_df['status'] == 0) & (hold_all_df['is_open_vip'] == 0) & (
                    hold_all_df['thaw_time'] <= datetime.datetime.now()) & (hold_all_df['is_sell'] == 1) & (
                                          hold_all_df['pay_type'] != 0)]
        # 0可转让 1不可转让 2转让中 3已转让
        hold_all_df['transfer_type'] = 0
        # 不可转让
        hold_all_df.loc[
            (~hold_all_df['pretty_id'].isin(normal_df['pretty_id'].tolist())) & (hold_all_df['is_open_vip'] == 0) & (
                        hold_all_df['status'] == 0), 'transfer_type'] = 1
        # 已转让
        hold_all_df.loc[hold_all_df['status'] == 3, 'transfer_type'] = 3
        # 转让中
        hold_all_df.loc[hold_all_df['status'] == 2, 'transfer_type'] = 2
        # 使用状态 0 已使用 1未使用
        hold_all_df['use_type'] = 1
        hold_all_df.loc[(hold_all_df['is_open_vip'] == 1) | (hold_all_df['status'] == 1), 'use_type'] = 0
        # 匹配使用时间
        use_pretty_id_list = list(set(hold_all_df.loc[hold_all_df['use_type'] == 0, 'pretty_id'].tolist()))
        if len(use_pretty_id_list) != 0:
            use_pretty_id_list = list(set(hold_all_df.loc[hold_all_df['use_type'] == 0, 'pretty_id'].tolist()))
            use_time_sql = '''
                select hold_phone, phone use_user_phone, pretty_id, min(bind_time) use_time from lh_pretty_client.le_pretty_bind_records where del_flag=0 and pretty_id in (%s) group by hold_phone, pretty_id
            ''' % ','.join(use_pretty_id_list)
            use_time_df = pd.read_sql(use_time_sql, conn_lh)
            use_time_df['use_time'] = use_time_df['use_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            hold_all_df = hold_all_df.merge(use_time_df, how='left', on=['hold_phone', 'pretty_id'])
        else:
            hold_all_df['use_time'] = ''
            hold_all_df['use_user_phone'] = ''
        # 匹配使用时间条件
        if time_type == 2:
            hold_all_df = hold_all_df[
                (hold_all_df['use_time'] >= hold_start_time) & (hold_all_df['use_time'] <= hold_end_time)]


        # 合并得到持有人信息
        hold_all_df = hold_all_df.merge(user_info_df.rename(
            columns={"unionid": "hold_unionid", "phone": "hold_phone", "nickname": "hold_name"}),
                                            how='left', on='hold_phone')
        # 使用人信息
        hold_all_df = hold_all_df.merge(user_info_df.rename(
            columns={"unionid": "use_user_unionid", "phone": "use_user_phone", "nickname": "use_user_name"}),
                                            how='left', on='use_user_phone')
        # 匹配持有人
        if hold_user_info:
            hold_all_df = hold_all_df.loc[(hold_all_df['hold_name'].str.contains(hold_user_info))
                                          | (hold_all_df['hold_unionid'].str.contains(hold_user_info))
                                          | (hold_all_df['hold_phone'].str.contains(hold_user_info)), :]
        # 匹配使用人
        if use_user_info:
            hold_all_df = hold_all_df.loc[(hold_all_df['use_user_name'].str.contains(hold_user_info))
                                          | (hold_all_df['use_user_unionid'].str.contains(hold_user_info))
                                          | (hold_all_df['use_user_phone'].str.contains(hold_user_info)), :]
        # 删除昵称
        hold_all_df.drop(['hold_name', 'use_user_name', 'is_sell', 'pay_type', 'is_open_vip', 'status'], axis=1, inplace=True)
        if count == 0:
            count = hold_all_df.shape[0]
            if page and size:
                hold_all_df = hold_all_df[start_index:end_index]
        hold_all_df['create_time'] = hold_all_df['create_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        hold_all_df['thaw_time'] = hold_all_df['thaw_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        hold_all_df.fillna('', inplace=True)
        if start_index == 0 and end_index == 0 and count>500000:
            return {'code': "10017", "status": "failed", "msg": "数据量过大,暂不支持导出"}
        return {"code": "0000", "status": "success", "msg": hold_all_df.to_dict("records"), "count": count}

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
    finally:
        try:
            conn_lh.close()
            conn_analyze.close()
        except:
            pass

# 个人持有名片网估值统计
@secondppbp.route('/personal/hold/total', methods=["POST"])
def personal_hold_total():
    try:
        try:
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

            # 持有人信息
            keyword = request.json['keyword'].strip()
            # 归属运营中心
            operateid = request.json['operateid']
            # 归属上级
            parent = request.json['parent']
            # 过滤条件
            # unionid_lists = [int(unionid) for unionid in request.json['unionid_lists']] # unionid
            # phone_lists = request.json['phone_lists'] # 手机号
            # bus_lists = [int(bus_id) for bus_id in request.json['bus_lists']] # 运营中心
            unionid_lists = request.json['unionid_lists'] # unionid
            phone_lists = request.json['phone_lists'] # 手机号
            bus_lists = request.json['bus_lists']
            # 用户标签
            tag_id = request.json['tag_id']

            # 每页显示条数
            size = request.json['size']
            # 页码
            page = request.json['page']
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_lh or not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # 读取user_storage_value
        user_storage_value_sql = '''select hold_phone, sum(transferred_count) transferred_count, sum(transferred_price) transferred_price from user_storage_eight{} group by hold_phone'''

        # 读取user_storage_value_today表(每10分钟一次，更新最近的是)
        today_storage_sql = '''
            select a.hold_phone, a.transferred_count, a.transferred_price, a.no_tran_count, a.no_tran_price, a.hold_count, a.hold_price, a.tran_count, a.tran_price from lh_analyze.user_storage_eight_today a
            join (select hold_phone, max(addtime) time from lh_analyze.user_storage_eight_today group by hold_phone) b
            on a.hold_phone = b.hold_phone
            and a.addtime = b.time
        '''
        # 读取转让中数据
        public_sql = '''
            select hold_phone,sum(public_count) public_count,sum(public_price) public_price from (select sell_phone hold_phone, sum(count) public_count,sum(total_price) public_price from le_second_hand_sell where del_flag = 0 and `status` != 1 group by hold_phone
            ) t group by hold_phone
        '''
        # 用户信息
        user_info_sql = '''
            select if(`name` is not null,`name`,nickname) nickname, phone hold_phone, unionid, operate_id, operatename, parentid, parent_phone from crm_user where del_flag = 0 and phone != "" and phone is not null
        '''

        # 关键词手机号列表
        keyword_phone_list = []
        # 标签手机号列表
        tag_phone_list = []
        # 条件拼接
        user_condition = ''
        # 是否有传条件判断
        condition_count = 0
        if keyword != "":
            condition_count += 1
            user_result = get_phone_by_keyword(keyword)
            if user_result[0] == 1:
                keyword_phone_list = user_result[1]
            else:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        if operateid != "":
            user_condition += ''' and operate_id=%s''' % operateid
        if parent != "":
            user_condition += ''' and (parentid={parent} or parent_phone={parent})'''.format(parent=parent)
        if tag_id != "":
            condition_count += 1
            result = find_tag_user_phone(tag_id)
            logger.info(result[1])
            if not result[0]:
                return {"code": "10000", "status": "failed", "msg": message["10000"]}
            tag_phone_list = result[1]

        if condition_count > 0:  # 有进行关键词或标签筛选
            if condition_count == 2:  # 有进行关键词和标签筛选，取手机号交集
                query_phone_list = list(set(keyword_phone_list).intersection(set(tag_phone_list)))
            else:  # 有进行关键词或标签筛选，取手机号
                tag_phone_list.extend(keyword_phone_list)
                query_phone_list = tag_phone_list
            # 如果query_phone_list为空，直接返回
            if len(query_phone_list) == 0:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
            else:
                user_condition += ''' and phone in (%s)''' % ','.join(query_phone_list)

        if user_condition != '':
            user_info_sql += user_condition
            user_info_df = pd.read_sql(user_info_sql, conn_analyze)
            # 条件筛选的手机号列表
            condition_phone_list = user_info_df['hold_phone'].tolist()

            # 读取user_storage_value数据sql拼接
            user_storage_value_sql = user_storage_value_sql.format(
                ''' where hold_phone in (%s)''' % ','.join(condition_phone_list))
            today_storage_sql += ''' where a.hold_phone in (%s)''' % ','.join(condition_phone_list)
            public_sql += ''' having hold_phone in (%s)''' % ','.join(condition_phone_list)
        else:  # 没有进行关键词或标签筛选
            user_info_sql += user_condition
            user_info_df = pd.read_sql(user_info_sql, conn_analyze)
            user_storage_value_sql = user_storage_value_sql.format('')
        user_storage_df = pd.read_sql(user_storage_value_sql, conn_analyze)
        today_storage_df = pd.read_sql(today_storage_sql, conn_analyze)
        public_df = pd.read_sql(public_sql, conn_lh)

        merge_df = pd.concat([user_storage_df, today_storage_df], axis=0, ignore_index=True)
        merge_df = merge_df.merge(public_df, how='outer', on='hold_phone')
        merge_df.fillna(0, inplace=True)

        group_df = merge_df.groupby('hold_phone').sum().reset_index()
        fina_df = group_df.merge(user_info_df, how='left', on='hold_phone')

        # 过滤条件
        fina_df = fina_df[
            ~((fina_df['unionid'].isin(unionid_lists)) |
              (fina_df['hold_phone'].isin(phone_lists)) |
              (fina_df['operate_id'].isin(bus_lists)))
        ]
        title_data = {
            "hold_count": int(fina_df['hold_count'].sum()), # 靓号总数
            "hold_price": round(float(fina_df['hold_price'].sum()), 2), # 靓号总价值
            "tran_count": int(fina_df['tran_count'].sum()), # 可转让数量
            "tran_price": round(float(fina_df['tran_price'].sum()) ,2), # 可转让价值
            "no_tran_count": int(fina_df['no_tran_count'].sum()), # 不可转让数量
            "no_tran_price": round(float(fina_df['no_tran_price'].sum()) ,2), # 不可转让价值
            "transferred_count": int(fina_df['transferred_count'].sum()), # 已转让数量
            "transferred_price": round(float(fina_df['transferred_price'].sum()) ,2), # 已转让价值
            "public_count": int(fina_df['public_count'].sum()), # 转让中数量
            "public_price": round(float(fina_df['public_price'].sum()) ,2), # 转让中价值
        }
        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
            cut_data = fina_df[start_index: end_index]
        else:
            cut_data = fina_df

        # 查找用户标签
        tag_phone_list  = cut_data['hold_phone'].tolist()
        if tag_phone_list:
            tag_sql = '''
                select t1.unionid, group_concat(t2.tag_name) tag_name from
                (select unionid, tag_id from lh_analyze.crm_user_tag where unionid in (
                    select unionid from lh_analyze.crm_user where phone in (%s)
                )) t1
                left join
                crm_tag t2
                on t1.tag_id = t2.id
                group by t1.unionid
            ''' % ','.join(tag_phone_list)
            tag_df = pd.read_sql(tag_sql, conn_analyze)
            tag_df['unionid'] = tag_df['unionid'].astype(str)
            cut_data['unionid'] = cut_data['unionid'].astype(str)
            cut_data = cut_data.merge(tag_df, how='left', on='unionid')
        else:
            cut_data['tag_name'] = []

        cut_data.drop(['operate_id', 'parent_phone'], axis=1, inplace=True)
        # 填补空值
        cut_data.fillna('', inplace=True)
        # 数值圆整
        for column in [columns for columns in cut_data.columns if 'price' in columns]:
            cut_data[column] = cut_data[column].astype(float)
            cut_data[column] = cut_data[column].apply(lambda x: round(x, 2))
        cut_data['unionid'] = cut_data['unionid'].replace("nan", '')
        cut_data['unionid'] = cut_data['unionid'].apply(lambda x: del_point(x))

        return_data = {
            "title_data": title_data,
            "data": cut_data.to_dict("records")
        }
        return {"code": "0000", "status": "success", "msg": return_data, "count": fina_df.shape[0]}
    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
    finally:
        try:
            conn_lh.close()
            conn_analyze.close()
        except:
            pass
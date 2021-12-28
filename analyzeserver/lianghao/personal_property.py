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

ppbp = Blueprint('property', __name__, url_prefix='/lh/property')

# 平台数据总览
@ppbp.route('/platform/all', methods=['POST'])
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

        args_phone_lists = []
        if phone_lists:
            args_phone_lists = ",".join(phone_lists)
        elif unioinid_lists:
            try:
                sql = '''select phone from crm_user_{} where find_in_set (unionid,%s)'''.format(current_time)
                ags_list = ",".join(unioinid_lists)
                logger.info(ags_list)
                cursor_analyze.execute(sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for p in phone_lists:
                    args_phone_lists.append(p[0])
                args_phone_lists = ",".join(args_phone_lists)
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
            args_phone_lists = ",".join(args_phone_lists)

        logger.info("args_phone_lists:%s" % args_phone_lists)

        all_data = {}
        today_data = {}

        condition = []
        if args_phone_lists:
            condition.append(''' hold_phone not in (%s)''' %args_phone_lists)

        #发布总的单独取
        all_public_sql = '''
        select sum(public_count) from (
        select day_time,hold_phone,sum(public_count) public_count,sum(public_price) public_price from (select DATE_FORMAT(create_time,"%Y-%m-%d") day_time,sell_phone hold_phone, sum(count) public_count,sum(total_price) public_price from lh_sell where del_flag = 0 and `status` = 0 group by day_time, hold_phone
        union all
        select DATE_FORMAT(lsrd.update_time,"%Y-%m-%d") day_time,lsr.retail_user_phone hold_phone,count(*) public_count,sum(lsrd.unit_price) public_price from lh_sell_retail lsr left join lh_sell_retail_detail lsrd
        on lsr.id = lsrd.retail_id where lsr.del_flag = 0 and lsrd.retail_status = 0
        group by day_time,hold_phone ) t group by day_time,hold_phone  order by day_time desc) t
        '''
        condition_sql = ""
        for i in range(0,len(condition)):
             condition_sql = " where " + condition[i] if i == 0 else " and " + condition[i]
        if condition_sql:
            all_public_sql+condition_sql

        cursor_lh.execute(all_public_sql)
        all_public_count = cursor_lh.fetchone()[0]

        # 按照统计表倒序排序按照时间分组取出最新的

        below_sql = '''select sum(public_count) public_total_count,sum(public_price) publish_total_price,sum(transferred_count) traned_total_count,sum(transferred_price) traned_total_price,sum(use_count) used_total_count,sum(use_total_price) used_total_price,sum(tran_count) tran_total_count,sum(tran_price) tran_total_price,sum(no_tran_count) no_tran_total_count,sum(no_tran_price) no_tran_total_price,sum(hold_count) hold_total_count,sum(hold_price) hold_total_price from user_storage_value_today '''
        below_group_sql = ''' group by addtime order by addtime desc limit 1'''

        below_sql = below_sql + condition_sql + below_group_sql if condition_sql else below_sql+below_group_sql

        below_data = (pd.read_sql(below_sql,conn_analyze)).to_dict("records")[0]
        today_data = {
            "public_count": below_data["public_total_count"], "public_price": below_data["publish_total_price"], "traned_count": below_data["traned_total_count"], "traned_price": below_data["traned_total_price"],
            "used_count": below_data["used_total_count"],"used_price": below_data["used_total_price"],
            "tran_count": below_data["tran_total_count"], "tran_price": below_data["tran_total_price"], "no_tran_count": below_data["no_tran_total_count"],
            "no_tran_price": below_data["no_tran_total_price"], "hold_count": below_data["hold_total_count"], "hold_price": below_data["hold_total_price"]
        }

        # 里面包含发布和使用
        if args_phone_lists:
            use_public_sql = '''select sum(use_count) use_count,sum(public_count) public_count from (
                        select sum(use_count) use_count,sum(public_count) public_count from user_storage_value ''' + condition_sql +'''union all
                        (select sum(use_count) use_count,sum(public_count) public_count from user_storage_value_today''' + condition_sql + '''group by addtime order by addtime desc 
                        limit 1)
                        ) user_storage'''
        else:
            use_public_sql = '''select sum(use_count) use_count,sum(public_count) public_count from (
            select sum(use_count) use_count,sum(public_count) public_count from user_storage_value union all
            (select sum(use_count) use_count,sum(public_count) public_count from user_storage_value_today group by addtime order by addtime desc 
            limit 1)
            ) user_storage'''


        cursor_analyze.execute(use_public_sql)
        use_public = cursor_analyze.fetchone()

        all_data["plat_count"] = plat_lh_total_count_seven
        all_data["plat_hold_count"] = today_data["hold_count"]
        all_data["plat_surplus_count"] = all_data["plat_count"] - all_data["plat_hold_count"]
        all_data["plat_tran_count"] = today_data["tran_count"]
        all_data["plat_no_tran_count"] = today_data["no_tran_count"]
        all_data["plat_used_count"] = use_public[0]
        # all_data["plat_public_count"] = use_public[1]
        all_data["plat_public_count"] = all_public_count
        msg_data = {"all_data":all_data,"today_data":today_data}

        return {"code":"0000","status":"success","msg":msg_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
        conn_lh.close()


@ppbp.route("plat/statis",methods=['POST'])
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

        args_phone_lists = []
        if phone_lists:
            args_phone_lists = ",".join(phone_lists)
        elif unioinid_lists:
            try:
                sql = '''select phone from crm_user_{} where find_in_set (unionid,%s)'''.format(current_time)
                ags_list = ",".join(unioinid_lists)
                logger.info(ags_list)
                cursor_analyze.execute(sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for p in phone_lists:
                    args_phone_lists.append(p[0])
                args_phone_lists = ",".join(args_phone_lists)
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
            args_phone_lists = ",".join(args_phone_lists)

        logger.info("args_phone_lists:%s" % args_phone_lists)


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
            if args_phone_lists:
                form_sql = '''
                            select * from (
                            select date_format(addtime,"%%Y-%%m-%%d %%H") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                            sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                            sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_value_today where hold_phone not in (%s) group by date_format(addtime,"%%Y-%%m-%%d %%H:%%i:%%S") 
                            order by date_format(addtime,"%%Y-%%m-%%d %%H:%%i:%%S")  desc
                            ) user_store group by day_time 
                            ''' %(args_phone_lists)
                circle_sql = '''
                            (select "current" statistic_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                            sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                            sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_value_today where hold_phone not in (%s) group by addtime order by addtime desc limit 1)
                            union all 
                            (select "yesterday" statistic_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                            sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                            sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_value_today where hold_phone not in (%s) order by day_time desc limit 1)
                            '''%(args_phone_lists,args_phone_lists)
            else:
                form_sql = '''
                select * from (
                select date_format(addtime,"%Y-%m-%d %H") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_value_today group by date_format(addtime,"%Y-%m-%d %H:%i:%S") 
                order by date_format(addtime,"%Y-%m-%d %H:%i:%S")  desc
                ) user_store group by day_time 
                '''
                circle_sql = '''
                (select "current" statistic_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_value_today group by addtime order by addtime desc limit 1)
                union all 
                (select "yesterday" statistic_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                sum(hold_count) hold_count,sum(hold_price)  hold_price from user_storage_value_today order by day_time desc limit 1)
                '''

            form_data = pd.read_sql(form_sql, conn_analyze)
            form_data = form_data.to_dict("records")

            circle_data = pd.read_sql(circle_sql, conn_analyze)
            circle_data = circle_data.to_dict('records')
            msg_data = {"form_data": form_data, "circle_data": circle_data}
            return {"code": "0000", "status": "sucesss", "msg": msg_data}

        elif time_type == 2 or time_type == 3:
            limit_list = [0,6,7,7] if time_type == 2 else [0,29,30,30]
            if args_phone_lists:
                form_sql = '''
                                (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                from user_storage_value where hold_phone not in (%s) group by day_time order by day_time desc limit %s)
                                union all
                                (
                                select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                                 from user_storage_value_today where hold_phone not in (%s) group by addtime order by addtime desc 
                                limit 1)
                                order by day_time desc
                                ''' % (args_phone_lists,limit_list[1],args_phone_lists)

                circle_sql = '''
                                            select "current" statistic_time,
                                sum(public_count) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                (select sum(public_count) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                from user_storage_value where hold_phone not in (%s) group by day_time order by day_time desc limit %s,%s)
                                union all
                                (
                                select sum(public_count) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                 from user_storage_value_today group by addtime order by addtime desc 
                                limit 1)
                                ) t1  
                                union all 
                                select "yesterday" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                                (select sum(public_count) public_count,sum(tran_count) tran_count,
                                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                                from user_storage_value where hold_phone not in (%s) group by day_time order by day_time desc limit %s,%s)
                                ) t2
                                ''' % (args_phone_lists,limit_list[0], limit_list[1],args_phone_lists, limit_list[2], limit_list[3])

            else:
                form_sql = '''
                (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                from user_storage_value group by day_time order by day_time desc limit %s)
                union all
                (
                select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                 from user_storage_value_today group by addtime order by addtime desc 
                limit 1)
                order by day_time desc
                ''' %(limit_list[1])

                circle_sql = '''
                            select "current" statistic_time,
                sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                (select sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                from user_storage_value group by day_time order by day_time desc limit %s,%s)
                union all
                (
                select sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                 from user_storage_value_today group by addtime order by addtime desc 
                limit 1)
                ) t1  
                union all 
                select "yesterday" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                (select sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                from user_storage_value group by day_time order by day_time desc limit %s,%s)
                ) t2
                ''' %(limit_list[0],limit_list[1],limit_list[2],limit_list[3])

            form_data = pd.read_sql(form_sql,conn_analyze)
            form_data = form_data.to_dict("records")

            circle_data = pd.read_sql(circle_sql,conn_analyze)
            circle_data = circle_data.to_dict('records')
            msg_data = {"form_data":form_data,"circle_data":circle_data}
            return {"code":"0000","status":"sucesss","msg":msg_data}
        elif time_type == 4:
            sub_day = int(daysss.days + 1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d")

            if args_phone_lists:
                form_sql = '''
                                        select * from (
                            (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                            sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                            sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                            from user_storage_value where hold_phone not in (%s) group by day_time order by day_time desc )
                            union all
                            (
                            select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                            sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                            sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                            from user_storage_value_today where hold_phone not in (%s) group by addtime order by addtime desc 
                            limit 1)
                            ) t where day_time >= "%s" and day_time <= "%s"
                                        ''' % (args_phone_lists,args_phone_lists,start_time, end_time)

                circle_sql = '''(select "curent" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                            sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                            (select day_time,sum(public_count) public_count,sum(tran_count) tran_count,
                            sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                            from user_storage_value where hold_phone not in (%s) group by day_time order by day_time desc )
                            union all
                            (
                            select day_time,sum(public_count) public_count,sum(tran_count) tran_count,
                            sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                            from user_storage_value_today where hold_phone not in (%s) group by addtime order by addtime desc 
                            limit 1)
                            ) t1 where day_time >="%s" and day_time <= "%s")
                            union all 
                            (
                            select "yeaterday" statistic_time,sum(public_count) public_count,sum(tran_count) tran_count,
                            sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                            (select day_time, sum(public_count) public_count,sum(tran_count) tran_count,
                            sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                            from user_storage_value where hold_phone not in (%s) group by day_time order by day_time desc )
                            ) t2 where day_time >= "%s" and day_time <="%s")''' % (args_phone_lists,args_phone_lists,
                start_time, end_time,args_phone_lists, before_start_time, before_end_time)
            else:
                form_sql = '''
                            select * from (
                (select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                from user_storage_value group by day_time order by day_time desc )
                union all
                (
                select date_format(day_time,"%%Y-%%m-%%d") day_time,sum(public_count) public_count,sum(public_price) public_price,sum(tran_count) tran_count,sum(tran_price) tran_price,
                sum(no_tran_count) no_tran_count,sum(no_tran_price) no_tran_price,sum(use_count) use_count,sum(use_total_price) use_total_price,
                sum(hold_count) hold_count,sum(hold_price)  hold_price,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price
                from user_storage_value_today group by addtime order by addtime desc 
                limit 1)
                ) t where day_time >= "%s" and day_time <= "%s"
                            ''' % (start_time,end_time)

                circle_sql = '''(select "curent",sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                (select day_time,sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                from user_storage_value group by day_time order by day_time desc )
                union all
                (
                select day_time,sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                from user_storage_value_today  group by addtime order by addtime desc 
                limit 1)
                ) t1 where day_time >="%s" and day_time <= "%s")
                union all 
                (
                select "yeaterday",sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count from (
                (select day_time, sum(public_count) public_count,sum(tran_count) tran_count,
                sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count
                from user_storage_value group by day_time order by day_time desc )
                ) t2 where day_time >= "%s" and day_time <="%s")''' %(start_time,end_time,before_start_time,before_end_time)

            logger.info(form_sql)
            logger.info(circle_sql)

            form_data = pd.read_sql(form_sql, conn_analyze)
            form_data = form_data.to_dict("records")

            circle_data = pd.read_sql(circle_sql, conn_analyze)
            circle_data = circle_data.to_dict('records')
            msg_data = {"form_data": form_data, "circle_data": circle_data}
            return {"code": "0000", "status": "sucesss", "msg": msg_data}

        else:
            return {"code":"10014","status":"failed","msg":message["10014"]}
    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
        conn_lh.close()


@ppbp.route('/belong', methods=['POST'])
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
        conn_an = direct_get_conn(analyze_mysql_conf)
        if not conn_lh or not conn_an:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        pretty_hold_sql = '''
            select pretty_id, pretty_type_name, pretty_type_length, hold_phone, order_type, `status`, is_sell, pay_type, is_open_vip, create_time, thaw_time
            from lh_pretty_client.lh_pretty_hold_{table_name}
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
        user_info_sql = '''select unionid, phone, if(`name` != "",`name`,if(nickname is not null,nickname,"")) nickname from crm_user_%s where phone is not null and phone != ""''' % current_time
        user_info_df = pd.read_sql(user_info_sql, conn_an)
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
            bind_log_sql = '''select hold_phone, phone from lh_pretty_client.lh_bind_pretty_log'''
            bind_log_df = pd.read_sql(bind_log_sql, conn_lh)
            bind_user_info_df = user_info_df.merge(bind_log_df, how='left', on='phone')
            hold_user_phone_list = set(hold_user_phone_list) & set(bind_user_info_df.loc[(bind_user_info_df['unionid'].str.contains(use_user_info)) |
                                                     (bind_user_info_df['phone'].str.contains(use_user_info)) |
                                                     (bind_user_info_df['nickname'].str.contains(use_user_info)), :]['hold_phone'].unique().tolist())
        if flag and len(hold_user_phone_list) == 0:
            return {"code": "0000", "status": "success", "msg": []}
        if hold_user_phone_list:
            logger.info(hold_user_phone_list)
            hold_table_type_list = hold_user_info_df.loc[hold_user_info_df['phone'].isin(hold_user_phone_list), :]['id'].unique().tolist()

        # use_user_table_list = []
        # if use_user_info:
        #     use_user_table_list.extend(user_info_df.loc[(user_info_df['unionid'].str.contains(use_user_info)) |
        #                                     (user_info_df['phone'].str.contains(use_user_info)) |
        #                                     (user_info_df['nickname'].str.contains(hold_user_info)), :])
        # logger.info(user_table_list)
        # 读取表格

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
        if pretty_hold_sql_1 == pretty_hold_sql and not hold_user_info and not use_user_info and not time_type:
            if start_index == 0 and end_index ==0:
                return {'code': "0000", "status": "success", "msg": "数据量过大,暂不支持导出"}

           # 获取统计次数
            count_list = []
            # sql列表
            sql_list = []
            for hold_table_type in hold_table_type_list:
                sql_list.append(pretty_hold_sql_1.format(table_name=hold_table_type))
                count_sql = '''
                select count(*) count
                from lh_pretty_client.lh_pretty_hold_%s
                where del_flag=0
                ''' % hold_table_type
                cursor.execute(count_sql)
                count_list.append(int(cursor.fetchone()[0]))
            count = sum(count_list)
            logger.info(count)
            pretty_hold_sql_1 = ''' union all '''.join(sql_list)
            pretty_hold_sql_1 = '''select * from (''' + pretty_hold_sql_1 + ''')t limit %s, %s''' % (start_index, end_index)
            hold_all_df = pd.read_sql(pretty_hold_sql_1, conn_lh)
        else:
            for hold_table_type in hold_table_type_list:
                hold_df = pd.read_sql(pretty_hold_sql_1.format(table_name=hold_table_type), conn_lh)
                hold_df_list.append(hold_df)
            hold_all_df = pd.concat(hold_df_list, axis=0)

        # 转让类型
        # 正常
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
                select hold_phone, phone use_user_phone, pretty_id, min(use_time) use_time from lh_pretty_client.lh_bind_pretty_log where del_flag=0 and pretty_id in (%s) group by hold_phone, pretty_id
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
            hold_all_df = hold_all_df[start_index:end_index]
        hold_all_df['create_time'] = hold_all_df['create_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        hold_all_df['thaw_time'] = hold_all_df['thaw_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        hold_all_df.fillna('', inplace=True)

        return {"code": "0000", "status": "success", "msg": hold_all_df.to_dict("records"), "count": count}

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
    finally:
        try:
            conn_lh.close()
            conn_an.close()
        except:
            pass
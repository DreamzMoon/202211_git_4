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
            limit_list = [0,7,7,7] if time_type == 2 else [0,29,30,30]
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
            if len(request.json) != 12:
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
            pretty_type = request.json['prettY_type']

            # 每页显示条数
            size = request.json['size']
            # 页码
            page = request.json['page']

            if hold_start_time or hold_end_time:
                order_time_result = judge_start_and_end_time(hold_start_time, hold_end_time)
                if not order_time_result[0]:
                    return {"code": order_time_result[1], "status": "failed", "msg": message[order_time_result[1]]}
                request.json['hold_start_time'] = order_time_result[0]
                request.json['hold_end_time'] = order_time_result[1]
        except:
            # 参数名错误
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        return '11111111111111'
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
# -*- coding: utf-8 -*-
# @Time : 2021/11/1  10:31
# @Author : shihong
# @File : .py
# --------------------------------------
import sys
sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token


tobp = Blueprint('transfer', __name__, url_prefix='/lh/transfer')

@tobp.route("/total",methods=["POST"])
def transfer_all():
    try:

        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        try:
            #连接靓号数据库 同步
            conn_read = direct_get_conn(lianghao_mysql_conf)
            if not conn_read:
                return {"code": "10008", "status": "failed", "msg": message["10008"]}
            logger.info(type(phone_lists))
            with conn_read.cursor() as cursor:
                args_list = []
                # 过滤手机号
                if phone_lists:
                    args_list = ",".join(phone_lists)
                    logger.info(args_list)
                #过滤用户id
                if unioinid_lists:
                    logger.info(unioinid_lists)
                    #先去crm查这些人对应的手机号码
                    try:
                        conn_crm = direct_get_conn(crm_mysql_conf)
                        crm_cursor = conn_crm.cursor()
                        sql = '''select * from luke_sincerechat.user where find_in_set (id,%s)'''
                        ags_list = ",".join(unioinid_lists)
                        logger.info(ags_list)
                        crm_cursor.execute(sql,ags_list)
                        phone_lists = crm_cursor.fetchall()
                        logger.info(phone_lists)
                        for p in phone_lists:
                            args_list.append(p["phone"])
                        args_list = ",".join(args_list)
                    except Exception as e:
                        logger.exception(e)
                        return {"code": "10006", "status": "failed", "msg": message["10006"]}
                    finally:
                        conn_crm.close()
                #过滤运营中心的
                if bus_lists:
                    # phone_lists = []
                    phone_lists_result = get_lukebus_phone(bus_lists)
                    logger.info(phone_lists_result)
                    if phone_lists_result[0] == 1:
                        args_list = phone_lists_result[1]
                        logger.info(args_list)
                    else:
                        return {"code": "10006", "status": "failed", "msg": message["10006"]}

                logger.info("args:%s" %args_list)

                if args_list:
                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE() and !find_in_set (phone,%s)'''
                    cursor.execute(sql,args_list)
                    order_data = cursor.fetchone()

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0  and status != 1
                                    and DATE_FORMAT(up_time, '%%Y%%m%%d') = CURRENT_DATE() and !find_in_set (sell_phone,%s)'''
                    cursor.execute(sql,args_list)
                    sell_data = cursor.fetchone()
                    logger.info(sell_data)

                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and !find_in_set (phone,%s)'''
                    cursor.execute(sql,args_list)
                    all_order_data = cursor.fetchone()

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0  and status != 1 and !find_in_set (sell_phone,%s)'''
                    cursor.execute(sql,args_list)
                    all_sell_data = cursor.fetchone()

                else:
                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                    cursor.execute(sql)
                    order_data = cursor.fetchone()
                    logger.info(order_data)

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 
                    and status != 1
                    and DATE_FORMAT(up_time, '%Y%m%d') = CURRENT_DATE()'''
                    cursor.execute(sql)
                    sell_data = cursor.fetchone()
                    logger.info(sell_data)

                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
                    cursor.execute(sql)
                    all_order_data = cursor.fetchone()

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1'''
                    cursor.execute(sql)
                    all_sell_data = cursor.fetchone()

            today_data={
                "buy_order_count":order_data[0],
                "buy_total_count":order_data[1],"buy_total_price":order_data[2],
                "sell_order_count":order_data[3],"sell_total_count":order_data[4],
                "sell_total_price":order_data[5],"sell_real_price":order_data[6],
                "sell_fee":order_data[7], "fee":order_data[8],
                "publish_total_price":sell_data[0],"publish_total_count":sell_data[1],
                "publish_sell_count":sell_data[2]
            }
            logger.info("today_data:%s" %today_data)

            all_data ={
                "buy_order_count": all_order_data[0],
                "buy_total_count": all_order_data[1], "buy_total_price": all_order_data[2],
                "sell_order_count": all_order_data[3], "sell_total_count": all_order_data[4],
                "sell_total_price": all_order_data[5], "sell_real_price": all_order_data[6],
                "sell_fee": all_order_data[7], "fee": all_order_data[8],
                "publish_total_price": all_sell_data[0], "publish_total_count": all_sell_data[1],
                "publish_sell_count": all_sell_data[2]
            }

            last_data = {
                "today_data":today_data,
                "all_data":all_data
            }

            return {"code":"0000","status":"success","msg":last_data}
        except:
            logger.exception(traceback.format_exc())
            return {"code": "11025", "status": "failed", "msg": message["11025"]}
        finally:
            conn_read.close()


    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}



@tobp.route("buy",methods=["POST"])
def transfer_buy_order():
    try:

        conn_read = direct_get_conn(lianghao_mysql_conf)
        if not conn_read:
            return {"code":"10008","status":"failed","msg":message["10008"]}

        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json["user_id"]

        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]
        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = int(request.json["time_type"])
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        cursor = conn_read.cursor()

        #校验参数
        sub_time = ""
        if time_type == 4:
            if not start_time or not end_time:
                return {"code":"11009","status":"failed","msg":message["11009"]}

            if start_time >= end_time:
                return {"code":"11020","status":"failed","msg":message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")
            datetime_end_time = datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")
            daysss = datetime_end_time-datetime_start_time
            if daysss.days+ daysss.seconds/(24.0*60.0*60.0) > 30:
                return {"code":"11018","status":"failed","msg":message["11018"]}

            #获取两个起始时间相减判断是否一天
            sub_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")

        args_phone_lists = []
        if phone_lists:
            args_phone_lists = ",".join(phone_lists)
        elif unioinid_lists:
            phone_lists_result = get_lukebus_phone(bus_lists)
            logger.info(phone_lists_result)
            if phone_lists_result[0] == 1:
                args_phone_lists = phone_lists_result[1]
            else:
                return {"code": "10006", "status": "failed", "msg": message["10006"]}

        logger.info("args_phone_lists:%s" %args_phone_lists)



        # 如果选择今天的就按照今天的时间返回
        if time_type == 1 or (time_type == 4 and sub_time):
            #今日

            if time_type == 1:
                circle_sql1 = '''select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''
            else:
                query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d")
                yesterday_query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d")
                circle_sql1 = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(query_time)
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(yesterday_query_time)

            # #直接拼接sql 不然会有很多重复的代码 很烦人
            circle_sql = ""
            if args_phone_lists:
                condition_sql1 = " and phone not in (%s)" %args_phone_lists
                condition_sql2 = " and phone not in (%s)" %args_phone_lists
                circle_sql = circle_sql1 +condition_sql1 + circle_conn + circle_sql2 + condition_sql2
            else:
                circle_sql = circle_sql1 + circle_conn + circle_sql2
            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today":circle_data[0][0],"today_buy_total_price":circle_data[0][1],"today_buy_order_count":circle_data[0][2],
                "yesterday":circle_data[1][0],"yes_buy_total_price":circle_data[1][1],"yes_buy_order_count":circle_data[1][2]
            }

            if time_type == 1:
                today_sql = '''select DATE_FORMAT(create_time, '%Y-%m-%d %H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
            else:
                today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d %%H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %query_time
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            if args_phone_lists:
                condition_sql = " and phone not in (%s)" % args_phone_lists
                today_sql = today_sql + condition_sql + group_order_sql
            else:
                today_sql = today_sql + group_order_sql
            logger.info("today_sql:%s" %today_sql)
            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle":circle,"today":today}
            return {"code":"0000","status":"successs","msg":last_data}
        elif time_type == 2 or time_type == 3:
            if time_type == 2:
                query_range = ["-0", "-6", "-7", "-13"]
            elif time_type == 3:
                query_range = ["-0","-29","-30","-59"]
            circle_sql = ""
            if args_phone_lists:
                circle_sql = '''select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone not in (%s) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc) a
                            union all
                            select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone not in (%s) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc ) b''' %(args_phone_lists,query_range[0],query_range[1],args_phone_lists,query_range[2],query_range[3])
            else:
                circle_sql = '''select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc) a
                union all
                select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc) b'''  %(query_range[0],query_range[1],query_range[2],query_range[3])

            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],"today_buy_order_count":circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1],"yes_buy_order_count":circle_data[1][2]
            }

            week_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) ''' %(query_range[0],query_range[1])
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            if args_phone_lists:
                condition_sql = ''' and phone not in (%s)''' %args_phone_lists
                week_sql = week_sql + condition_sql + group_order_sql
            else:
                week_sql = week_sql + group_order_sql

            logger.info(week_sql)
            cursor.execute(week_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle": circle, "today": today}
            return {"code": "0000", "status": "successs", "msg": last_data}
        elif time_type == 4:
            # 自定义
            sub_day = int(daysss.days + 1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")

            if args_phone_lists:
                circle_sql = '''
                                select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time<="%s" and create_time>="%s" and phone not in (%s) group by statistic_time order by statistic_time asc) a
                                union all
                                select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)  and create_time<="%s" and create_time>="%s" and phone not in (%s) group by statistic_time order by statistic_time asc) b 
                                ''' % (end_time, start_time,args_phone_lists, before_end_time, before_start_time,args_phone_lists)
            else:
                circle_sql = '''
                select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) a
                union all
                select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)  and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) b 
                ''' %(end_time,start_time,before_end_time,before_start_time)
            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)

            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],
                "today_buy_order_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1],
                "yes_buy_order_count": circle_data[1][2]
            }

            sql = '''select DATE_FORMAT(create_time,"%%Y-%%m-%%d") statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time >= "%s" and create_time <= "%s"''' %(start_time,end_time)
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''
            if args_phone_lists:
                condition_sql = ''' and phone not in (%s)''' %(args_phone_lists)
                sql = sql + condition_sql + group_order_sql
            else:
                sql = sql + group_order_sql
            logger.info(sql)
            cursor.execute(sql)
            current_datas = cursor.fetchall()
            logger.info(current_datas)
            datas = []
            for td in reversed(current_datas):
                logger.info(td)
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                datas.append(td_dict)
            logger.info(datas)
            last_data = {"circle": circle, "today": datas}
            return {"code": "0000", "status": "successs", "msg": last_data}
        else:
            return {"code": "11009", "status": "failed", "msg": message["11009"]}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()



@tobp.route("sell",methods=["POST"])
def transfer_sell_order():
    try:

        conn_read = direct_get_conn(lianghao_mysql_conf)
        if not conn_read:
            return {"code":"10008","status":"failed","msg":message["10008"]}

        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json["user_id"]
        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]
        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = int(request.json["time_type"])
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        cursor = conn_read.cursor()

        #校验参数
        sub_time = ""
        if time_type == 4:
            if not start_time or not end_time:
                return {"code":"11009","status":"failed","msg":message["11009"]}

            if start_time >= end_time:
                return {"code":"11020","status":"failed","msg":message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")
            datetime_end_time = datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")
            daysss = datetime_end_time-datetime_start_time
            if daysss.days+ daysss.seconds/(24.0*60.0*60.0) > 30:
                return {"code":"11018","status":"failed","msg":message["11018"]}
                # 获取两个起始时间相减判断是否一天
            sub_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(
                start_time, "%Y-%m-%d %H:%M:%S")

        args_phone_lists = []
        if phone_lists:
            args_phone_lists = ",".join(phone_lists)
        elif unioinid_lists:
            phone_lists_result = get_lukebus_phone(bus_lists)
            logger.info(phone_lists_result)
            if phone_lists_result[0] == 1:
                args_phone_lists = phone_lists_result[1]
            else:
                return {"code": "10006", "status": "failed", "msg": message["10006"]}

        logger.info("args_phone_lists:%s" %args_phone_lists)

        # 如果选择今天的就按照今天的时间返回
        if time_type == 1 or (time_type ==4 and sub_time):
            #今日

            if time_type == 1:
                circle_sql1 = '''select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''
            else:
                query_time = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d")
                yesterday_query_time = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d")

                circle_sql1 = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %query_time
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %yesterday_query_time

            # #直接拼接sql 不然会有很多重复的代码 很烦人
            circle_sql = ""
            if args_phone_lists:
                condition_sql1 = " and sell_phone not in (%s)" %args_phone_lists
                condition_sql2 = " and sell_phone not in (%s)" %args_phone_lists
                circle_sql = circle_sql1 +condition_sql1 + circle_conn + circle_sql2 + condition_sql2
            else:
                circle_sql = circle_sql1 + circle_conn + circle_sql2
            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today":circle_data[0][0],"today_sell_total_price":circle_data[0][1],"today_sell_order_count":circle_data[0][2],
                "yesterday":circle_data[1][0],"yes_sell_total_price":circle_data[1][1],"yes_sell_order_count":circle_data[1][2]
            }

            if time_type == 1:
                today_sql = '''select DATE_FORMAT(create_time, '%Y-%m-%d %H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
            else:
                today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d %%H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %query_time

            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            if args_phone_lists:
                condition_sql = " and sell_phone not in (%s)" % args_phone_lists
                today_sql = today_sql + condition_sql + group_order_sql
            else:
                today_sql = today_sql + group_order_sql
            logger.info("today_sql:%s" %today_sql)
            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["sell_order_count"] = int(td[1])
                td_dict["sell_total_count"] = float(td[2])
                td_dict["sell_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle":circle,"today":today}
            return {"code":"0000","status":"successs","msg":last_data}
        elif time_type == 2 or time_type == 3:
            if time_type == 2:
                query_range = ["-0", "-6", "-7", "-13"]
            elif time_type == 3:
                query_range = ["-0","-29","-30","-59"]
            circle_sql = ""
            if args_phone_lists:
                circle_sql = '''select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone not in (%s) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc) a
                            union all
                            select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone not in (%s) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc ) b''' %(args_phone_lists,query_range[0],query_range[1],args_phone_lists,query_range[2],query_range[3])
            else:
                circle_sql = '''select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc) a
                union all
                select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)  group by statistic_time order by statistic_time desc) b'''  %(query_range[0],query_range[1],query_range[2],query_range[3])

            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_sell_total_price": circle_data[0][1],"today_sell_order_count":circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_sell_total_price": circle_data[1][1],"yes_sell_order_count":circle_data[1][2]
            }

            week_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) ''' %(query_range[0],query_range[1])
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            if args_phone_lists:
                condition_sql = ''' and sell_phone not in (%s)''' %args_phone_lists
                week_sql = week_sql + condition_sql + group_order_sql
            else:
                week_sql = week_sql + group_order_sql

            logger.info(week_sql)
            cursor.execute(week_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["sell_order_count"] = int(td[1])
                td_dict["sell_total_count"] = float(td[2])
                td_dict["sell_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle": circle, "today": today}
            return {"code": "0000", "status": "successs", "msg": last_data}
        elif time_type == 4:
            # 自定义
            sub_day = int(daysss.days + 1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")

            if args_phone_lists:
                circle_sql = '''
                                select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time<="%s" and create_time>="%s" and sell_phone not in (%s) group by statistic_time order by statistic_time asc) a
                                union all
                                select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)  and create_time<="%s" and create_time>="%s" and sell_phone not in (%s) group by statistic_time order by statistic_time asc) b 
                                ''' % (end_time, start_time,args_phone_lists,before_end_time, before_start_time, args_phone_lists)
            else:
                circle_sql = '''
                select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) a
                union all
                select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)  and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) b 
                ''' %(end_time,start_time,before_end_time,before_start_time)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)

            circle = {
                "today": circle_data[0][0], "today_sell_total_price": circle_data[0][1],
                "today_sell_order_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_sell_total_price": circle_data[1][1],
                "yes_sell_order_count": circle_data[1][2]
            }

            sql = '''select DATE_FORMAT(create_time,"%%Y-%%m-%%d") statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time >= "%s" and create_time <= "%s"''' %(start_time,end_time)
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''
            if args_phone_lists:
                condition_sql = ''' and sell_phone not in (%s)''' %(args_phone_lists)
                sql = sql + condition_sql + group_order_sql
            else:
                sql = sql + group_order_sql
            logger.info(sql)
            cursor.execute(sql)
            current_datas = cursor.fetchall()
            logger.info(current_datas)
            datas = []
            for td in reversed(current_datas):
                logger.info(td)
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["sell_order_count"] = int(td[1])
                td_dict["sell_total_count"] = float(td[2])
                td_dict["sell_total_price"] = float(td[3])
                datas.append(td_dict)
            logger.info(datas)
            last_data = {"circle": circle, "today": datas}
            return {"code": "0000", "status": "successs", "msg": last_data}
        else:
            return {"code": "11009", "status": "failed", "msg": message["11009"]}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()





@tobp.route("public",methods=["POST"])
def transfer_public_order():
    try:

        conn_read = direct_get_conn(lianghao_mysql_conf)
        if not conn_read:
            return {"code":"10008","status":"failed","msg":message["10008"]}

        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json["user_id"]
        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]
        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = int(request.json["time_type"])
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        cursor = conn_read.cursor()

        #校验参数
        sub_time = ""
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
                # 获取两个起始时间相减判断是否一天
            sub_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

        args_phone_lists = []
        if phone_lists:
            args_phone_lists = ",".join(phone_lists)
        elif unioinid_lists:
            logger.info(unioinid_lists)
            # 先去crm查这些人对应的手机号码
            try:
                conn_crm = direct_get_conn(crm_mysql_conf)
                crm_cursor = conn_crm.cursor()
                sql = '''select * from luke_sincerechat.user where find_in_set (id,%s)'''
                ags_list = ",".join(unioinid_lists)
                logger.info(ags_list)
                crm_cursor.execute(sql, ags_list)
                phone_lists = crm_cursor.fetchall()
                logger.info(phone_lists)
                for p in phone_lists:
                    args_phone_lists.append(p["phone"])
                args_phone_lists = ",".join(args_phone_lists)
            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}
            finally:
                conn_crm.close()
        elif bus_lists:
            phone_lists_result = get_lukebus_phone(bus_lists)
            logger.info(phone_lists_result)
            if phone_lists_result[0] == 1:
                args_phone_lists = phone_lists_result[1]
            else:
                return {"code": "10006", "status": "failed", "msg": message["10006"]}

        # 如果选择今天的就按照今天的时间返回
        if time_type == 1 or (time_type == 4 and sub_time):
            #今日
            if time_type == 1:
                circle_sql1 = '''select DATE_FORMAT(up_time, '%Y-%m-%d') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and DATE_FORMAT(up_time, '%Y%m%d') = CURRENT_DATE()'''
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(up_time, '%Y-%m-%d') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and DATE_FORMAT(up_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''
            else:
                query_time = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d")
                yesterday_query_time = (datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d")

                circle_sql1 = '''select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and DATE_FORMAT(up_time, '%%Y-%%m-%%d') = "%s"''' %query_time
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and DATE_FORMAT(up_time, '%%Y-%%m-%%d') = "%s"''' %(yesterday_query_time)

            # #直接拼接sql 不然会有很多重复的代码 很烦人
            circle_sql = ""
            if args_phone_lists:
                condition_sql1 = " and sell_phone not in (%s)" %args_phone_lists
                condition_sql2 = " and sell_phone not in (%s)" %args_phone_lists
                circle_sql = circle_sql1 +condition_sql1 + circle_conn + circle_sql2 + condition_sql2
            else:
                circle_sql = circle_sql1 + circle_conn + circle_sql2
            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today":circle_data[0][0],"today_public_total_price":circle_data[0][1],"today_publish_sell_count":circle_data[0][2],
                "yesterday":circle_data[1][0],"yes_public_total_price":circle_data[1][1],"yes_publish_sell_count":circle_data[1][2]
            }

            if time_type == 1:
                today_sql = '''select DATE_FORMAT(up_time, '%Y%m%d %H') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,if(sum(count),sum(count),0) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and DATE_FORMAT(up_time, '%Y%m%d') = CURRENT_DATE()'''
            else:
                today_sql = '''select DATE_FORMAT(up_time, '%%Y-%%m-%%d %%H') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,if(sum(count),sum(count),0) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and DATE_FORMAT(up_time, '%%Y-%%m-%%d') = "%s"''' %query_time

            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            if args_phone_lists:
                condition_sql = " and sell_phone not in (%s)" % args_phone_lists
                today_sql = today_sql + condition_sql + group_order_sql
            else:
                today_sql = today_sql + group_order_sql

            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["publish_total_price"] = int(td[1])
                td_dict["publish_total_count"] = float(td[2])
                td_dict["publish_sell_count"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle":circle,"today":today}
            return {"code":"0000","status":"successs","msg":last_data}
        elif time_type == 2 or time_type == 3:
            circle_sql = ""
            if time_type == 2:
                query_range = ["-0", "-6", "-7", "-13"]
            elif time_type == 3:
                query_range = ["-0", "-29", "-30", "-59"]

            if args_phone_lists:
                circle_sql = '''select "current" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and sell_phone not in (%s) 
                and DATE_FORMAT(up_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(up_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)
                group by statistic_time order by statistic_time desc
                ) a
                union all
                select "last" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and sell_phone not in (%s) 
                and DATE_FORMAT(up_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(up_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)
                group by statistic_time order by statistic_time desc) b''' %(args_phone_lists,query_range[0],query_range[1],args_phone_lists,query_range[2],query_range[3])
            else:
                circle_sql = '''select "current" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1
                and DATE_FORMAT(up_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(up_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)
                group by statistic_time order by statistic_time desc) a
                union all
                select "last" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1
                and DATE_FORMAT(up_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(up_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)
                group by statistic_time order by statistic_time desc) b''' %(query_range[0],query_range[1],query_range[2],query_range[3])



            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_public_total_price": circle_data[0][1],"today_publish_sell_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_public_total_price": circle_data[1][1],"yes_publish_sell_count": circle_data[1][2]}

            # 本周
            week_sql = '''select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,if(sum(count),sum(count),0) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and DATE_FORMAT(up_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(up_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)'''  %(query_range[0],query_range[1])
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            if args_phone_lists:
                condition_sql = ''' and sell_phone not in (%s)''' %args_phone_lists
                week_sql = week_sql + condition_sql + group_order_sql
            else:
                week_sql = week_sql + group_order_sql

            logger.info(week_sql)
            cursor.execute(week_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["publish_total_price"] = int(td[1])
                td_dict["publish_total_count"] = float(td[2])
                td_dict["publish_sell_count"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle": circle, "today": today}
            return {"code": "0000", "status": "successs", "msg": last_data}

        elif time_type == 4:
            sub_day = int(daysss.days + 1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")


            if args_phone_lists:
                circle_sql = '''select "current" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and sell_phone not in (%s) 
                and up_time <= "%s" and up_time >= "%s"
                group by statistic_time order by statistic_time desc
                ) a
                union all
                select "last" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 and sell_phone not in (%s) 
                and up_time <= "%s" and up_time >= "%s"
                group by statistic_time order by statistic_time desc) b''' %(args_phone_lists,end_time,start_time,args_phone_lists,before_end_time,before_start_time)
            else:
                circle_sql = '''select "current" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1
                and up_time <= "%s" and up_time >= "%s"
                group by statistic_time order by statistic_time desc) a
                union all
                select "last" week,if(sum(publish_total_price),sum(publish_total_price),0) publish_total_price,sum(publish_sell_count) publish_sell_count from(
                select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1
                and up_time <= "%s" and up_time >= "%s"
                group by statistic_time order by statistic_time desc) b''' %(end_time,start_time,before_end_time,before_start_time)

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)

            circle = {
                "today": circle_data[0][0], "today_public_total_price": circle_data[0][1],
                "today_publish_sell_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_public_total_price": circle_data[1][1],
                "yes_publish_sell_count": circle_data[1][2]}

            # 自定义
            sql = '''select DATE_FORMAT(up_time, '%%Y-%%m-%%d') AS statistic_time,if(sum(total_price),sum(total_price),0) publish_total_price,if(sum(count),sum(count),0) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1  and up_time <= "%s" and up_time >= "%s"''' %(end_time,start_time)
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''
            if args_phone_lists:
                condition_sql = ''' and sell_phone not in (%s)''' %(args_phone_lists)
                sql = sql + condition_sql + group_order_sql
            else:
                sql = sql + group_order_sql
            logger.info(sql)
            cursor.execute(sql)
            current_datas = cursor.fetchall()
            logger.info(current_datas)
            datas = []
            for td in reversed(current_datas):
                logger.info(td)
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["publish_total_price"] = int(td[1])
                td_dict["publish_total_count"] = float(td[2])
                td_dict["publish_sell_count"] = float(td[3])
                datas.append(td_dict)
            logger.info(datas)
            last_data = {"circle": circle,"data": datas}
            return {"code": "0000", "status": "successs", "msg": last_data}
        else:
            return {"code": "11009", "status": "failed", "msg": message["11009"]}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()
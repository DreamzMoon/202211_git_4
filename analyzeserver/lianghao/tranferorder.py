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

lhbp = Blueprint('transfer', __name__, url_prefix='/lh/transfer')

@lhbp.route("/total",methods=["POST"])
def transfer_all():
    try:
        logger.info(request.json)
        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]

        #连接靓号数据库 同步
        conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)
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
                if phone_lists_result[0] == 0:
                    args_list = phone_lists_result[1]

                # try:
                #     phone_lists = []
                #     conn_crm = direct_get_conn(crm_mysql_conf)
                #     crm_cursor = conn_crm.cursor()
                #     sql = '''select * from luke_lukebus.operationcenter where find_in_set(operatename,%s)'''
                #     crm_cursor.execute(sql,(",".join(bus_lists)))
                #     operate_datas = crm_cursor.fetchall()
                #     logger.info("operate_datas:%s" %operate_datas)
                #     filter_phone_lists = []
                #     for operate_data in operate_datas:
                #         below_person_sql = '''
                #         select a.*,b.operatename from
                #         (WITH RECURSIVE temp as (
                #             SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                #             UNION ALL
                #             SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id
                #         )
                #         SELECT * FROM temp
                #         )a left join luke_lukebus.operationcenter b
                #         on a.id = b.unionid
                #             where a.phone != ""
                #         '''
                #         logger.info(operate_data)
                #         crm_cursor.execute(below_person_sql,operate_data["telephone"])
                #         below_datas = crm_cursor.fetchall()
                #         logger.info(len(below_datas))
                #         #找运营中心
                #         other_operatecenter_phone_list = []
                #         all_phone_lists = []
                #
                #         # 直接下级的运营中心
                #         all_phone_lists.append(below_datas[0]["phone"])
                #         # logger.info('operate_data["unionid"]:%s' %operate_data["unionid"])
                #         for i in range(0,len(below_datas)):
                #             if i == 0:
                #                 continue
                #             if below_datas[i]["operatename"]:
                #                 other_operatecenter_phone_list.append(below_datas[i]["phone"])
                #             all_phone_lists.append(below_datas[i]["phone"])
                #
                #         logger.info("other_operatecenter_phone_list:%s" %other_operatecenter_phone_list)
                #         #对这些手机号码进行下级查询
                #         # filter_phone_lists = []
                #         for center_phone in other_operatecenter_phone_list:
                #             logger.info(center_phone)
                #             if center_phone in filter_phone_lists:
                #                 continue
                #             filter_sql = '''
                #             select a.*,b.operatename from
                #             (WITH RECURSIVE temp as (
                #                 SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                #                 UNION ALL
                #                 SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id
                #             )
                #             SELECT * FROM temp
                #             )a left join luke_lukebus.operationcenter b
                #             on a.id = b.unionid
                #                 where a.phone != "" and phone != %s
                #             '''
                #             crm_cursor.execute(filter_sql,(center_phone,center_phone))
                #             filter_data = crm_cursor.fetchall()
                #             for k in range(0,len(filter_data)):
                #                 filter_phone_lists.append(filter_data[k]["phone"])
                #
                #         logger.info("filter_phone_lists:%s" %filter_phone_lists)
                #         logger.info("all_phone_lists:%s" %all_phone_lists)
                #
                #         phone_lists = list(set(all_phone_lists)-set(filter_phone_lists))
                #         logger.info(len(phone_lists))
                #         args_list = ",".join(phone_lists)
                #         logger.info("args_list:%s" %args_list)
                #
                # except Exception as e:
                #     logger.exception(e)
                #     return {"code": "10006", "status": "failed", "msg": message["10006"]}
                # finally:
                #     conn_crm.close()

            logger.info("args:%s" %args_list)

            if args_list:
                sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE() and !find_in_set (phone,%s)'''
                cursor.execute(sql,args_list)
                order_data = cursor.fetchone()

                sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0  and status != 1
                                and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE() and !find_in_set (sell_phone,%s)'''
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
                and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
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

        return {"code":"0000","status":"success","data":last_data}


    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


@lhbp.route("buy",methods=["POST"])
def transfer_buy_order():
    try:
        conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        if not conn_read:
            return {"code":"10008","status":"failed","msg":message["10008"]}

        logger.info(request.json)
        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]
        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = request.json["time_type"]
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)
        cursor = conn_read.cursor()

        #校验参数
        if time_type == "4":
            if not start_time or not end_time:
                return {"code":"10009","status":"failed","msg":message["10009"]}

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
            try:
                phone_lists = []
                conn_crm = direct_get_conn(crm_mysql_conf)
                crm_cursor = conn_crm.cursor()
                sql = '''select * from luke_lukebus.operationcenter where find_in_set(operatename,%s)'''
                crm_cursor.execute(sql,(",".join(bus_lists)))
                operate_datas = crm_cursor.fetchall()
                logger.info("operate_datas:%s" %operate_datas)
                filter_phone_lists = []
                for operate_data in operate_datas:
                    below_person_sql = '''
                    select a.*,b.operatename from 
                    (WITH RECURSIVE temp as (
                        SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                        UNION ALL
                        SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id)
                    SELECT * FROM temp
                    )a left join luke_lukebus.operationcenter b
                    on a.id = b.unionid where a.phone != ""
                    '''
                    logger.info(operate_data)
                    crm_cursor.execute(below_person_sql,operate_data["telephone"])
                    below_datas = crm_cursor.fetchall()
                    logger.info(len(below_datas))
                    #找运营中心
                    other_operatecenter_phone_list = []
                    all_phone_lists = []

                    # 直接下级的运营中心
                    all_phone_lists.append(below_datas[0]["phone"])

                    for i in range(0,len(below_datas)):
                        if i == 0:
                            continue
                        if below_datas[i]["operatename"]:
                            other_operatecenter_phone_list.append(below_datas[i]["phone"])
                        all_phone_lists.append(below_datas[i]["phone"])

                    logger.info("other_operatecenter_phone_list:%s" %other_operatecenter_phone_list)
                    #对这些手机号码进行下级查询
                    for center_phone in other_operatecenter_phone_list:
                        logger.info(center_phone)
                        if center_phone in filter_phone_lists:
                            continue
                        filter_sql = '''
                        select a.*,b.operatename from 
                        (WITH RECURSIVE temp as (
                            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                            UNION ALL
                            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id)
                        SELECT * FROM temp
                        )a left join luke_lukebus.operationcenter b
                        on a.id = b.unionid 
                            where a.phone != "" and phone != %s
                        '''
                        crm_cursor.execute(filter_sql,(center_phone,center_phone))
                        filter_data = crm_cursor.fetchall()
                        for k in range(0,len(filter_data)):
                            filter_phone_lists.append(filter_data[k]["phone"])

                    phone_lists = list(set(all_phone_lists)-set(filter_phone_lists))
                    logger.info(len(phone_lists))
                    args_phone_lists = ",".join(phone_lists)


            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}
            finally:
                conn_crm.close()



        # 如果选择今天的就按照今天的时间返回
        if time_type == "1":
            #今日
#             circle_sql = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE() union all
# select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''

            circle_sql1 = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
            circle_conn = " union all"
            circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''


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
                "today":circle_data[0][0],"today_buy_total_price":circle_data[0][1],
                "yesterday":circle_data[1][0],"yes_buy_total_price":circle_data[1][1]
            }

            # today_sql = '''select DATE_FORMAT(create_time, '%Y%m%d %H') AS statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE group by statistic_time order by statistic_time desc'''
            today_sql = '''select DATE_FORMAT(create_time, '%Y%m%d %H') AS statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            if args_phone_lists:
                condition_sql = " and phone not in (%s)" % args_phone_lists
                today_sql = today_sql + condition_sql + group_order_sql
            else:
                today_sql = today_sql + group_order_sql

            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle":circle,"today":today}
            return {"code":"0000","status":"successs","msg":last_data}
        elif time_type == "2":
            circle_sql = ""
            if args_phone_lists:
                circle_sql = '''select "current_week" week,sum(buy_total_price) buy_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone not in (%s) group by statistic_time order by statistic_time desc limit 7) a
                            union all
                            select "last_week" week,sum(buy_total_price) buy_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone not in (%s) group by statistic_time order by statistic_time desc limit 7,7) b''' %(args_phone_lists,args_phone_lists)
            else:
                circle_sql = '''select "current_week" week,sum(buy_total_price) buy_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 7) a
                union all
                select "last_week" week,sum(buy_total_price) buy_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 7,7) b'''



            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1]
            }
            # 本周
            # week_sql = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 7'''
            week_sql = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
            group_order_sql = ''' group by statistic_time order by statistic_time desc limit 7'''

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
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle": circle, "today": today}
            return {"code": "0000", "status": "successs", "msg": last_data}
        elif time_type == "3":
            circle_sql = ""

            if args_phone_lists:
                circle_sql = '''select "current_month" week,sum(buy_total_price) buy_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone not in (%s) group by statistic_time order by statistic_time desc limit 30) a
                            union all
                            select "last_month" week,sum(buy_total_price) buy_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone not in (%s) group by statistic_time order by statistic_time desc limit 30,30) b''' %(args_phone_lists,args_phone_lists)

            else:
                circle_sql = '''select "current_month" week,sum(buy_total_price) buy_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 30) a
                union all
                select "last_month" week,sum(buy_total_price) buy_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 30,30) b'''
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1]
            }
            # 本月
            # week_sql = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 30'''
            week_sql = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
            group_order_sql = ''' group by statistic_time order by statistic_time desc limit 30'''

            if args_phone_lists:
                condition_sql = " and phone not in (%s)" %(args_phone_lists)
                week_sql = week_sql + condition_sql + group_order_sql
            else:
                week_sql = week_sql + group_order_sql

            cursor.execute(week_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle": circle, "today": today}
            return {"code": "0000", "status": "successs", "msg": last_data}
        elif time_type == "4":
            logger.info("1111")
            # 自定义
            # sql = '''select DATE_FORMAT(create_time,"%Y%m%d") statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time >= "2021-11-01 00:00:00" and create_time <= "2021-11-03 00:00:00"
            #                 group by statistic_time order by statistic_time desc'''
            sql = '''select DATE_FORMAT(create_time,"%%Y%%m%%d") statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time >= %s and create_time <= %s'''
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''
            if args_phone_lists:
                condition_sql = ''' and phone not in (%s)''' %(args_phone_lists)
                sql = sql + condition_sql + group_order_sql
            else:
                sql = sql + group_order_sql
            logger.info(sql)
            cursor.execute(sql,(start_time,end_time))
            current_datas = cursor.fetchall()
            logger.info(current_datas)
            datas = []
            for td in current_datas:
                logger.info(td)
                td_dict = {}
                td_dict["buy_order_count"] = int(td[0])
                td_dict["buy_total_count"] = float(td[1])
                td_dict["buy_total_price"] = float(td[2])
                datas.append(td_dict)
            logger.info(datas)
            last_data = {"data": datas}
            return {"code": "0000", "status": "successs", "msg": last_data}
        else:
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()



@lhbp.route("sell",methods=["POST"])
def transfer_sell_order():
    try:
        conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        if not conn_read:
            return {"code":"10008","status":"failed","msg":message["10008"]}

        logger.info(request.json)
        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]
        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = request.json["time_type"]
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)
        cursor = conn_read.cursor()

        #校验参数
        if time_type == "4":
            if not start_time or not end_time:
                return {"code":"10009","status":"failed","msg":message["10009"]}

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
            try:
                phone_lists = []
                conn_crm = direct_get_conn(crm_mysql_conf)
                crm_cursor = conn_crm.cursor()
                sql = '''select * from luke_lukebus.operationcenter where find_in_set(operatename,%s)'''
                crm_cursor.execute(sql,(",".join(bus_lists)))
                operate_datas = crm_cursor.fetchall()
                logger.info("operate_datas:%s" %operate_datas)
                filter_phone_lists = []
                for operate_data in operate_datas:
                    below_person_sql = '''
                    select a.*,b.operatename from 
                    (WITH RECURSIVE temp as (
                        SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                        UNION ALL
                        SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id)
                    SELECT * FROM temp
                    )a left join luke_lukebus.operationcenter b
                    on a.id = b.unionid where a.phone != ""
                    '''
                    logger.info(operate_data)
                    crm_cursor.execute(below_person_sql,operate_data["telephone"])
                    below_datas = crm_cursor.fetchall()
                    logger.info(len(below_datas))
                    #找运营中心
                    other_operatecenter_phone_list = []
                    all_phone_lists = []

                    # 直接下级的运营中心
                    all_phone_lists.append(below_datas[0]["phone"])

                    for i in range(0,len(below_datas)):
                        if i == 0:
                            continue
                        if below_datas[i]["operatename"]:
                            other_operatecenter_phone_list.append(below_datas[i]["phone"])
                        all_phone_lists.append(below_datas[i]["phone"])

                    logger.info("other_operatecenter_phone_list:%s" %other_operatecenter_phone_list)
                    #对这些手机号码进行下级查询
                    for center_phone in other_operatecenter_phone_list:
                        logger.info(center_phone)
                        if center_phone in filter_phone_lists:
                            continue
                        filter_sql = '''
                        select a.*,b.operatename from 
                        (WITH RECURSIVE temp as (
                            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                            UNION ALL
                            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id)
                        SELECT * FROM temp
                        )a left join luke_lukebus.operationcenter b
                        on a.id = b.unionid 
                            where a.phone != "" and phone != %s
                        '''
                        crm_cursor.execute(filter_sql,(center_phone,center_phone))
                        filter_data = crm_cursor.fetchall()
                        for k in range(0,len(filter_data)):
                            filter_phone_lists.append(filter_data[k]["phone"])

                    phone_lists = list(set(all_phone_lists)-set(filter_phone_lists))
                    logger.info(len(phone_lists))
                    args_phone_lists = ",".join(phone_lists)


            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}
            finally:
                conn_crm.close()



        # 如果选择今天的就按照今天的时间返回
        if time_type == "1":
            #今日
            circle_sql1 = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
            circle_conn = " union all"
            circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''


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
                "today":circle_data[0][0],"today_sell_total_price":circle_data[0][1],
                "yesterday":circle_data[1][0],"yes_sell_total_price":circle_data[1][1]
            }

            today_sql = '''select DATE_FORMAT(create_time, '%Y%m%d %H') AS statistic_time,count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
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
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["sell_order_count"] = int(td[1])
                td_dict["sell_total_count"] = float(td[2])
                td_dict["sell_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle":circle,"today":today}
            return {"code":"0000","status":"successs","msg":last_data}
        elif time_type == "2":
            circle_sql = ""
            if args_phone_lists:
                circle_sql = '''select "current_week" week,sum(sell_total_price) sell_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone not in (%s) group by statistic_time order by statistic_time desc limit 7) a
                            union all
                            select "last_week" week,sum(sell_total_price) sell_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone not in (%s) group by statistic_time order by statistic_time desc limit 7,7) b''' %(args_phone_lists,args_phone_lists)
            else:
                circle_sql = '''select "current_week" week,sum(sell_total_price) sell_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 7) a
                union all
                select "last_week" week,sum(sell_total_price) sell_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 7,7) b'''



            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_sell_total_price": circle_data[0][1],
                "yesterday": circle_data[1][0], "yes_sell_total_price": circle_data[1][1]
            }
            # 本周
            week_sql = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
            group_order_sql = ''' group by statistic_time order by statistic_time desc limit 7'''

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
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["sell_order_count"] = int(td[1])
                td_dict["sell_total_count"] = float(td[2])
                td_dict["sell_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle": circle, "today": today}
            return {"code": "0000", "status": "successs", "msg": last_data}
        elif time_type == "3":
            circle_sql = ""

            if args_phone_lists:
                circle_sql = '''select "current_month" week,sum(sell_total_price) sell_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone not in (%s) group by statistic_time order by statistic_time desc limit 30) a
                            union all
                            select "last_month" week,sum(sell_total_price) sell_total_price from(
                            select DATE_FORMAT(create_time, '%%Y%%m%%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone not in (%s) group by statistic_time order by statistic_time desc limit 30,30) b''' %(args_phone_lists,args_phone_lists)

            else:
                circle_sql = '''select "current_month" week,sum(sell_total_price) sell_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 30) a
                union all
                select "last_month" week,sum(sell_total_price) sell_total_price from(
                select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by statistic_time order by statistic_time desc limit 30,30) b'''
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_sell_total_price": circle_data[0][1],
                "yesterday": circle_data[1][0], "yes_sell_total_price": circle_data[1][1]
            }
            # 本月
            week_sql = '''select DATE_FORMAT(create_time, '%Y%m%d') statistic_time,count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
            group_order_sql = ''' group by statistic_time order by statistic_time desc limit 30'''

            if args_phone_lists:
                condition_sql = " and sell_phone not in (%s)" %(args_phone_lists)
                week_sql = week_sql + condition_sql + group_order_sql
            else:
                week_sql = week_sql + group_order_sql

            cursor.execute(week_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["sell_order_count"] = int(td[1])
                td_dict["sell_total_count"] = float(td[2])
                td_dict["sell_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            last_data = {"circle": circle, "today": today}
            return {"code": "0000", "status": "successs", "msg": last_data}
        elif time_type == "4":
            logger.info("1111")
            # 自定义
            sql = '''select DATE_FORMAT(create_time,"%%Y%%m%%d") statistic_time,count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and create_time >= %s and create_time <= %s'''
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''
            if args_phone_lists:
                condition_sql = ''' and phone not in (%s)''' %(args_phone_lists)
                sql = sql + condition_sql + group_order_sql
            else:
                sql = sql + group_order_sql
            logger.info(sql)
            cursor.execute(sql,(start_time,end_time))
            current_datas = cursor.fetchall()
            logger.info(current_datas)
            datas = []
            for td in current_datas:
                logger.info(td)
                td_dict = {}
                td_dict["sell_order_count"] = int(td[0])
                td_dict["sell_total_count"] = float(td[1])
                td_dict["sell_total_price"] = float(td[2])
                datas.append(td_dict)
            logger.info(datas)
            last_data = {"data": datas}
            return {"code": "0000", "status": "successs", "msg": last_data}
        else:
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()
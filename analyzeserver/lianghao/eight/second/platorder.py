# -*- coding: utf-8 -*-

# @Time : 2021/12/23 15:16

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : platorder.py


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


platsecondbp = Blueprint('sectran', __name__, url_prefix='/le/second')

@platsecondbp.route("/plat/total",methods=["POST"])
def transfer_all():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()

        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]

        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

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
                    # 走统计表
                    try:
                        sql = '''select phone from crm_user_{} where find_in_set (unionid,%s)'''.format(current_time)
                        ags_list = ",".join(unioinid_lists)
                        logger.info(ags_list)
                        cursor_analyze.execute(sql, ags_list)
                        phone_lists = cursor_analyze.fetchall()
                        for p in phone_lists:
                            args_list.append(p[0])
                        args_list = ",".join(args_list)
                    except Exception as e:
                        logger.exception(e)
                        return {"code": "10006", "status": "failed", "msg": message["10006"]}


                #过滤运营中心的
                if bus_lists:
                    str_bus_lists = ",".join(bus_lists)
                    sql = '''select not_contains from operate_relationship_crm where find_in_set (id,%s) and crm = 1 and del_flag = 0'''
                    cursor_analyze.execute(sql, str_bus_lists)
                    phone_lists = cursor_analyze.fetchall()
                    for p in phone_lists:
                        ok_p = json.loads(p[0])
                        for op in ok_p:
                            args_list.append(op)
                    args_list = ",".join(args_list)

                logger.info("args:%s" %args_list)

                if args_list:
                    # 今天的
                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from le_order where `status` = 1 and  del_flag = 0 and type = 4 and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE() and phone not in (%s)''' %args_list
                    cursor.execute(sql)
                    order_data = cursor.fetchone()

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count
                    from le_second_hand_sell where del_flag=0  and DATE_FORMAT(create_time,"%%Y-%%m-%%d") = CURRENT_DATE()
                     and sell_phone not in (%s)''' %args_list
                    cursor.execute(sql)
                    sell_data = cursor.fetchone()

                    #总的
                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from le_order where `status` = 1 and  del_flag = 0 and type = 4 and phone not in (%s)''' % args_list
                    if start_time and end_time:
                        time_condition = ''' and date_format(create_time,"%%Y-%%m-%%d") >= "%s" and date_format(create_time,"%%Y-%%m-%%d") <= "%s"''' %(start_time,end_time)
                        sql = sql + time_condition
                    cursor.execute(sql)
                    all_order_data = cursor.fetchone()

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from le_second_hand_sell where del_flag=0 and sell_phone not in (%s)''' % args_list
                    if start_time and end_time:
                        time_condition = ''' and date_format(create_time,"%%Y-%%m-%%d") >= "%s" and date_format(create_time,"%%Y-%%m-%%d") <= "%s"''' % (start_time, end_time)
                        sql = sql + time_condition
                    cursor.execute(sql)

                    all_sell_data = cursor.fetchone()

                else:
                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from le_order where `status` = 1 and  del_flag = 0 and type = 4 and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                    cursor.execute(sql)
                    order_data = cursor.fetchone()
                    logger.info(order_data)

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count
                    from le_second_hand_sell where del_flag=0
                    and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                    cursor.execute(sql)
                    sell_data = cursor.fetchone()
                    logger.info(sell_data)

                    sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from le_order where `status` = 1 and  del_flag = 0 and type = 4'''
                    if start_time and end_time:
                        time_condition = ''' and date_format(create_time,"%%Y-%%m-%%d") >= "%s" and date_format(create_time,"%%Y-%%m-%%d") <= "%s"''' % (start_time, end_time)
                        sql = sql + time_condition
                    logger.info(sql)
                    cursor.execute(sql)
                    all_order_data = cursor.fetchone()

                    sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from le_second_hand_sell where del_flag=0 '''
                    if start_time and end_time:
                        time_condition = ''' and date_format(create_time,"%%Y-%%m-%%d") >= "%s" and date_format(create_time,"%%Y-%%m-%%d") <= "%s"''' % (start_time, end_time)
                        sql = sql + time_condition
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
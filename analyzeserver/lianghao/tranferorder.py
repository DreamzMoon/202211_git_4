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
            if phone_lists:
                args_list = ",".join(phone_lists)
                logger.info(args_list)

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
            logger.info("args:%s" %args_list)
            if args_list:
                sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE() and !find_in_set (phone,%s)'''
                cursor.execute(sql,args_list)
                order_data = cursor.fetchone()

                sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0
                                and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE() and !find_in_set (sell_phone,%s)'''
                cursor.execute(sql,args_list)
                sell_data = cursor.fetchone()
                logger.info(sell_data)

                sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and !find_in_set (phone,%s)'''
                cursor.execute(sql,args_list)
                all_order_data = cursor.fetchone()

                sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and !find_in_set (sell_phone,%s)'''
                cursor.execute(sql,args_list)
                all_sell_data = cursor.fetchone()

            else:
                sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE()'''
                cursor.execute(sql)
                order_data = cursor.fetchone()
                logger.info(order_data)

                sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 
                and DATE_FORMAT(create_time, '%%Y%%m%%d') = CURRENT_DATE()'''
                cursor.execute(sql)
                sell_data = cursor.fetchone()
                logger.info(sell_data)

                sql = '''select count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price, count(*) sell_order_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
                cursor.execute(sql)
                all_order_data = cursor.fetchone()

                sql = '''select sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 '''
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

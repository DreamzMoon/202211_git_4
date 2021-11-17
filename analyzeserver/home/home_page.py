# -*- coding: utf-8 -*-

# @Time : 2021/11/16 17:11

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : home_page.py

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
from analyzeserver.common import *
import threading


homebp = Blueprint('homepage', __name__, url_prefix='/home')

# 今日成交排行版--个人 每分钟刷新一次
@homebp.route("deal/person",methods=["GET"])
def deal_person():
    try:
        try:
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_crm = direct_get_conn(crm_mysql_conf)
        logger.info(conn_lh)

        sql = '''select nick_name,phone,sum(total_price) total_money from lh_order where del_flag = 0 and `status`=1 and type in (1,4) and DATE_FORMAT(create_time,"%Y%m%d") =CURRENT_DATE() group by phone order by total_money desc limit 3'''

        logger.info(sql)
        datas = pd.read_sql(sql,conn_lh)
        datas = datas.to_dict("records")
        return {"code":"0000","status":"success","msg":datas}

    except Exception as e:
        logger.error(e)
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()


# 运营中心每小时刷新一次
@homebp.route("deal/bus",methods=["GET"])
def deal_bus():
    try:
        try:
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        fifter_operate = ["测试", "乔二运营中心", "快了", "测试公司", "卡拉公司", "施鸿公司", "快乐公司123", "禄可集团杭州技术生产部", "王大锤", "福州高新区测试运营中心, 请勿选择"]
        # fifter_operate = ["测试","乔二运营中心"]

        logger.info(fifter_operate)
        #先查运营中心的人数
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        cursor_crm = conn_crm.cursor()
        cursor_lh = conn_lh.cursor()

        bus_sql = '''select operatename from luke_lukebus.operationcenter where operatename not in (%s)''' %(','.join(["'%s'" % item for item in fifter_operate]))

        logger.info(bus_sql)
        cursor_crm.execute(bus_sql)
        operate_datas = cursor_crm.fetchall()

        logger.info(operate_datas)
        logger.info(len(operate_datas))
        operate_list = []
        for operate_data in operate_datas:
            operate_dict = {}
            result = get_lukebus_phone([operate_data["operatename"]])
            # result = get_operationcenter_child(operate_data["id"])
            operate_dict["phone_list"] = result[1]
            operate_dict["operatename"] = operate_data["operatename"]
            operate_list.append(operate_dict)
        logger.info(operate_list)

        return_lists = []
        for ol in operate_list:
            ol_dict = {}
            sql = '''select sum(total_price) total_money from lh_order where del_flag = 0 and type in (1,4) and `status`=1 and phone in (%s)''' %(ol["phone_list"])
            cursor_lh.execute(sql)
            ol_dict["operatename"] = ol["operatename"]
            ol_dict["total_money"]=cursor_lh.fetchone()[0]
            return_lists.append(ol_dict)
        logger.info(return_lists)
        return "1"

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_crm.close()
        conn_lh.close()


@homebp.route("/today/data", methods=["POST"])
def today_data():
    try:
        logger.info(request.json)
        # 参数个数错误
        if len(request.json) != 4:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        # 1今日 2昨日 3自定义-->必须传起始和结束时间
        time_type = request.json['time_type']
        # 首次发布时间
        start_time = request.json['start_time']
        end_time = request.json['end_time']

        if (time_type != 3 and start_time and end_time) or time_type not in range(1, 3) or (
                time_type == 3 and not start_time and not end_time):
            return {"code": "10014", "status": "failed", "msg": message["10014"]}
        # 时间判断
        elif start_time or end_time:
            judge_result = judge_start_and_end_time(start_time, end_time)
            if not judge_result[0]:
                return {"code": judge_result[1], "status": "failed", "msg": message[judge_result[1]]}
            sub_day = judge_result[1] - judge_result[0]
            if sub_day.days + sub_day.seconds / (60.0 * 60.0) > 24:
                return {"code": "10015", "status": "failed", "msg": message["10015"]}
            request.json['start_time'] = judge_result[0]
            request.json['end_time'] = judge_result[1]
    except Exception as e:
        # 参数名错误
        logger.error(e)
        return {"code": "10009", "status": "failed", "msg": message["10009"]}
    # 今日
    if time_type == 1:
        today_sql = '''
            select date_format(create_time,"%%H:00") today_time, sum(total_price) total_price, count(*) order_count, count(distinct phone) order_person, sum(count) pretty_count
            from lh_order
            where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and date_format(create_time,"%%Y-%%m-%%d") = curdate()
            group by today_time
            order by today_time desc
        '''
        yesterday_sql = '''
            select date_format(create_time,"%%H:00") today_time, sum(total_price) total_price, count(*) order_count, count(distinct phone) order_person, sum(count) pretty_count
            from lh_order
            where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and date_format(create_time,"%%Y-%%m-%%d") = date_sub(curdate(), interval 1 day)
            group by today_time
            order by today_time desc
        '''
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}
        cursor = conn_lh.cursor()

        cursor.execute(today_sql)
        today_df = pd.DataFrame(cursor.fetchall())

        cursor.execute(yesterday_sql)
        yesterday_df = pd.DataFrame(cursor.fetchall())

        # 环比
        # 今日交易金额
        today_price = today_df['total_price'].sum()
        # 今日交易订单数
        today_order_count = today_df['order_count'].sum()
        # 今日交易人数
        today_order_person = today_df['order_person'].sum()

        # 昨日交易金额
        yesterday_price = yesterday_df['total_price'].sum()
        # 昨日交易订单数
        yesterday_order_count = yesterday_df['order_count'].sum()
        # 昨日交易人数
        yesterday_order_person = yesterday_df['order_person'].sum()




    # pass
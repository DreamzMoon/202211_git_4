# -*- coding: utf-8 -*-

# @Time : 2021/12/13 11:17

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : daily_summary.py


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
from functools import reduce
import json

dailybp = Blueprint('daily', __name__, url_prefix='/lh/daily')

'''平台每日订单数据统计报表'''
@dailybp.route("plat",methods=["POST"])
def daily_plat_summary():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()

        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        logger.info(request.json)
        page = request.json.get("page")
        size = request.json.get("size")

        unioinid_lists = request.json["unioinid_lists"]
        phone_lists = request.json["phone_lists"]
        bus_lists = request.json["bus_lists"]

        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")

        args_list = []
        # 过滤手机号
        if phone_lists:
            args_list = ",".join(phone_lists)
            logger.info(args_list)
        # 过滤用户id
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

        # 过滤运营中心的
        if bus_lists:
            str_bus_lists = ",".join(bus_lists)
            sql = '''select not_contains from operate_relationship_crm where find_in_set (id,%s) and crm = 1 and del_flag = 0'''
            cursor_analyze.execute(sql,str_bus_lists)
            phone_lists = cursor_analyze.fetchall()
            for p in phone_lists:
                ok_p = json.loads(p[0])
                for op in ok_p:
                    args_list.append(op)
            args_list = ",".join(args_list)

        #过滤手机号码
        logger.info("args:%s" % args_list)

        code_page = ""
        code_size = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size


        buy_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) buy_order_count,sum(count) buy_lh_count,sum(total_price) buy_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0'''
        sell_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) sell_order_count,sum(count) sell_lh_count,sum(total_price) sell_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0'''
        public_sql = '''select DATE_FORMAT(lh_sell.create_time,"%Y-%m-%d") statistic_time,count(*) public_order_count,sum(lh_sell.count) public_lh_count,sum(lh_sell.total_price) public_total_price,sum(sell_fee) total_sell_fee,(sum(lh_sell.total_price)-sum(sell_fee)) total_real_money from lh_sell
        left join lh_order on lh_sell.id = lh_order.sell_id
        where lh_sell.del_flag = 0 and lh_sell.`status`!=1 and lh_order.del_flag = 0
        '''

        buy_group_sql = ''' group by statistic_time having statistic_time != CURRENT_DATE'''
        sell_group_sql = ''' group by statistic_time having statistic_time != CURRENT_DATE'''
        public_group_sql = ''' group by statistic_time  having statistic_time != CURRENT_DATE'''

        if args_list:
            buy_condition = " and phone not in (%s)" % args_list
            sell_condition = " and sell_phone not in (%s)" % args_list
            public_condition = " and lh_sell.sell_phone not in (%s)" % args_list

            buy_sql = buy_sql + buy_condition + buy_group_sql
            sell_sql = sell_sql + sell_condition + sell_group_sql
            public_sql = public_sql + public_condition + public_group_sql
        else:
            buy_sql = buy_sql + buy_group_sql
            sell_sql = sell_sql + sell_group_sql
            public_sql = public_sql + public_group_sql

        buy_data = pd.read_sql(buy_sql, conn_lh)
        sell_data = pd.read_sql(sell_sql, conn_lh)
        public_data = pd.read_sql(public_sql, conn_lh)

        df_list = []
        df_list.append(buy_data)
        df_list.append(sell_data)
        df_list.append(public_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['statistic_time'], how='outer'), df_list)
        df_merged.sort_values(by=["statistic_time"],ascending=[False],inplace=True)

        if start_time and end_time:
            df_merged = df_merged[(df_merged["statistic_time"] >= start_time) & (df_merged["statistic_time"] <= end_time)]

        all_data = {}
        #准备算钱
        all_data["buy_order_count"] = int(df_merged["buy_lh_count"].sum())
        all_data["buy_lh_count"] = int(df_merged["buy_lh_count"].sum())
        all_data["buy_total_price"] = round(df_merged["buy_total_price"].sum(),2)
        all_data["public_lh_count"] = int(df_merged["public_lh_count"].sum())
        all_data["public_order_count"] = int(df_merged["public_order_count"].sum())
        all_data["public_total_price"] = round(df_merged["public_total_price"].sum(),2)
        all_data["sell_lh_count"] = int(df_merged["sell_lh_count"].sum())
        all_data["sell_order_count"] = int(df_merged["sell_order_count"].sum())
        all_data["sell_total_price"] = round(df_merged["sell_total_price"].sum(),2)
        all_data["total_real_money"] = round(df_merged["total_real_money"].sum(),2)
        all_data["total_sell_fee"] = round(df_merged["total_sell_fee"].sum(),2)
        logger.info(all_data)

        df_merged.fillna("",inplace=True)
        count = len(df_merged)
        return_data = df_merged[code_page:code_size] if page and size else df_merged.copy()

        msg_data = {"all_data":all_data,"data":return_data.to_dict("records")}
        return {"code":"0000","status":"success","msg":msg_data,"count":count}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
        conn_analyze.close()

'''运营中心每日订单数据统计报表'''
@dailybp.route("operate",methods=["POST"])
def daily_operate():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        page = request.json.get("page")
        size = request.json.get("size")
        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")
        keyword = request.json.get("keyword")

        code_page = ""
        code_size = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        sql = '''
        select day_time,operatename,leader,leader_phone,leader_unionid ,sum(buy_count) buy_count,sum(buy_pretty_count) buy_pretty_count,sum(buy_total_price) buy_total_price,sum(publish_count) publish_count,sum(publish_pretty_count) publish_pretty_count,sum(publish_total_price) publish_total_price,sum(sell_count) sell_count,sum(sell_pretty_count) sell_pretty_count,sum(sell_total_price) sell_total_price,sum(truth_price) truth_price,sum(sell_fee)sell_fee from user_daily_order_data where operatename != "" group by day_time,operatename order by day_time desc,buy_total_price desc
        '''
        order_data = pd.read_sql(sql,conn_analyze)
        order_data["day_time"] = order_data["day_time"].apply(lambda x: x.strftime('%Y-%m-%d'))
        order_data["leader_unionid"] = order_data["leader_unionid"].astype(str)

        if start_time and end_time:
            order_data = order_data[(order_data["day_time"] >= start_time) & (order_data["day_time"] <= end_time)]
        #筛选出关键词
        order_data = order_data[(order_data["leader_phone"].str.contains(keyword))|(order_data["leader"].str.contains(keyword))|(order_data["leader_unionid"].str.contains(keyword))]

        all_data = {}
        # 准备算钱
        all_data["buy_count"] = int(order_data["buy_count"].sum())
        all_data["buy_pretty_count"] = int(order_data["buy_pretty_count"].sum())
        all_data["buy_total_price"] = round(order_data["buy_total_price"].sum(), 2)
        all_data["publish_count"] = int(order_data["publish_count"].sum())
        all_data["publish_pretty_count"] = int(order_data["publish_pretty_count"].sum())
        all_data["publish_total_price"] = round(order_data["publish_total_price"].sum(), 2)
        all_data["sell_count"] = int(order_data["sell_count"].sum())
        all_data["sell_pretty_count"] = int(order_data["sell_pretty_count"].sum())
        all_data["sell_total_price"] = round(order_data["sell_total_price"].sum(), 2)
        all_data["truth_price"] = round(order_data["truth_price"].sum(), 2)
        all_data["sell_fee"] = round(order_data["sell_fee"].sum(), 2)

        count = len(order_data)
        return_data = order_data[code_page:code_size] if page and size else order_data.copy()
        return_data = return_data.to_dict("records")
        msg_data = {"data":return_data,"all_data":all_data}
        return {"code": "0000", "status": "success", "msg": msg_data, "count": count}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
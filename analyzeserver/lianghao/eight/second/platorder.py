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
from functools import reduce

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


# 个人转卖市场订单数据统计分析
@platsecondbp.route("person/total",methods=["POST"])
def personal_total():
    try:

        conn_read = direct_get_conn(lianghao_mysql_conf)

        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        page = request.json["page"]
        size = request.json["size"]

        # 可以是用户名称 手机号 unionid 模糊的
        keyword = request.json["keyword"]

        # 查询归属上级 精准的
        parent = request.json["parent"]
        # bus = request.json["bus"]
        bus_id = request.json["bus_id"]

        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_id = ""
        bus_phone = []

        # 模糊查询
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
            else:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        # 只查一个
        if parent:
            if len(parent) == 11:
                result = get_parent_by_phone(parent)
                if result[0] == 1:
                    parent_id = result[1]
                else:
                    return {"code": "11014", "status": "failed", "msg": message["code"]}
            else:
                parent_id = parent

        if bus_id:
            result = get_busphne_by_id(bus_id)
            if result[0] == 1:
                bus_phone = result[1].split(",")
            else:
                return {"code":"11015","status":"failed","msg":message["11015"]}

        logger.info(len(bus_phone))

        # 对手机号码差交集
        if keyword_phone and bus_phone:
            query_phone = list(set(keyword_phone).intersection(set(bus_phone)))
            if not query_phone:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        elif keyword_phone:
            query_phone = keyword_phone
        elif bus_phone:
            query_phone = bus_phone
        else:
            query_phone = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        logger.info("--------------------")
        logger.info(query_phone)
        logger.info("-------------------")

        # 采购
        order_sql = '''select phone,count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 4'''
        group_sql = ''' group by phone'''
        order_condition = []
        if query_phone:
            order_condition.append(''' phone in (%s) ''' %(",".join(query_phone)))
        if start_time and end_time:
            order_condition.append(''' date_format(create_time,"%%Y-%%m-%%d") >= "%s" and date_format(create_time,"%%Y-%%m-%%d") <= "%s"''' %(start_time,end_time))
        for i in range(0,len(order_condition)):
            order_sql = order_sql + " and " + order_condition[i]
        order_sql = order_sql + group_sql
        logger.info("order_sql:%s" %order_sql)
        order_data = pd.read_sql(order_sql,conn_read)

        # 出售
        sell_sql = '''select sell_phone phone,count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee from le_order where `status` = 1 and  del_flag = 0 and type = 4'''
        group_sql = ''' group by sell_phone'''
        sell_condition = []
        if query_phone:
            sell_condition.append(''' sell_phone in (%s) ''' % (",".join(query_phone)))
        if start_time and end_time:
            sell_condition.append(''' date_format(create_time,"%%Y-%%m-%%d") >= "%s" and date_format(create_time,"%%Y-%%m-%%d") <= "%s"''' % (start_time, end_time))
        for i in range(0, len(sell_condition)):
            sell_sql = sell_sql + " and " + sell_condition[i]
        sell_sql = sell_sql + group_sql
        logger.info("sell_sql:%s" %sell_sql)
        sell_order = pd.read_sql(sell_sql,conn_read)

        # 发布
        public_sql = '''select sell_phone phone,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from le_second_hand_sell where del_flag=0 '''
        group_sql = ''' group by sell_phone '''
        public_condition = []
        if query_phone:
            public_condition.append(''' sell_phone in (%s) ''' % (",".join(query_phone)))
        if start_time and end_time:
            public_condition.append(''' date_format(create_time,"%%Y-%%m-%%d") >= "%s" and date_format(create_time,"%%Y-%%m-%%d") <= "%s"''' % (start_time, end_time))
        for i in range(0, len(public_condition)):
            public_sql = public_sql + " and " + public_condition[i]
        public_sql = public_sql + group_sql
        logger.info("public_sql:%s" %public_sql)
        public_order = pd.read_sql(public_sql,conn_read)

        df_list = []
        df_list.append(order_data)
        df_list.append(sell_order)
        df_list.append(public_order)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        #无数据返回空
        if df_merged.empty:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        #这里要进行一个crm数据的合并
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        sql = '''select unionid,parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user_%s where phone != "" and phone is not null and del_flag=0''' %current_time
        logger.info(sql)
        crm_data = pd.read_sql(sql, conn_analyze)
        conn_analyze.close()
        df_merged = df_merged.merge(crm_data, how="left", on="phone")

        df_merged["parentid"] = df_merged['parentid'].astype(str)
        df_merged["unionid"] = df_merged['unionid'].astype(str)
        df_merged['parentid'] = df_merged['parentid'].apply(lambda x: del_point(x))
        df_merged['unionid'] = df_merged['unionid'].apply(lambda x: del_point(x))
        if parent_id:
            df_merged = df_merged[df_merged["parentid"] == parent_id]

        # 算全部的钱
        all_data = {"buy_count": 0, "buy_total_count": 0, "buy_total_price": 0, "sell_count": 0, "sell_fee": 0,
                    "sell_real_money": 0, "sell_total_count": 0, "sell_total_price": 0}

        # #把nan都填充0
        df_merged["buy_count"].fillna(0,inplace=True)
        df_merged["buy_total_count"].fillna(0,inplace=True)
        df_merged["buy_total_price"].fillna(0,inplace=True)
        df_merged["sell_count"].fillna(0,inplace=True)
        df_merged["sell_fee"].fillna(0,inplace=True)
        df_merged["sell_real_money"].fillna(0,inplace=True)
        df_merged["sell_total_count"].fillna(0,inplace=True)
        df_merged["sell_total_price"].fillna(0,inplace=True)
        df_merged["publish_sell_count"].fillna(0,inplace=True)
        df_merged["publish_total_count"].fillna(0,inplace=True)
        df_merged["publish_total_price"].fillna(0,inplace=True)

        df_merged["buy_total_count"] = df_merged["buy_total_count"].astype(int)
        df_merged["publish_total_count"] = df_merged["publish_total_count"].astype("int")
        df_merged["sell_total_count"] = df_merged["sell_total_count"].astype("int")

        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()


        all_data["buy_count"] = int(df_merged["buy_count"].sum())
        all_data["buy_total_count"] = int(df_merged["buy_total_count"].sum())
        all_data["buy_total_price"] = round(float(df_merged["buy_total_price"].sum()), 2)
        all_data["sell_count"] = int(df_merged["sell_count"].sum())
        all_data["sell_fee"] = round(float(df_merged["sell_fee"].sum()), 2)
        all_data["sell_real_money"] = round(float(df_merged["sell_real_money"].sum()), 2)
        all_data["sell_total_count"] = int(df_merged["sell_total_count"].sum())
        all_data["sell_total_price"] = round(float(df_merged["sell_total_price"].sum()), 2)

        need_data.fillna("",inplace=True)
        msg_data = {"data": need_data.to_dict("records"), "all_data": all_data}
        logger.info(msg_data)
        return {"code": "0000", "status": "success", "msg": msg_data, "count": len(df_merged)}


    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()

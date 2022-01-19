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

def str_to_date(x):
    try:
        if x:
           return pd.to_datetime(x).strftime('%Y-%m-%d')
        else:
            pass
    except:
       return "error_birth"


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

        unioinid_lists = [x.strip() for x in request.json["unioinid_lists"]]
        phone_lists = [x.strip() for x in request.json["phone_lists"]]
        bus_lists = [x.strip() for x in request.json["bus_lists"]]

        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")
        tag_id = request.json.get("tag_id")

        args_list = []
        # 过滤手机号
        if phone_lists:
            # args_list = ",".join(phone_lists)
            args_list = phone_lists.copy()
            logger.info(args_list)
        # 过滤用户id
        if unioinid_lists:
            # 走统计表
            try:
                sql = '''select phone from crm_user where find_in_set (unionid,%s) and phone != "" and phone is not null'''
                ags_list = ",".join(unioinid_lists)
                logger.info(ags_list)
                cursor_analyze.execute(sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for p in phone_lists:
                    args_list.append(p[0])
                # args_list = ",".join(args_list)
            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}

        # 过滤运营中心的
        if bus_lists:
            str_bus_lists = ",".join(bus_lists)
            logger.info(str_bus_lists)
            # sql = '''select not_contains from operate_relationship_crm where find_in_set (id,%s) and crm = 1 and del_flag = 0'''
            sql = '''select phone from crm_user where find_in_set (operate_id,%s)  and del_flag = 0'''
            cursor_analyze.execute(sql,str_bus_lists)
            phone_lists = cursor_analyze.fetchall()
            logger.info(phone_lists)
            args_list = [p[0] for p in phone_lists]
            # args_list = ",".join(args_list)

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
                if tp in args_list:
                    continue
                else:
                    select_phone.append(tp)
        else:
            lh_user_sql = '''select distinct(phone) phone from lh_user where del_flag = 0 and phone != "" and phone is not null'''
            lh_user_phone = pd.read_sql(lh_user_sql, conn_lh)
            lh_phone = lh_user_phone["phone"].to_list()

            select_phone = list(set(lh_phone) - set(args_list))

        flag = 0
        if len(lh_phone) == len(select_phone):
            flag = 1
        else:
            flag = 0

        logger.info(flag)

        select_phone = ",".join(select_phone)

        # logger.info(select_phone)

        #过滤手机号码


        code_page = ""
        code_size = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size


        buy_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) buy_order_count,sum(count) buy_lh_count,sum(total_price) buy_total_price from lh_order where type in (1,4) and `status`=1 and del_flag = 0'''
        sell_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) sell_order_count,sum(count) sell_lh_count,sum(total_price) sell_total_price,sum(sell_fee) total_sell_fee,sum(total_price)-sum(sell_fee) total_real_money  from lh_order where type in (1,4) and `status`=1 and del_flag = 0 '''
        public_sql = '''select DATE_FORMAT(create_time,"%Y-%m-%d") statistic_time,count(*) public_order_count,sum(count) public_lh_count,sum(total_price) public_total_price from lh_sell where del_flag = 0 and `status`!=1 '''

        buy_group_sql = ''' group by statistic_time having statistic_time != CURRENT_DATE'''
        sell_group_sql = ''' group by statistic_time having statistic_time != CURRENT_DATE'''
        public_group_sql = ''' group by statistic_time  having statistic_time != CURRENT_DATE'''

        if select_phone:
            if not flag:
                buy_condition = " and phone in (%s)" % select_phone
                sell_condition = " and sell_phone in (%s)" % select_phone
                public_condition = " and sell_phone in (%s)" % select_phone

                buy_sql = buy_sql + buy_condition + buy_group_sql
                sell_sql = sell_sql + sell_condition + sell_group_sql
                public_sql = public_sql + public_condition + public_group_sql

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
                all_data["buy_order_count"] = int(df_merged["buy_order_count"].sum())
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
            else:


                buy_sql = buy_sql  + buy_group_sql
                sell_sql = sell_sql + sell_group_sql
                public_sql = public_sql + public_group_sql

                buy_data = pd.read_sql(buy_sql, conn_lh)
                sell_data = pd.read_sql(sell_sql, conn_lh)
                public_data = pd.read_sql(public_sql, conn_lh)

                df_list = []
                df_list.append(buy_data)
                df_list.append(sell_data)
                df_list.append(public_data)
                df_merged = reduce(lambda left, right: pd.merge(left, right, on=['statistic_time'], how='outer'),
                                   df_list)
                df_merged.sort_values(by=["statistic_time"], ascending=[False], inplace=True)

                if start_time and end_time:
                    df_merged = df_merged[
                        (df_merged["statistic_time"] >= start_time) & (df_merged["statistic_time"] <= end_time)]

                all_data = {}
                # 准备算钱
                all_data["buy_order_count"] = int(df_merged["buy_order_count"].sum())
                all_data["buy_lh_count"] = int(df_merged["buy_lh_count"].sum())
                all_data["buy_total_price"] = round(df_merged["buy_total_price"].sum(), 2)
                all_data["public_lh_count"] = int(df_merged["public_lh_count"].sum())
                all_data["public_order_count"] = int(df_merged["public_order_count"].sum())
                all_data["public_total_price"] = round(df_merged["public_total_price"].sum(), 2)
                all_data["sell_lh_count"] = int(df_merged["sell_lh_count"].sum())
                all_data["sell_order_count"] = int(df_merged["sell_order_count"].sum())
                all_data["sell_total_price"] = round(df_merged["sell_total_price"].sum(), 2)
                all_data["total_real_money"] = round(df_merged["total_real_money"].sum(), 2)
                all_data["total_sell_fee"] = round(df_merged["total_sell_fee"].sum(), 2)
                logger.info(all_data)

                df_merged.fillna("", inplace=True)
                count = len(df_merged)
                return_data = df_merged[code_page:code_size] if page and size else df_merged.copy()

                msg_data = {"all_data": all_data, "data": return_data.to_dict("records")}
                return {"code": "0000", "status": "success", "msg": msg_data, "count": count}
        else:
            all_data = {}
            # 准备算钱
            all_data["buy_order_count"] = 0
            all_data["buy_lh_count"] = 0
            all_data["buy_total_price"] = 0
            all_data["public_lh_count"] = 0
            all_data["public_order_count"] = 0
            all_data["public_total_price"] = 0
            all_data["sell_lh_count"] = 0
            all_data["sell_order_count"] = 0
            all_data["sell_total_price"] = 0
            all_data["total_real_money"] = 0
            all_data["total_sell_fee"] = 0
            msg_data = {"all_data": all_data, "data": []}
            return {"code": "0000", "status": "success", "msg": msg_data, "count": 0}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()

# 用户每日订单数据统计报表
@dailybp.route("user",methods=["POST"])
def daily_user_summary():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 8:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            # 表单选择operateid
            operateid = request.json['operateid']
            # 出售人信息
            search_key = request.json['keyword'].strip()
            # 归属上级
            parent = request.json['parent'].strip()
            # 时间
            start_time = request.json['start_time']
            end_time = request.json['end_time']

            page = request.json['page']
            size = request.json['size']

            # 时间判断
            # if start_time or end_time:
            #     # 判断时间是否大于2021-11-28
            #     if start_time < '2021-11-28':
            #         return {"code": "0000", "status": "success", "msg": []}
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        base_sql = '''select day_time,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname, phone, unionid, operate_id, operatename, parentid, parent_phone, buy_count, buy_pretty_count,
                    buy_total_price, publish_pretty_count, publish_count, publish_total_price, sell_count, sell_total_price, sell_pretty_count, truth_price, sell_fee
                    from lh_analyze.user_daily_order_data'''

        # 数据库连接
        conn_an = direct_get_conn(analyze_mysql_conf)
        if not conn_an:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        all_data = pd.read_sql(base_sql, conn_an)

        # 匹配数据
        all_data['unionid'].fillna('', inplace=True)
        all_data['unionid'] = all_data['unionid'].astype(str)
        logger.info(all_data.shape)

        all_data['day_time'] = pd.to_datetime(all_data['day_time'])
        all_data['day_time'] = all_data['day_time'].dt.strftime('%Y-%m-%d')
        if search_key or parent or operateid or (start_time and end_time):
            match_data = all_data.loc[(all_data['unionid'].str.contains(search_key)) | (
                all_data['nickname'].str.contains(search_key)) | (all_data['phone'].str.contains(search_key)), :]
            logger.info(match_data.shape)
            if parent:
                match_data['parentid'] = match_data['parentid'].astype(str)
                match_data = match_data.loc[(match_data['parentid'] == parent) | (match_data['parent_phone'] == parent), :]
            if operateid:
                match_data = match_data.loc[match_data['operate_id'].notna(), :]
                match_data['operate_id'] = match_data['operate_id'].astype(int)
                match_data = match_data.loc[match_data['operate_id'] == operateid, :]
            if start_time and end_time:
                match_data = match_data.loc[(match_data['day_time'] >= start_time) & (match_data['day_time'] <= end_time), :]
        else:
            match_data = all_data.copy()
            # day_time = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
            # match_data = all_data.loc[all_data['day_time'] == day_time, :]

        match_data.sort_values("day_time",ascending=False,inplace=True)
        # 汇总数据
        tital_data = {}
        tital_data['buy_count'] = int(match_data['buy_count'].sum())
        tital_data['buy_pretty_count'] = int(match_data['buy_pretty_count'].sum())
        tital_data['buy_total_price'] = round(float(match_data['buy_total_price'].sum()), 2)
        tital_data['publish_count'] = int(match_data['publish_count'].sum())
        tital_data['publish_pretty_count'] = int(match_data['publish_pretty_count'].sum())
        tital_data['publish_total_price'] = round(float(match_data['publish_total_price'].sum()), 2)
        tital_data['sell_count'] = int(match_data['sell_count'].sum())
        tital_data['sell_pretty_count'] = int(match_data['sell_pretty_count'].sum())
        tital_data['sell_total_price'] = round(float(match_data['sell_total_price'].sum()), 2)
        tital_data['truth_price'] = round(float(match_data['truth_price'].sum()), 2)
        tital_data['sell_fee'] = round(float(match_data['sell_fee'].sum()), 2)

        # 根据采购金额倒叙排序
        match_data.sort_values(['day_time', 'buy_total_price'], ascending=False, inplace=True)
        match_data.drop(['parent_phone', 'operate_id'], axis=1, inplace=True)
        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
            cut_data = match_data[start_index:end_index]
        else:
            cut_data = match_data.copy()
        cut_data.fillna("", inplace=True)
        cut_data['unionid'] = cut_data['unionid'].apply(lambda x: del_point(x))
        return_data = {
            'tital_data': tital_data,
            'data': cut_data.to_dict('records')
        }
        return {"code": "0000", "status": "success", "msg": return_data, "count": len(match_data)}
    except:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_an.close()
        except:
            pass


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
        bus_id = request.json.get("bus_id")

        code_page = ""
        code_size = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        sql = '''
        select day_time,operate_id,operatename,leader,leader_phone,leader_unionid ,sum(buy_count) buy_count,sum(buy_pretty_count) buy_pretty_count,sum(buy_total_price) buy_total_price,sum(publish_count) publish_count,sum(publish_pretty_count) publish_pretty_count,sum(publish_total_price) publish_total_price,sum(sell_count) sell_count,sum(sell_pretty_count) sell_pretty_count,sum(sell_total_price) sell_total_price,sum(truth_price) truth_price,sum(sell_fee)sell_fee from user_daily_order_data where operatename != "" group by day_time,operatename order by day_time desc,buy_total_price desc
        '''
        order_data = pd.read_sql(sql,conn_analyze)
        order_data["day_time"] = order_data["day_time"].apply(lambda x: x.strftime('%Y-%m-%d'))
        order_data["leader_unionid"] = order_data["leader_unionid"].astype(str)

        if start_time and end_time:
            order_data = order_data[(order_data["day_time"] >= start_time) & (order_data["day_time"] <= end_time)]

        if bus_id:
            logger.info("bus_id:%s" %bus_id)
            order_data = order_data[order_data["operate_id"] == bus_id]

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


'''平台每日名片网估值'''
@dailybp.route("plat/value",methods=["POST"])
def daily_plat_value():
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

        page = request.json.get("page")
        size = request.json.get("size")

        unioinid_lists = [x.strip() for x in request.json["unioinid_lists"]]
        phone_lists = [x.strip() for x in request.json["phone_lists"]]
        bus_lists = [x.strip() for x in request.json["bus_lists"]]

        tag_id = request.json.get("tag_id")

        start_time = request.json.get("start_time")
        end_time = request.json.get("end_time")

        if start_time and end_time:
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d")
            start_time = datetime.date(start_time.year, start_time.month, start_time.day)
            end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d")
            end_time = datetime.date(end_time.year, end_time.month, end_time.day)
        args_list = []
        # 过滤手机号
        if phone_lists:
            args_list = phone_lists.copy()
            logger.info(args_list)
        # 过滤用户id
        if unioinid_lists:
            # 走统计表
            try:
                sql = '''select phone from crm_user where find_in_set (unionid,%s) and phone != "" and phone is not null'''
                ags_list = ",".join(unioinid_lists)
                logger.info(ags_list)
                cursor_analyze.execute(sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for p in phone_lists:
                    args_list.append(p[0])
                # args_list = ",".join(args_list)
            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}

        # 过滤运营中心的
        if bus_lists:
            str_bus_lists = ",".join(bus_lists)
            sql = '''select phone from crm_user where find_in_set (operate_id,%s)  and del_flag = 0'''
            cursor_analyze.execute(sql, str_bus_lists)
            phone_lists = cursor_analyze.fetchall()
            logger.info(phone_lists)
            args_list = [p[0] for p in phone_lists]
            # args_list = ",".join(args_list)

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
                if tp in args_list:
                    continue
                else:
                    select_phone.append(tp)
        else:
            lh_user_sql = '''select distinct(phone) phone from lh_user where del_flag = 0 and phone != "" and phone is not null'''
            lh_user_phone = pd.read_sql(lh_user_sql, conn_lh)
            lh_phone = lh_user_phone["phone"].to_list()

            select_phone = list(set(lh_phone) - set(args_list))

        flag = 0
        if len(lh_phone) == len(select_phone):
            flag = 1
        else:
            flag = 0

        logger.info(flag)

        select_phone = ",".join(select_phone)

        code_page = ""
        code_size = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        sql = '''select day_time,sum(no_tran_price) no_tran_price,sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price,sum(public_count) public_count,sum(public_price) public_price,sum(use_total_price) use_total_price,sum(use_count) use_count,sum(hold_price) hold_price,sum(hold_count) hold_count,sum(tran_price) tran_price,sum(tran_count) tran_count from user_storage_value'''
        group_sql = '''  group by day_time order by day_time desc'''
        if select_phone:
            if not flag:
                condition_sql = " where hold_phone in (%s)" % select_phone
                sql = sql + condition_sql +group_sql

                logger.info(sql)
                data = pd.read_sql(sql,conn_analyze)
                # logger.info(data)


                if start_time and end_time:
                    data = data[(data["day_time"] >= start_time) & (data["day_time"] <= end_time)]

                data = data.to_dict("records")
                for d in data:
                    d["day_time"] = datetime.datetime.strftime(d["day_time"], "%Y-%m-%d")
                count = len(data)

                return_data = data[code_page:code_size] if page and size else data.copy()
                msg = {"data": return_data}

                return {"code":"0000","status":"success","msg":msg,"count":count}
            else:
                sql = sql + group_sql

                logger.info(sql)
                data = pd.read_sql(sql, conn_analyze)
                # logger.info(data)

                if start_time and end_time:
                    data = data[(data["day_time"] >= start_time) & (data["day_time"] <= end_time)]

                data = data.to_dict("records")
                for d in data:
                    d["day_time"] = datetime.datetime.strftime(d["day_time"], "%Y-%m-%d")
                count = len(data)

                return_data = data[code_page:code_size] if page and size else data.copy()
                msg = {"data": return_data}

                return {"code": "0000", "status": "success", "msg": msg, "count": count}
        else:
            msg = {"data": []}

            return {"code": "0000", "status": "success", "msg": msg, "count": 0}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()


@dailybp.route("operate/value",methods=["POST"])
def daily_operate_value():
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
        bus_id = request.json.get("bus_id")

        code_page = ""
        code_size = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        sql = '''
        select day_time,operate_id,operatename,leader,leader_phone,leader_unionid ,sum(no_tran_price) no_tran_price,sum(no_tran_count) no_tran_count,sum(transferred_count) transferred_count,sum(transferred_price) transferred_price,sum(public_count) public_count,sum(public_price) public_price,sum(use_total_price) use_total_price,sum(use_count) use_count,sum(hold_price) hold_price,sum(hold_count) hold_count,sum(tran_price) tran_price,sum(tran_count) tran_count from user_storage_value where operatename != "" group by day_time,operatename order by day_time desc,hold_count desc'''


        order_data = pd.read_sql(sql,conn_analyze)
        order_data["day_time"] = order_data["day_time"].apply(lambda x: x.strftime('%Y-%m-%d'))
        order_data["leader_unionid"] = order_data["leader_unionid"].astype(str)

        if start_time and end_time:
            order_data = order_data[(order_data["day_time"] >= start_time) & (order_data["day_time"] <= end_time)]

        if bus_id:
            logger.info("bus_id:%s" %bus_id)
            order_data = order_data[order_data["operate_id"] == bus_id]

        #筛选出关键词
        order_data = order_data[(order_data["leader_phone"].str.contains(keyword))|(order_data["leader"].str.contains(keyword))|(order_data["leader_unionid"].str.contains(keyword))]


        count = len(order_data)
        return_data = order_data[code_page:code_size] if page and size else order_data.copy()
        return_data = return_data.to_dict("records")
        msg_data = {"data":return_data}
        return {"code": "0000", "status": "success", "msg": msg_data, "count": count}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()




@dailybp.route("user/value",methods=["POST"])
def daily_ser_value():
    try:
        conn_an = direct_get_conn(analyze_mysql_conf)
        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        # 表单选择operateid
        operateid = request.json['operateid']
        # 出售人信息
        search_key = request.json['keyword'].strip()
        # 归属上级
        parent = request.json['parent'].strip()
        # 时间
        start_time = request.json['start_time']
        end_time = request.json['end_time']

        page = request.json['page']
        size = request.json['size']

        condition = []
        if search_key:
            condition.append(''' (nickname like "%%%s%%" or hold_phone like "%%%s%%" or unionid like "%%%s%%")''' %(search_key,search_key,search_key))
        if operateid:
            condition.append(''' operate_id = %s''' %operateid)
        if parent:
            condition.append(''' (parentid = %s or parent_phone = %s)''' %(parent,parent))
        if start_time and end_time:
            condition.append(''' day_time>="%s" and day_time <= "%s"''' %(start_time,end_time))

        sql = '''select day_time, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname, hold_phone, unionid, operate_id, operatename, parentid, parent_phone, no_tran_price, no_tran_count,
                    transferred_count, transferred_price, public_count, public_price, use_total_price, 
                    use_count, hold_price, hold_count, tran_price,tran_count
                    from lh_analyze.user_storage_value'''
        count_sql = '''
        select count(*) count 
                    from lh_analyze.user_storage_value
        '''


        for i in range(0,len(condition)):
            if i == 0:
                sql = sql + " where "+condition[i]
                count_sql = count_sql + " where "+condition[i]
            else:
                sql = sql + " and " + condition[i]
                count_sql = count_sql + " and " + condition[i]

        count = int(pd.read_sql(count_sql,conn_an)["count"][0])



        logger.info(count_sql)

        order_sql = ''' order by day_time desc,hold_count desc '''
        limit_sql = ''' limit %s,%s''' %((page-1)*size,size)
        sql = sql + order_sql + limit_sql
        logger.info(sql)
        value_data = pd.read_sql(sql,conn_an)

        # value_data.sort_values(['day_time', 'hold_count'], ascending=False, inplace=True)
        value_data.fillna("",inplace=True)
        value_data = value_data.to_dict("records")

        # if page and size:
        #     code_page = (page - 1) * size
        #     code_size = page * size

        # return_data = value_data[code_page:code_size] if page and size else value_data.copy()
        logger.info("count:%s" %count)
        logger.info(type(value_data))
        for r in value_data:
            r["day_time"] = datetime.datetime.strftime(r["day_time"], "%Y-%m-%d")
        logger.info(value_data)
        msg_data = {"data":value_data}
        return {"code":"0000","status":"success","msg":msg_data,"count":count}


    except Exception as e:
        logger.error(traceback.format_exc())

        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_an.close()
        except:
            pass


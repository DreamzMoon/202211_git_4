# -*- coding: utf-8 -*-

# @Time : 2021/12/23 16:47

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : person_market.py


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

personpfbp = Blueprint('personpfbp', __name__, url_prefix='/le/pifa/person')


# 个人转卖市场订单数据统计分析
@personpfbp.route("total",methods=["POST"])
def personal_total():
    try:
        conn_read = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
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
        tag_id = request.json['tag_id']

        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_id = ""
        bus_phone = []
        tag_phone = []

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
                    parent_id = str(result[1])
                else:
                    return {"code": "11014", "status": "failed", "msg": message["code"]}
            else:
                parent_id = parent

        if bus_id:
            sql = '''select phone from crm_user where operate_id = %s''' %bus_id
            phone_data = pd.read_sql(sql, conn_analyze)
            bus_phone = phone_data["phone"].tolist()
            if not bus_phone:
                return {"code": "11015", "status": "failed", "msg": message["11015"]}
            # result = get_busphne_by_id(bus_id)
            # if result[0] == 1:
            #     bus_phone = result[1].split(",")
            # else:
            #     return {"code":"11015","status":"failed","msg":message["11015"]}

        if tag_id:
            result = find_tag_user_phone(tag_id)
            if not result:
                return {"code": "10000", "status": "failed", "msg": message["10000"]}
            tag_phone = result[1]

        logger.info(len(bus_phone))
        bus_phone.extend(tag_phone)
        bus_phone = list(set(bus_phone))

        logger.info(bus_phone)

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
        order_sql = '''select phone,count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1'''
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
        sell_sql = '''select sell_phone phone,count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee from le_order where `status` = 1 and  del_flag = 0 and type = 1'''
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
        public_sql = '''select sell_phone phone,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from le_sell where del_flag=0 and status != 1'''
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
        # conn_analyze = direct_get_conn(analyze_mysql_conf)
        sql = '''select unionid,parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user where phone != "" and phone is not null and del_flag=0'''
        logger.info(sql)
        crm_data = pd.read_sql(sql, conn_analyze)
        # conn_analyze.close()
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
        df_merged.sort_values(by=["buy_total_price"], ascending=False, inplace=True)
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

        # 查找用户标签
        tag_phone_list = need_data['phone'].tolist()
        if tag_phone_list:
            tag_sql = '''
                select t1.unionid, group_concat(t2.tag_name) tag_name from
                (select unionid, tag_id from lh_analyze.crm_user_tag where unionid in (
                    select unionid from lh_analyze.crm_user where phone in (%s)
                )) t1
                left join
                crm_tag t2
                on t1.tag_id = t2.id
                group by t1.unionid
            ''' % ','.join(tag_phone_list)
            tag_df = pd.read_sql(tag_sql, conn_analyze)
            tag_df['unionid'] = tag_df['unionid'].astype(str)
            need_data = need_data.merge(tag_df, how='left', on='unionid')
        else:
            need_data['tag_name'] = []

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
        conn_analyze.close()



'''个人转卖市场采购数据分析总'''
@personpfbp.route("buy/all",methods=["POST"])
def personal_buy_all():
    try:
        conn_read = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
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

        bus_id = request.json["bus_id"]
        # 必须传年月日时分秒
        first_start_time = request.json["first_start_time"]
        first_end_time = request.json["first_end_time"]
        last_start_time = request.json["last_start_time"]
        last_end_time = request.json["last_end_time"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_id = ""
        bus_phone = []

        time_condition_sql = ""

        if first_start_time and first_end_time:
            if first_start_time >= first_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
        if last_start_time and last_end_time:
            if last_start_time >= last_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}


        time_condition_sql = ""
        if first_start_time and first_end_time and last_start_time and last_end_time:

            # 11.2 11.5 10.31-11.1 no
            if last_end_time > first_start_time:
                pass
            else:
                return {"code":"11019","status":"failed","msg":message[["11019"]]}


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
                    parent_id = str(result[1])
                else:
                    return {"code": "11014", "status": "failed", "msg": message["code"]}
            else:
                parent_id = parent


        if bus_id:
            sql = '''select phone from crm_user where operate_id = %s''' %bus_id
            phone_data = pd.read_sql(sql,conn_analyze)
            bus_phone = phone_data['phone'].tolist()
            if not bus_phone:
                return {"code": "11015", "status": "failed", "msg": message["11015"]}

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

        buy_sql = '''select phone,total_price,create_time from le_order where `status` = 1 and  del_flag = 0 and type = 1'''
        if time_condition_sql:
            buy_sql = buy_sql+time_condition_sql

        group_sql = ''' group by phone'''
        if query_phone:
            condition_sql = ''' and phone in (%s)''' % (",".join(query_phone))
            order_sql = buy_sql+condition_sql
        else:
            order_sql = buy_sql

        #返回条数
        logger.info("order_sql:%s" %order_sql)
        order_data = pd.read_sql(order_sql,conn_read)
        order_data_group = order_data.groupby("phone")

        #排序取出按时间第一条和最后一条的
        first_data = order_data.sort_values("create_time", ascending=True).groupby("phone").first().reset_index()
        first_data.rename(columns={"phone":"phone","create_time":"first_time","total_price":"first_total_price"},inplace=True)
        first_data["first_time"] = first_data['first_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

        last_data = order_data.sort_values("create_time", ascending=True).groupby("phone").last().reset_index()
        last_data.rename(columns={"phone": "phone", "create_time": "last_time", "total_price": "last_total_price"},inplace=True)
        last_data["last_time"] = last_data['last_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

        sum_data = order_data.sort_values("create_time", ascending=True).groupby("phone").sum("total_price").reset_index()
        logger.info(sum_data)
        count_data = order_data.sort_values("create_time", ascending=True).groupby("phone").count().reset_index().drop("create_time",axis=1)
        count_data.rename(columns={"phone":"phone","total_price":"count"},inplace=True)

        df_list = []
        df_list.append(first_data)
        df_list.append(last_data)
        df_list.append(sum_data)
        df_list.append(count_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        if first_start_time and first_end_time:
            df_merged = df_merged[(df_merged["first_time"] >= first_start_time) & (df_merged["first_time"] <= first_end_time)]
            logger.info(df_merged.shape)
        if last_start_time and last_end_time:
            df_merged = df_merged[(df_merged["last_time"] >= last_start_time) & (df_merged["last_time"] <= last_end_time)]

        # 无数据返回空
        if df_merged.empty:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        df_merged_count = len(df_merged)
        logger.info(df_merged.shape)


        sql = '''select unionid,parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user where phone != "" and phone is not null and del_flag=0'''
        crm_data = pd.read_sql(sql, conn_analyze)

        df_merged = df_merged.merge(crm_data, how="left", on="phone")


        # 转类型
        df_merged["parentid"] = df_merged['parentid'].astype(str)
        df_merged["unionid"] = df_merged['unionid'].astype(str)
        df_merged['parentid'] = df_merged['parentid'].apply(lambda x: del_point(x))
        df_merged['unionid'] = df_merged['unionid'].apply(lambda x: del_point(x))
        if parent_id:
            df_merged = df_merged[df_merged["parentid"] == parent_id]
        df_merged.sort_values('last_time', ascending=False, inplace=True)
        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()

        need_data.fillna("", inplace=True)

        last_data = need_data.to_dict("records")

        last_data[0]["total_price"] = round(last_data[0]["total_price"],2)

        logger.info("last_data:%s" %last_data)
        return {"code": "0000", "status": "success", "msg": last_data, "count": df_merged_count}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()
        conn_analyze.close()



'''个人转卖市场采购数据分析--个人'''
@personpfbp.route("buy",methods=["POST"])
def person_buy():
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


        phone = request.json["phone"]

        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = int(request.json["time_type"])
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        if time_type == 4:
            if not start_time or not end_time:
                return {"code":"10001","status":"failed","msg":message["10001"]}
            if start_time >= end_time:
                return {"code":"11020","status":"failed","msg":message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")
            datetime_end_time = datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")
            daysss = datetime_end_time-datetime_start_time
            if daysss.days+ daysss.seconds/(24.0*60.0*60.0) > 30:
                return {"code":"11018","status":"failed","msg":message["11018"]}



        if not phone:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        cursor = conn_read.cursor()
        sql = '''select o.create_time,o.total_price,o.pay_type,s.pretty_type_name,o.count from le_order o 
        left join lh_sell s on o.sell_id = s.id
        where o.phone = %s and o.del_flag = 0 and o.type = 1  and o.`status` = 1
        order by create_time asc'''
        cursor.execute(sql,(phone))
        datas = cursor.fetchall()

        # logger.info(datas)

        first_data={"order_time":"","order_total_price":"","order_pay":"","order_type":"","order_count":""}
        second_data={"order_time":"","order_total_price":"","order_pay":"","order_type":"","order_count":""}
        last_data={"order_time":"","order_total_price":"","order_pay":"","order_type":"","order_count":""}

        personal_datas = {"first":first_data,"second":second_data,"last":last_data,"person":{}}
        try:
            first_data["order_time"] = datetime.datetime.strftime(datas[0][0], "%Y-%m-%d %H:%M:%S")
            first_data["order_total_price"] = datas[0][1]
            first_data["order_pay"] = datas[0][2]
            first_data["order_type"] = datas[0][3]
            first_data["order_count"] = datas[0][4]
        except:
            pass

        try:
            second_data["order_time"] = datetime.datetime.strftime(datas[1][0], "%Y-%m-%d %H:%M:%S")
            second_data["order_total_price"] = datas[1][1]
            second_data["order_pay"] = datas[1][2]
            second_data["order_type"] = datas[1][3]
            second_data["order_count"] = datas[1][4]
        except:
            pass

        try:
            if len(datas)>2:
                last_data["order_time"] = datetime.datetime.strftime(datas[-1][0], "%Y-%m-%d %H:%M:%S")
                last_data["order_total_price"] = datas[-1][1]
                last_data["order_pay"] = datas[-1][2]
                last_data["order_type"] = datas[-1][3]
                last_data["order_count"] = datas[-1][4]
        except:
            pass


        #通过手机号码直接查运营中心字段 并返回 nickname operate_name parent_phone parentid phone unionid
        crm_sql = '''select unionid,phone,parentid,parent_phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user where phone = %s and del_flag=0''' % phone
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        user_data = pd.read_sql(crm_sql,conn_analyze)
        user_data = user_data.to_dict("records")
        if not user_data:
            # return {"code": "0000", "status": "success", "msg": "暂无该用户数据"}
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        logger.info(user_data)
        personal_datas["person"] = user_data[0]


        #获取所有的数据

        all_sql = '''select count(*) order_count,sum(count) total_count,sum(total_price) total_price,GROUP_CONCAT(pay_type) sum_pay_type from le_order where del_flag = 0 and type = 1 and `status`=1 group by phone having phone = %s'''
        cursor.execute(all_sql,(phone))
        datas = cursor.fetchone()
        user_order_data = {"order_total_price":datas[2],"order_count":datas[0],"total_count":datas[1],"pay_type":datas[3]}
        order_data = {"user_order_data":user_order_data}

        # 今天
        last_data = {}

        if time_type == 1 or (time_type == 4 and daysss and daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0)<1):
            #今日

            if time_type == 1:
                circle_sql1 = '''select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''
            else:
                query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d")
                yesterday_query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d")
                circle_sql1 = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(query_time)
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(yesterday_query_time)

            # #直接拼接sql 不然会有很多重复的代码 很烦人
            circle_sql = ""

            condition_sql = " and phone = %s" %phone
            circle_sql = circle_sql1 +condition_sql + circle_conn + circle_sql2 + condition_sql

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today":circle_data[0][0],"today_buy_total_price":circle_data[0][1],"today_buy_order_count":circle_data[0][2],
                "yesterday":circle_data[1][0],"yes_buy_total_price":circle_data[1][1],"yes_buy_order_count":circle_data[1][2]
            }

            if time_type == 1:
                today_sql = '''select DATE_FORMAT(create_time, '%Y-%m-%d %H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
            else:
                today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d %%H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %query_time
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            today_sql = today_sql + condition_sql + group_order_sql

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
            order_data["circle"] = circle
            order_data["today"] = today

        elif time_type == 2 or time_type == 3:
            query_range = []
            if time_type == 2:
                query_range = ["-0","-6","-7","-13"]
            elif time_type == 3:
                query_range = ["-0","-29","-30","-59"]
            circle_sql = '''
            select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc) a
            union all
            select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)   group by statistic_time order by statistic_time asc) b ''' %(phone,query_range[0],query_range[1],phone,query_range[2],query_range[3])

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

            # 查询近七天的
            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc ''' %(phone,query_range[0],query_range[1])
            logger.info(today_sql)
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

            order_data["circle"] = circle
            order_data["today"] = today
        elif time_type == 4:
            sub_day = int(daysss.days+1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(start_time)
            logger.info(end_time)
            logger.info(before_start_time)
            logger.info(before_end_time)
            circle_sql = '''
                        select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) a
                        union all
                        select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) b ''' %(phone,end_time,start_time,phone,before_end_time,before_start_time)

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


            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and phone = %s and create_time <= "%s" and create_time >= "%s" group by statistic_time order by statistic_time asc ''' %(phone,end_time,start_time)

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

            order_data["circle"] = circle
            order_data["today"] = today
        else:
            return {"code":"11009","status":"failed","msg":message["11009"]}

        datas = {"person":personal_datas,"order_data":order_data}
        return {"code":"0000","status":"success","msg":datas}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()



'''个人转卖市场出售数据分析总'''
@personpfbp.route("sell/all", methods=["POST"])
def personal_sell_all():
    try:

        conn_read = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)

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

        bus_id = request.json["bus_id"]
        # 必须传年月日时分秒
        first_start_time = request.json["first_start_time"]
        first_end_time = request.json["first_end_time"]
        last_start_time = request.json["last_start_time"]
        last_end_time = request.json["last_end_time"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_id = ""
        bus_phone = []

        time_condition_sql = ""

        if first_start_time and first_end_time:
            if first_start_time >= first_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
        if last_start_time and last_end_time:
            if last_start_time >= last_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}

        if first_start_time and first_end_time and last_start_time and last_end_time:

            # 11.2 11.5 10.31-11.1 no
            if last_end_time > first_start_time:
                pass
            else:
                return {"code": "11019", "status": "failed", "msg": message[["11019"]]}

        # 模糊查询
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
            else:
                # return {"code": "11016", "status": "failed", "msg": message["11016"]}
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        # 只查一个
        if parent:
            if len(parent) == 11:
                result = get_parent_by_phone(parent)
                if result[0] == 1:
                    parent_id = str(result[1])
                else:
                    return {"code": "11014", "status": "failed", "msg": message[11014]}
            else:
                parent_id = parent

        if bus_id:
            bus_sql = '''select phone from crm_user where operate_id=%s''' % bus_id
            phone_data = pd.read_sql(bus_sql, conn_analyze)
            bus_phone = phone_data['phone'].tolist()
            if not bus_phone:
                return {"code": "11015", "status": "failed", "msg": message["11015"]}


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

        buy_sql = '''select sell_phone phone,total_price,create_time from le_order where `status` = 1 and  del_flag = 0 and type = 1'''
        if time_condition_sql:
            buy_sql = buy_sql + time_condition_sql

        group_sql = ''' group by sell_phone'''
        if query_phone:
            condition_sql = ''' and sell_phone in (%s)''' % (",".join(query_phone))
            order_sql = buy_sql + condition_sql
        else:
            order_sql = buy_sql

        # 返回条数

        logger.info("order_sql:%s" % order_sql)
        order_data = pd.read_sql(order_sql, conn_read)
        order_data_group = order_data.groupby("phone")

        # 排序取出按时间第一条和最后一条的
        first_data = order_data.sort_values("create_time", ascending=True).groupby("phone").first().reset_index()
        first_data.rename(columns={"phone": "phone", "create_time": "first_time", "total_price": "first_total_price"},
                          inplace=True)
        first_data["first_time"] = first_data['first_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

        last_data = order_data.sort_values("create_time", ascending=True).groupby("phone").last().reset_index()
        last_data.rename(columns={"phone": "phone", "create_time": "last_time", "total_price": "last_total_price"},inplace=True)
        # last_data["last_time"] = last_data['last_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        last_data["last_time"] = last_data['last_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

        sum_data = order_data.sort_values("create_time", ascending=True).groupby("phone").sum("total_price").reset_index()
        count_data = order_data.sort_values("create_time", ascending=True).groupby("phone").count().reset_index().drop("create_time", axis=1)
        count_data.rename(columns={"phone": "phone", "total_price": "count"}, inplace=True)

        df_list = []
        df_list.append(first_data)
        df_list.append(last_data)
        df_list.append(sum_data)
        df_list.append(count_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        if first_start_time and first_end_time:
            df_merged = df_merged[(df_merged["first_time"] >= first_start_time) & (df_merged["first_time"] <= first_end_time)]
            logger.info(df_merged.shape)
        if last_start_time and last_end_time:
            df_merged = df_merged[(df_merged["last_time"] >= last_start_time) & (df_merged["last_time"] <= last_end_time)]

        # 无数据返回空
        if df_merged.empty:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        result_count = len(df_merged)

        # conn_analyze = direct_get_conn(analyze_mysql_conf)
        sql = '''select unionid,parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user where phone != "" and phone is not null and del_flag=0'''
        crm_data = pd.read_sql(sql, conn_analyze)
        # conn_analyze.close()

        df_merged = df_merged.merge(crm_data,how="left",on="phone")

        # 转类型
        df_merged["parentid"] = df_merged['parentid'].astype(str)
        df_merged["unionid"] = df_merged['unionid'].astype(str)
        df_merged['parentid'] = df_merged['parentid'].apply(lambda x: del_point(x))
        df_merged['unionid'] = df_merged['unionid'].apply(lambda x: del_point(x))
        if parent_id:
            df_merged = df_merged[df_merged["parentid"] == parent_id]
        # 按时间倒序
        df_merged.sort_values('last_time', ascending=False, inplace=True)
        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()
        need_data.fillna("", inplace=True)
        last_data = need_data.to_dict("records")
        last_data[0]["total_price"] = round(last_data[0]["total_price"], 2)
        return {"code": "0000", "status": "success", "msg": last_data, "count": result_count}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_read.close()
            conn_analyze.close()
        except:
            pass



'''个人转卖市场出售数据分析--个人'''
@personpfbp.route("sell", methods=["POST"])
def person_sell():
    try:
        logger.info(request.json)
        conn_read = direct_get_conn(lianghao_mysql_conf)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        sell_phone = request.json["sell_phone"]

        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = int(request.json["time_type"])
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        # sub_time = ""
        daysss = ""
        if time_type == 4:
            if not start_time or not end_time:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}
            if start_time >= end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            datetime_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            daysss = datetime_end_time - datetime_start_time
            if daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0) > 30:
                return {"code": "11018", "status": "failed", "msg": message["11018"]}

            # 获取两个起始时间相减判断是否一天
            # sub_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

        if not sell_phone:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        cursor = conn_read.cursor()
        sql = '''select o.create_time,o.total_price,o.pay_type,s.pretty_type_name,o.count from le_order o 
        left join lh_sell s on o.sell_id = s.id
        where o.sell_phone = %s and o.del_flag = 0 and o.type = 1  and o.`status` = 1
        order by create_time asc'''
        cursor.execute(sql, (sell_phone))
        datas = cursor.fetchall()


        first_data = {"order_time": "", "order_total_price": "", "order_pay": "", "order_type": "", "order_count": ""}
        second_data = {"order_time": "", "order_total_price": "", "order_pay": "", "order_type": "", "order_count": ""}
        last_data = {"order_time": "", "order_total_price": "", "order_pay": "", "order_type": "", "order_count": ""}

        personal_datas = {"first": first_data, "second": second_data, "last": last_data, "person": {}}
        try:
            first_data["order_time"] = datetime.datetime.strftime(datas[0][0], "%Y-%m-%d %H:%M:%S")
            first_data["order_total_price"] = datas[0][1]
            first_data["order_pay"] = datas[0][2]
            first_data["order_type"] = datas[0][3]
            first_data["order_count"] = datas[0][4]
        except:
            pass

        try:
            second_data["order_time"] = datetime.datetime.strftime(datas[1][0], "%Y-%m-%d %H:%M:%S")
            second_data["order_total_price"] = datas[1][1]
            second_data["order_pay"] = datas[1][2]
            second_data["order_type"] = datas[1][3]
            second_data["order_count"] = datas[1][4]
        except:
            pass

        try:
            if len(datas) > 2:
                last_data["order_time"] = datetime.datetime.strftime(datas[-1][0], "%Y-%m-%d %H:%M:%S")
                last_data["order_total_price"] = datas[-1][1]
                last_data["order_pay"] = datas[-1][2]
                last_data["order_type"] = datas[-1][3]
                last_data["order_count"] = datas[-1][4]
        except:
            pass

        crm_sql = '''select unionid,phone,parentid,parent_phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user where phone = %s and del_flag=0''' % sell_phone
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        user_data = pd.read_sql(crm_sql, conn_analyze)
        user_data = user_data.to_dict("records")
        if not user_data:
            # return {"code": "0000", "status": "success", "msg": "暂无该用户数据"}
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        personal_datas["person"] = user_data[0]
        # 获取所有的数据

        all_sql = '''select count(*) order_count,sum(count) total_count,sum(total_price) total_price,GROUP_CONCAT(pay_type) sum_pay_type from le_order where del_flag = 0 and type = 1 and `status`=1 group by sell_phone having sell_phone = %s'''
        cursor.execute(all_sql, (sell_phone))
        datas = cursor.fetchone()
        user_order_data = {"order_total_price": datas[2], "order_count": datas[0], "total_count": datas[1],
                           "pay_type": datas[3]}
        order_data = {"user_order_data": user_order_data}

        # 今天
        last_data = {}

        if time_type == 1 or (time_type == 4 and daysss and daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0)<1):
            #今日
            logger.info("进来了")
            if time_type == 1:
                circle_sql1 = '''select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''
            else:
                query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d")
                yesterday_query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d")
                circle_sql1 = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(query_time)
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(yesterday_query_time)

            # #直接拼接sql 不然会有很多重复的代码 很烦人
            circle_sql = ""

            condition_sql = " and sell_phone = %s" %sell_phone
            circle_sql = circle_sql1 +condition_sql + circle_conn + circle_sql2 + condition_sql

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today":circle_data[0][0],"today_buy_total_price":circle_data[0][1],"today_buy_order_count":circle_data[0][2],
                "yesterday":circle_data[1][0],"yes_buy_total_price":circle_data[1][1],"yes_buy_order_count":circle_data[1][2]
            }

            if time_type == 1:
                today_sql = '''select DATE_FORMAT(create_time, '%Y-%m-%d %H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
            else:
                today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d %%H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %query_time
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            today_sql = today_sql + condition_sql + group_order_sql

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
            order_data["circle"] = circle
            order_data["today"] = today
        elif time_type == 2 or time_type == 3:
            query_range = []
            if time_type == 2:
                query_range = ["-0", "-6", "-7", "-13"]
            elif time_type == 3:
                query_range = ["-0", "-29", "-30", "-59"]
            circle_sql = '''
            select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and sell_phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc) a
            union all
            select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and sell_phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)   group by statistic_time order by statistic_time asc) b ''' % (
            sell_phone, query_range[0], query_range[1], sell_phone, query_range[2], query_range[3])

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

            # 查询近七天的
            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and sell_phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc ''' % (
            sell_phone, query_range[0], query_range[1])
            logger.info(today_sql)
            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[3])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[1])
                today.append(td_dict)
            logger.info(today)

            order_data["circle"] = circle
            order_data["today"] = today
        elif time_type == 4:
            sub_day = int(daysss.days + 1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(start_time)
            logger.info(end_time)
            logger.info(before_start_time)
            logger.info(before_end_time)
            circle_sql = '''
                        select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and sell_phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) a
                        union all
                        select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from le_order where `status` = 1 and  del_flag = 0 and type = 1 and sell_phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) b ''' % (
            sell_phone, end_time, start_time, sell_phone, before_end_time, before_start_time)

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

            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from le_order where `status` = 1 and  del_flag = 0 and type = 1 and sell_phone = %s and create_time <= "%s" and create_time >= "%s" group by statistic_time order by statistic_time asc ''' % (
            sell_phone, end_time, start_time)

            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[3])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[1])
                today.append(td_dict)
            logger.info(today)

            order_data["circle"] = circle
            order_data["today"] = today
        else:
            return {"code": "11009", "status": "failed", "msg": message["11009"]}

        datas = {"person": personal_datas, "order_data": order_data}
        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()

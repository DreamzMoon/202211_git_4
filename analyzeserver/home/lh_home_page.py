# -*- coding: utf-8 -*-

# @Time : 2021/12/6 9:11

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : lh_home_page.py
import sys

import pandas as pd

sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime
from datetime import timedelta
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
from analyzeserver.common import *
import threading


lhhomebp = Blueprint('lhhomepage', __name__, url_prefix='/lhhome')
r = get_redis()

#个人成交排行
@lhhomebp.route("deal/person",methods=["GET"])
def deal_person():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_crm = direct_get_conn(crm_mysql_conf)
        cursor = conn_crm.cursor()

        logger.info(conn_lh)
        #7位 8位个人
        sql = '''
        select phone,sum(total_money) total_money from (
        select phone,sum(total_price) total_money from lh_order where del_flag = 0 and `status`=1 and type in (1,4) and DATE_FORMAT(create_time,"%Y%m%d") =CURRENT_DATE()  group by phone
        union all 
        select phone,sum(total_price) total_money from lh_order where del_flag = 0 and `status`=1 and type in (1,4) and DATE_FORMAT(create_time,"%Y%m%d") =CURRENT_DATE()  group by phone
        ) t group by phone 
         order by total_money desc limit 3
        '''
        logger.info(sql)
        datas = pd.read_sql(sql,conn_lh)
        datas = datas.to_dict("records")
        for data in datas:
            logger.info(data)
            sql = '''select if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from luke_sincerechat.user where phone = %s'''
            cursor.execute(sql,(data["phone"]))
            user_data = cursor.fetchone()
            logger.info(user_data)
            if user_data["username"]:
                data["username"] = user_data["username"][0]+len(user_data["username"][1:])*"*"
            if data["phone"]:
                data["phone"] = data["phone"][0:4]+len(data["phone"][4:])*"*"

        return {"code":"0000","status":"success","msg":datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
        conn_crm.close()


# 运营中心
@lhhomebp.route("deal/bus",methods=["GET"])
def deal_business():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)


        #先查运营中心的人数

        cursor_lh = conn_lh.cursor()
        cursor_ana = conn_analyze.cursor()

        bus_sql = '''select operatename,contains from operate_relationship_crm where operatename  crm = 1'''

        logger.info(bus_sql)
        cursor_ana.execute(bus_sql)
        operate_datas = cursor_ana.fetchall()

        return_lists = []

        #取出今天的订单表
        # sql = '''select phone,sum(total_price) total_money from lh_order where del_flag = 0 and type in (1,4) and `status`=1 and date_format(create_time,"%Y%m%d") = CURRENT_DATE() group by phone'''
        sql = '''
        select phone,sum(total_money) total_money from(
        select phone,sum(total_price) total_money from lh_order where del_flag = 0 and type in (1,4) and `status`=1 and date_format(create_time,"%Y%m%d") = CURRENT_DATE() group by phone 
        union all 
        select phone,sum(total_price) total_money from le_order where del_flag = 0 and type in (1,4) and `status`=1 and date_format(create_time,"%Y%m%d") = CURRENT_DATE() group by phone ) t group by phone
        '''
        cursor_lh.execute(sql)
        order_datas = pd.DataFrame(cursor_lh.fetchall())
        # 如果暂无交易数据，返回空
        if order_datas.shape[0] > 0:
            for ol in reversed(operate_datas):
                ol_dict = {}
                phone_list = json.loads(ol[1])
                ol_dict["operatename"] = ol[0]
                order_data_phone = order_datas[order_datas[0].isin(phone_list)][1]
                ol_dict["total_money"] = order_data_phone.sum() if len(order_data_phone)>0 else 0
                return_lists.append(ol_dict)

            logger.info(return_lists)
            return_lists = pd.DataFrame(return_lists)
            return_lists.sort_values(by="total_money",ascending=False,inplace=True)
            return_lists['total_money'] = return_lists['total_money'].astype(float)
            logger.info(return_lists.iloc[0:3])
            return_data = return_lists.iloc[0:3].to_dict("records")
        else:
            return_data = return_lists
        return {"code":"0000","status":"success","msg":return_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
        conn_lh.close()


# 交易数据中心累计
@lhhomebp.route("datacenter",methods=["GET"])
def data_center():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        cursor_analyze = conn_analyze.cursor()
        sql = '''select start_time,end_time from sys_activity where id = 1'''
        cursor_analyze.execute(sql)
        time_data = cursor_analyze.fetchone()
        start_time = time_data[0]
        end_time = time_data[1]
        logger.info(start_time)
        logger.info(end_time)
        logger.info(type(start_time))

        sql='''select sum(person_count) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from (
        select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
        select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" group by phone) t1
        union all 
        select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
        select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" group by phone) t2)t''' %(start_time,end_time,start_time,end_time)
        logger.info(sql)
        data = pd.read_sql(sql,conn_lh)
        data = data.to_dict("records")[0]
        data["start_time"] = datetime.datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        data["end_time"] = datetime.datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        logger.info(data)
        return {"code":"0000","status":"success","msg":data}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()

# 交易数据中心 今日
@lhhomebp.route("order/datacenter",methods=["GET"])
def order_data_center():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)


        sql='''select sum(person_count) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from (
select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_order where del_flag = 0 and type in (1,4) and `status` = 1 and DATE_FORMAT(create_time,'%Y-%m-%d') = CURRENT_DATE() group by phone) t1
union all 
select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1,4) and `status` = 1 and DATE_FORMAT(create_time,'%Y-%m-%d') = CURRENT_DATE() group by phone) t2)t'''
        logger.info(sql)
        data = pd.read_sql(sql,conn_lh)
        data = data.to_dict("records")[0]
        logger.info(data)
        return {"code":"0000","status":"success","msg":data}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()

# 今日名片网火爆类型交易排行版
@lhhomebp.route("deal/top",methods=["GET"])
def deal_top():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)


        sql = '''select pretty_type_name,unit_price,sum(count) total_count,sum(total_price) total_price from (
        select s.pretty_type_name,o.unit_price,o.count,o.total_price from lh_order o
        left join lh_sell s on o.sell_id = s.id
        where DATE_FORMAT(o.create_time,"%Y%m%d") = CURRENT_DATE
        and o.del_flag = 0 and o.type in (1,4) and o.`status` = 1
        order by o.create_time desc) t group by pretty_type_name order by total_count desc'''


        data = (pd.read_sql(sql, conn_lh)).to_dict("records")
        return {"code": "0000", "status": "success", "msg": data}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()



# 今日实时交易数据分析
@lhhomebp.route("/today/data", methods=["GET"])
def today_data():
    try:
        # try:
        #     logger.info(request.json)
        #     # 参数个数错误
        #     if len(request.json) != 4:
        #         return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token = request.headers["Token"]
            # user_id = request.json["user_id"]
            #
            # if not user_id and not token:
            #     return {"code": "10001", "status": "failed", "msg": message["10001"]}
            #
            # check_token_result = check_token(token, user_id)
            # if check_token_result["code"] != "0000":
            #     return check_token_result

            # 1今日 2昨日 3自定义-->必须传起始和结束时间
            # time_type = request.json['time_type']
            # # 首次发布时间
            # start_time = request.json['start_time']
            # end_time = request.json['end_time']
            #
            # if (time_type != 3 and start_time and end_time) or time_type not in range(1, 4) or (
            #         time_type == 3 and not start_time and not end_time):
            #     return {"code": "10014", "status": "failed", "msg": message["10014"]}
            # # 时间判断
            # elif start_time or end_time:
            #     strp_start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M")
            #     strp_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M")
            #     if strp_start_time > strp_end_time:
            #         return {"code": "10012", "status": "failed", "msg": message["10012"]}
            #     sub_day = strp_end_time - strp_start_time
            #     if sub_day.days + sub_day.seconds / (60.0 * 60.0) > 24:
            #         return {"code": "10015", "status": "failed", "msg": message["10015"]}
        # except Exception as e:
        #     # 参数名错误
        #     logger.error(e)
        #     return {"code": "10009", "status": "failed", "msg": message["10009"]}
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        order_sql= '''
            select today_time, sum(total_price) total_price, count(*) order_count, count(distinct phone) order_person, sum(count) pretty_count from 
                (select DATE_FORMAT(create_time,'%%Y-%%m-%%d %%H') today_time, total_price, phone, count
                from lh_pretty_client.lh_order
                where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%%Y-%%m-%%d') = %s
                union all
                select DATE_FORMAT(create_time,'%%Y-%%m-%%d %%H') today_time, total_price, phone, count
                from lh_pretty_client.le_order
                where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%%Y-%%m-%%d') = %s
                ) t1
            group by today_time
        '''
        # CURDATE() DATE_SUB(CURDATE(), interval 1 day)

        person_sql = '''
            select today_time, count(distinct phone) person_count from
                (select DATE_FORMAT(create_time, '%%Y-%%m-%%d') today_time, phone from lh_pretty_client.lh_order
                where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%%Y-%%m-%%d') = %s
                union all
                select DATE_FORMAT(create_time, '%%Y-%%m-%%d') today_time, phone from lh_pretty_client.le_order
                where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%%Y-%%m-%%d') = %s
                ) t1
            group by today_time
        '''
        # # 今日
        # today_sql = '''
        #     select today_time, sum(total_price) total_price, count(*) order_count, count(distinct phone) order_person, sum(count) pretty_count from
        #         (select DATE_FORMAT(create_time,'%Y-%m-%d %H') today_time, total_price, phone, count
        #         from lh_pretty_client.lh_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = CURDATE()
        #         union all
        #         select DATE_FORMAT(create_time,'%Y-%m-%d %H') today_time, total_price, phone, count
        #         from lh_pretty_client.le_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = CURDATE()
        #         ) t1
        #     group by today_time
        # '''
        # # 昨日
        # yesterday_sql = '''
        #     select today_time, sum(total_price) total_price, count(*) order_count, count(distinct phone) order_person, sum(count) pretty_count from
        #         (select DATE_FORMAT(create_time,'%Y-%m-%d %H') today_time, total_price, phone, count
        #         from lh_pretty_client.lh_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = DATE_SUB(CURDATE(), interval 1 day)
        #         union all
        #         select DATE_FORMAT(create_time,'%Y-%m-%d %H') today_time, total_price, phone, count
        #         from lh_pretty_client.le_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = DATE_SUB(CURDATE(), interval 1 day)
        #         ) t1
        #     group by today_time
        # '''
        # today_person_sql = '''
        #     select today_time, count(distinct phone) person_count from
        #         (select DATE_FORMAT(create_time, '%Y-%m-%d') today_time, phone from lh_pretty_client.lh_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = CURDATE()
        #         union all
        #         select DATE_FORMAT(create_time, '%Y-%m-%d') today_time, phone from lh_pretty_client.le_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = CURDATE()
        #         ) t1
        #     group by today_time
        # '''
        # yesterday_person_sql = '''
        #     select today_time, count(distinct phone) person_count from
        #         (select DATE_FORMAT(create_time, '%Y-%m-%d') today_time, phone from lh_pretty_client.lh_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = date_sub(CURDATE(), interval 1 day)
        #         union all
        #         select DATE_FORMAT(create_time, '%Y-%m-%d') today_time, phone from lh_pretty_client.le_order
        #         where del_flag =0 and type in (1, 4) and `status` = 1 and (phone is not null or phone != "") and DATE_FORMAT(create_time,'%Y-%m-%d') = date_sub(CURDATE(), interval 1 day)
        #         ) t1
        #     group by today_time
        # '''
        today_sql = order_sql % ('CURDATE()', 'CURDATE()')
        yesterday_sql = order_sql % ('DATE_SUB(CURDATE(), interval 1 day)', 'DATE_SUB(CURDATE(), interval 1 day)')

        today_df = pd.read_sql(today_sql, conn_lh)
        yesterday_df = pd.read_sql(yesterday_sql, conn_lh)
        # 今日交易人数
        today_person_sql = person_sql % ('CURDATE()', 'CURDATE()')
        today_person_count_df = pd.read_sql(today_person_sql, conn_lh)
        if today_person_count_df.empty:
            today_order_person = 0
        else:
            today_order_person = int(today_person_count_df['person_count'].values[0])

        # 昨日交易人数
        yesterday_person_sql = person_sql % ('DATE_SUB(CURDATE(), interval 1 day)', 'DATE_SUB(CURDATE(), interval 1 day)')
        yesterday_person_count_df = pd.read_sql(yesterday_person_sql, conn_lh)
        if yesterday_person_count_df.empty:
            yesterday_order_person = 0
        else:
            yesterday_order_person = int(yesterday_person_count_df['person_count'].values[0])

        today_df['pretty_count'] = today_df['pretty_count'].astype(int)
        today_data = {
            'today_price': round(float(today_df['total_price'].sum()), 2), # 今日交易金额
            'today_order_count': int(today_df['order_count'].sum()), # 今日交易订单数
            'today_order_person': today_order_person, # 今日交易人数
        }
        yesterday_data = {
            'yesterday_price': round(float(yesterday_df['total_price'].sum()), 2), # 昨日交易金额
            'yesterday_order_count': int(yesterday_df['order_count'].sum()), # 昨日交易订单数
            'yesterday_order_person': yesterday_order_person, # 昨日交易人数
        }
        return_data = {
            "today_data": today_data,
            "yesterday_data": yesterday_data,
            "day_data": today_df.to_dict("records")
        }

        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error((traceback.format_exc()))
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
        except:
            pass

# 今日交易实时动态
@lhhomebp.route('/today/dynamic/transaction', methods=["GET"])
def today_dynamic_transaction():
    try:
        # try:
        #     logger.info(request.json)
        #     token = request.headers["Token"]
        #     user_id = request.args.get("user_id")
        #
        #     if not user_id and not token:
        #         return {"code": "10001", "status": "failed", "msg": message["10001"]}
        #
        #     check_token_result = check_token(token, user_id)
        #     if check_token_result["code"] != "0000":
        #         return check_token_result
        # except Exception as e:
        #     # 参数名错误
        #     logger.error(e)
        #     return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_crm or not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        # 七位
        # 今日交易时时动态
        sell_order_sql_7 = '''
            select t1.sub_time, t1.phone, t2.pretty_type_name from
            (select TIMESTAMPDIFF(second,pay_time,now())/60 sub_time, phone, sell_id from lh_pretty_client.lh_order
            where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
            and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE
            order by pay_time desc
            limit 3) t1
            left join
            (select id, pretty_type_name from lh_pretty_client.lh_sell) t2
            on t1.sell_id = t2.id
        '''

        # 八位
        sell_order_sql_8 = '''
            select t1.sub_time, t1.phone, t2.pretty_type_name from
            (select TIMESTAMPDIFF(second,pay_time,now())/60 sub_time, phone, sell_id from lh_pretty_client.le_order
            where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
            and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE
            order by pay_time desc
            limit 3
            ) t1
            left join
            (select id, pretty_type_name from lh_pretty_client.le_second_hand_sell
            where id in
                (select sell_id from lh_pretty_client.le_order where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE)
            union all
            select id, pretty_type_name from lh_pretty_client.le_sell
            where id in
                (select sell_id from lh_pretty_client.le_order where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE)
            ) t2
            on t1.sell_id = t2.id
        '''
        search_name_sql = '''
                select phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from luke_sincerechat.user where phone = "%s"
            '''

        order_df_7 = pd.read_sql(sell_order_sql_7, conn_lh)
        order_df_8 = pd.read_sql(sell_order_sql_8, conn_lh)
        order_df = pd.concat([order_df_7, order_df_8], axis=0)
        if order_df.shape[0] > 0:
            order_df['sub_time'] = round(order_df['sub_time'], 0).astype(int)

            sell_phone_list = order_df['phone'].tolist()
            sell_df_list = []
            for phone in set(sell_phone_list):
                sell_df_list.append(pd.read_sql(search_name_sql % phone, conn_crm))
            sell_df = pd.concat(sell_df_list, axis=0)
            sell_fina_df = order_df.merge(sell_df, how='left', on='phone')
            sell_fina_df.sort_values('sub_time', ascending=True, inplace=True)
            sell_fina_df = sell_fina_df[:3]
            sell_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            sell_list = sell_fina_df.to_dict("records")
        else:
            sell_list = []

        for sl in sell_list:
            if sl["phone"]:
                sl["phone"] = sl["phone"][0:4]+len(sl["phone"][4:])*"*"
            if sl["username"]:
                sl["username"] = sl["username"][0]+len(sl["username"][1:])*"*"

        return_data = {
            "sell_dynamic": sell_list,
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
            conn_crm.close()
        except:
            pass


@lhhomebp.route('/change/time', methods=["POST"])
def change_time():
    try:
        try:
            logger.info(request.json)
            if len(request.json) != 3:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            start_time = request.json['start_time']
            end_time = request.json['end_time']
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_lh = direct_get_conn(analyze_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        update_sql = '''
            update lh_analyze.sys_activity set start_time=%s, end_time=%s where id = 1
        '''
        with conn_lh.cursor() as cursor:
            cursor.execute(update_sql, (start_time, end_time))
        conn_lh.commit()
        return {"code": "0000", "status": "success", "msg": '更新成功'}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
        except:
            pass

@lhhomebp.route('/search/time', methods=["GET"])
def search_time():
    try:
        # try:
        #     logger.info(request.json)
        #     if len(request.json) != 3:
        #         return {"code": "10004", "status": "failed", "msg": message["10004"]}
        #
        #     token = request.headers["Token"]
        #     user_id = request.json["user_id"]
        #
        #     if not user_id and not token:
        #         return {"code": "10001", "status": "failed", "msg": message["10001"]}
        #
        #     check_token_result = check_token(token, user_id)
        #     if check_token_result["code"] != "0000":
        #         return check_token_result
        #
        #     start_time = request.json['start_time']
        #     end_time = request.json['end_time']
        # except Exception as e:
        #     # 参数名错误
        #     logger.error(e)
        #     return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_lh = direct_get_conn(analyze_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        search_sql = '''
            select start_time, end_time from sys_activity where id = 1
        '''
        time_data = pd.read_sql(search_sql, conn_lh)
        time_data['start_time'] = time_data['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        time_data['end_time'] = time_data['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        time_data = time_data.to_dict("records")[0]
        return {"code": "0000", "status": "success", "msg": time_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
        except:
            pass

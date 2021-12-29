# -*- coding: utf-8 -*-

# @Time : 2021/12/6 9:11

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : lh8_second_home_page.py
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

#8位个人成交排行
@lhhomebp.route("deal/person",methods=["GET"])
def deal_person():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()
        try:
            logger.info("env:%s" %ENV)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}




        logger.info(conn_lh)
        #8位个人
        sql = '''select phone,sum(total_price) total_money from lh_pretty_client.le_order where del_flag = 0 and `status`=1 and type=4 and DATE_FORMAT(create_time,"%Y%m%d") =CURRENT_DATE() group by phone order by total_money desc limit 10'''
        logger.info(sql)
        datas = pd.read_sql(sql,conn_lh)
        # datas = datas.to_dict("records")
        # logger.info(len(datas))
        phone_lists = datas["phone"].tolist()

        logger.info(phone_lists)
        if not phone_lists:
            return {"code":"0000","status":"success","msg":[]}
        sql = '''select phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from crm_user_{} where phone in ({})'''.format(current_time,",".join(phone_lists))
        logger.info(sql)
        user_data = pd.read_sql(sql,conn_analyze)
        datas = datas.merge(user_data,on="phone",how="left")
        logger.info(datas)
        datas["username"].fillna("",inplace=True)
        datas = datas.to_dict("records")

        return {"code":"0000","status":"success","msg":datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
        conn_analyze.close()


# 运营中心
@lhhomebp.route("deal/bus",methods=["GET"])
def deal_business():
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
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        #先查运营中心的人数
        cursor_lh = conn_lh.cursor()
        cursor_ana = conn_analyze.cursor()
        bus_sql = '''select operatename,contains from operate_relationship_crm where crm = 1'''

        logger.info(bus_sql)
        cursor_ana.execute(bus_sql)
        operate_datas = cursor_ana.fetchall()

        return_lists = []
        #取出今天的订单表
        sql = '''
        select phone,sum(total_price) total_money from lh_pretty_client.le_order where del_flag = 0 and type=4 and `status`=1 and date_format(create_time,"%Y%m%d") = CURRENT_DATE() group by phone 
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
            logger.info(return_lists.iloc[0:10])
            return_data = return_lists.iloc[0:10].to_dict("records")
        else:
            return_data = return_lists
        return {"code":"0000","status":"success","msg":return_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
        conn_lh.close()


# 交易数据中心累计 7 8 位
@lhhomebp.route("datacenter",methods=["GET"])
def data_center():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        cursor_analyze = conn_analyze.cursor()
        sql = '''select start_time, end_time, filter_phone,remarks,filter_pay_type_7,filter_pay_type_8 from sys_activity where id = 1'''
        cursor_analyze.execute(sql)
        data = cursor_analyze.fetchone()
        logger.info(data)

        start_time = data[0]
        end_time = data[1]
        filter_phone = data[2]
        remarks = data[3]
        filter_pay_type_7 = data[4]
        filter_pay_type_8 = data[5]


        # if filter_phone:
        #     filter_phone = filter_phone[1: -1]
        #     sql = '''select sum(person_count) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from (
        #     select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
        #     select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_pretty_client.lh_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" and phone not in (%s) group by phone) t1
        #     union all
        #     select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
        #     select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_pretty_client.le_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" and phone not in (%s) group by phone) t2)t
        #     ''' % (start_time, end_time, filter_phone, start_time, end_time, filter_phone)
        # else:
        #     sql = '''select sum(person_count) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from (
        #     select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
        #     select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" group by phone) t1
        #     union all
        #     select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
        #     select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" group by phone) t2)t''' % (
        #     start_time, end_time, start_time, end_time)

        condition_sql = ""
        if start_time and end_time:
            condition_sql = condition_sql + ''' and create_time >= "%s" and create_time <= "%s" ''' %(start_time,end_time)
        if filter_phone:
            condition_sql = condition_sql + ''' and phone not in (%s)''' %(filter_phone[1:-1])


        sql_7 =  '''
        select sum(person_count) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from (
            select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
            select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_order where del_flag = 0 and type in (1,4) and `status` = 1 
        '''
        sql_7_group = ''' group by phone) t1'''


        sql_8 = '''
           select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
            select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1,4) and `status` = 1     
        '''
        sql_8_group = ''' group by phone) t2)t '''
        logger.info(condition_sql)

        if condition_sql:
            sql_7 = sql_7 + condition_sql
            sql_8 = sql_8 + condition_sql

        if filter_pay_type_7:
            sql_7 = sql_7 + ''' and pay_type not in (%s) ''' % (filter_pay_type_7[1:-1])

        if filter_pay_type_8:
            sql_8 = sql_8 + condition_sql + ''' and pay_type not in (%s) ''' % (filter_pay_type_8[1:-1])

        sql = sql_7 + sql_7_group + " union all " + sql_8 + sql_8_group

        logger.info(sql)
        data = pd.read_sql(sql,conn_lh)
        data = data.to_dict("records")[0]
        data["start_time"] = datetime.datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        data["end_time"] = datetime.datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        data["remarks"] = remarks
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
        sql='''select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_pretty_client.le_order where del_flag = 0 and type=4 and `status` = 1 and DATE_FORMAT(create_time,'%Y-%m-%d') = CURRENT_DATE() group by phone) t2'''
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



# 今日交易实时动态
@lhhomebp.route('/today/dynamic/transaction', methods=["GET"])
def today_dynamic_transaction():
    try:
        try:
            logger.info(request.json)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_analyze or not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        # 八位
        sell_order_sql_8 = '''
                select t1.sub_time, t1.phone, t2.pretty_type_name from
                (select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, phone, sell_id from lh_pretty_client.le_order
                where del_flag=0 and type=4 and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE
                order by pay_time desc
                limit 10
                ) t1
                left join
                (select id, pretty_type_name from lh_pretty_client.le_second_hand_sell
                where id in
                (select sell_id from lh_pretty_client.le_order where del_flag=0 and type=4 and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE)
                union all
                select id, pretty_type_name from lh_pretty_client.le_sell
                where id in
                (select sell_id from lh_pretty_client.le_order where del_flag=0 and type=4 and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE)
                ) t2
                on t1.sell_id = t2.id
            '''

        search_name_sql = '''
                select phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from lh_analyze.crm_user_%s where phone = "%s"
            '''

        order_df_8 = pd.read_sql(sell_order_sql_8, conn_lh)
        if order_df_8.shape[0] > 0:
            order_df_8['sub_time'] = round(order_df_8['sub_time'], 0).astype(int)
            sell_phone_list = order_df_8['phone'].tolist()
            sell_df_list = []
            for phone in set(sell_phone_list):
                sell_df_list.append(pd.read_sql(search_name_sql % (current_time, phone), conn_analyze))
            sell_df = pd.concat(sell_df_list, axis=0)
            sell_fina_df = order_df_8.merge(sell_df, how='left', on='phone')
            sell_fina_df.sort_values('sub_time', ascending=True, inplace=True)
            sell_fina_df = sell_fina_df[:3]
            sell_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            sell_fina_df["username"].fillna("",inplace=True)
            sell_list = sell_fina_df.to_dict("records")
        else:
            sell_list = []

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
            conn_analyze.close()
        except:
            pass

# 今日发布实时动态
@lhhomebp.route('/today/dynamic/publish', methods=["GET"])
def today_dynamic_publish():
    try:
        try:
            logger.info(request.json)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh or not conn_analyze:
            return {"code": "10002", "status": "failed", "message": message["10002"]}

        # 用户名称搜索
        search_name_sql = '''
                select phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from lh_analyze.crm_user_%s where phone = "%s"
            '''



        publish_order_sql = '''
        select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, sell_phone phone, pretty_type_name
        from lh_pretty_client.le_second_hand_sell
        where del_flag=0 and (sell_phone is not null or sell_phone != '')
        and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE
        order by create_time desc
        limit 3'''

        publish_order_df = pd.read_sql(publish_order_sql, conn_lh)
        if publish_order_df.shape[0] > 0:
            publish_order_df['sub_time'] = round(publish_order_df['sub_time'], 0).astype(int)

            publish_phone_list = publish_order_df['phone'].to_list()
            publish_df_list = []
            for phone in set(publish_phone_list):
                publish_df_list.append(pd.read_sql(search_name_sql % ( current_time, phone), conn_analyze))
            publish_df = pd.concat(publish_df_list, axis=0)
            publish_fina_df = publish_order_df.merge(publish_df, how='left', on='phone')
            publish_fina_df["username"].fillna("", inplace=True)
            publish_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            publish_list = publish_fina_df.to_dict("records")
        else:
            publish_list = []

        # for pl in publish_list:
        #     if pl["phone"]:
        #         pl["phone"] = pl["phone"][0:4]+len(pl["phone"][4:])*"*"
        #     if pl["username"]:
        #         pl["username"] = pl["username"][0]+len(pl["username"][1:])*"*"

        return_data = {
            "publish_dynamic": publish_list,
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
            conn_analyze.close()
        except:
            pass

# 查看活动数据（时间，名称，筛选条件）
@lhhomebp.route('/search/activity/data', methods=["GET"])
def search_activity_data():
    try:
        try:
            token = request.headers["Token"]
            user_id = request.args["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_lh = direct_get_conn(analyze_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        search_sql = '''
            select id, start_time, end_time, remarks, filter_phone,filter_pay_type_7,filter_pay_type_8 from sys_activity
        '''
        data = pd.read_sql(search_sql, conn_lh)
        data['start_time'] = data['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data['end_time'] = data['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data['filter_phone'] = data['filter_phone'].apply(lambda x: json.loads(x) if x else [])
        data['filter_pay_type_7'] = data['filter_pay_type_7'].apply(lambda x: json.loads(x) if x else [])
        data['filter_pay_type_8'] = data['filter_pay_type_8'].apply(lambda x: json.loads(x) if x else [])
        data = data.to_dict("records")
        return {"code": "0000", "status": "success", "msg": data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
        except:
            pass

# 修改活动数据
@lhhomebp.route('/change/activity/data', methods=["POST"])
def change_activity_data():
    try:
        try:
            logger.info(request.json)
            if len(request.json) != 8:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            activity_id = request.json['id']
            remarks = request.json['remarks']
            start_time = request.json['start_time']
            end_time = request.json['end_time']
            filter_phone = request.json['filter_phone']
            filter_pay_type_7 = request.json["filter_pay_type_7"]
            filter_pay_type_8 = request.json["filter_pay_type_8"]
            if not start_time or not end_time:
                return {"code": "10016", "status": "failed", "msg": message["10016"]}

        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_an = direct_get_conn(analyze_mysql_conf)


        if not conn_an:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        update_sql = '''
            update lh_analyze.sys_activity set start_time=%s, end_time=%s, remarks=%s, filter_phone=%s,filter_pay_type_7=%s,filter_pay_type_8=%s where id = %s
        '''
        if len(filter_phone) == 0:
            filter_phone = []
        else:
            filter_phone = filter_phone

        filter_pay_type_7 = [] if len(filter_pay_type_7) == 0 else filter_pay_type_7
        filter_pay_type_8 = [] if len(filter_pay_type_8) == 0 else filter_pay_type_8

        # 原活动数据
        select_sql = '''select * from sys_activity where id = %s''' % activity_id
        old_activity = pd.read_sql(select_sql, conn_an)
        old_activity = old_activity.to_dict("records")[0]
        logger.info(old_activity)
        old_start_time = old_activity["start_time"].strftime('%Y-%m-%d %H:%M:%S')
        old_end_time = old_activity["end_time"].strftime('%Y-%m-%d %H:%M:%S')
        old_remarks = old_activity["remarks"]
        old_filter_phone = json.loads(old_activity["filter_phone"])
        old_filter_pay_type_7 = json.loads(old_activity["filter_pay_type_7"])
        old_filter_pay_type_8 = json.loads(old_activity["filter_pay_type_8"])

        compare = []
        if old_start_time != start_time:
            compare.append("开始时间 %s 变更为 %s" % (old_start_time, start_time))
        if old_end_time != end_time:
            compare.append("结束时间 %s 变更为 %s" % (old_end_time, end_time))
        if old_remarks != remarks:
            compare.append("备注 %s 变更为 %s" % (old_remarks, remarks))
        if old_filter_phone != filter_phone:
            compare.append("过滤手机号 %s 变更为 %s" % (old_filter_phone, filter_phone))
        if old_filter_pay_type_7 != filter_pay_type_7:
            pay_type_7 = {-1: "未知", 0: "信用点", 1: "诚聊余额", 2: "诚聊通余额", 3: "微信", 4: "支付宝", 5: "后台", 6: "银行卡",
             7: "诚聊通佣金", 8: "诚聊通红包"}
            old_type_list = [pay_type_7.get(old_type) for old_type in old_filter_pay_type_7]
            new_type_list = [pay_type_7.get(new_type) for new_type in filter_pay_type_7]
            compare.append("7位支付类型过滤 %s 变更为 %s" % (old_type_list, new_type_list))
            # compare.append("七位支付类型:%s" %{"-1":"未知","0":"信用点","1":"诚聊余额","2":"诚聊通余额","3":"微信","4":"支付宝","5":"后台","6":"银行卡","7":"诚聊通佣金","8":"诚聊通红包"})
        if old_filter_pay_type_8 != filter_pay_type_8:
            pay_type_8 = {-1: "信用点",0:"采购金", 1: "收银台", 2: "诚聊通余额", 3: "微信", 4: "支付宝", 5: "后台",6: "银行卡",8: "禄可商务转入"}
            old_type_list = [pay_type_8.get(old_type) for old_type in old_filter_pay_type_8]
            new_type_list = [pay_type_8.get(new_type) for new_type in filter_pay_type_8]
            compare.append("8位支付类型过滤 %s 变更为 %s" % (old_type_list, new_type_list))
            # compare.append("八位支付类型:%s" % {"-1": "信用点","0":"采购金", "1": "收银台", "2": "诚聊通余额", "3": "微信", "4": "支付宝", "5": "后台","6": "银行卡","8": "禄可商务转入"})
        logger.info("compare:%s" % compare)


        with conn_an.cursor() as cursor:
            cursor.execute(update_sql, (start_time, end_time, remarks, json.dumps(filter_phone),json.dumps(filter_pay_type_7),json.dumps(filter_pay_type_8), activity_id))



            if compare:
                insert_sql = '''insert into sys_log (user_id,log_url,log_req,log_action,remark) values (%s,%s,%s,%s,%s)'''
                params = []
                params.append(user_id)
                params.append("/user/relate/update/user/ascription")
                params.append(json.dumps(request.json))
                params.append("修改活动数据")
                params.append("<br>".join(compare))
                logger.info(params)
                cursor.execute(insert_sql, params)

        conn_an.commit()
        return {"code": "0000", "status": "success", "msg": '更新成功'}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_an.close()
        except:
            pass


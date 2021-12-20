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

#8位个人成交排行
@lhhomebp.route("deal/person",methods=["GET"])
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
        cursor = conn_crm.cursor()

        logger.info(conn_lh)
        #7位 8位个人
        sql = '''select phone,sum(total_price) total_money from le_order where del_flag = 0 and `status`=1 and type in (1,4) and DATE_FORMAT(create_time,"%Y%m%d") =CURRENT_DATE() group by phone order by total_money desc limit 3'''
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

        bus_sql = '''select operatename,contains from operate_relationship_crm where operatename  crm = 1'''

        logger.info(bus_sql)
        cursor_ana.execute(bus_sql)
        operate_datas = cursor_ana.fetchall()

        return_lists = []

        #取出今天的订单表
        sql = '''
        select phone,sum(total_price) total_money from le_order where del_flag = 0 and type in (1,4) and `status`=1 and date_format(create_time,"%Y%m%d") = CURRENT_DATE() group by phone 
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


# 交易数据中心累计 7 8 位
@lhhomebp.route("datacenter",methods=["GET"])
def data_center():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        cursor_analyze = conn_analyze.cursor()
        sql = '''select start_time, end_time, filter_phone,remarks from sys_activity where id = 1'''
        cursor_analyze.execute(sql)
        data = cursor_analyze.fetchone()
        logger.info(data)

        start_time = data[0]
        end_time = data[1]
        filter_phone = data[2]
        remarks = data[3]

        if filter_phone:
            filter_phone = filter_phone[1: -1]
            sql = '''select sum(person_count) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from (
            select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
            select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" and phone not in (%s) group by phone) t1
            union all 
            select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
            select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" and phone not in (%s) group by phone) t2)t
            ''' % (start_time, end_time, filter_phone, start_time, end_time, filter_phone)
        else:
            sql = '''select sum(person_count) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from (
            select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
            select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from lh_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" group by phone) t1
            union all 
            select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
            select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1,4) and `status` = 1 and create_time >= "%s" and create_time <= "%s" group by phone) t2)t''' % (
            start_time, end_time, start_time, end_time)

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
select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1,4) and `status` = 1 and DATE_FORMAT(create_time,'%Y-%m-%d') = CURRENT_DATE() group by phone) t2'''
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

        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_crm or not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        # 八位
        sell_order_sql_8 = '''
                sselect t1.sub_time, t1.phone, t2.pretty_type_name from
                (select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, phone, sell_id from lh_pretty_client.le_order
                where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE
                order by pay_time desc
                limit 10
                ) t1
                left join
                (select id, pretty_type_name from lh_pretty_client.le_second_hand_sell
                where id in
                (select sell_id from lh_pretty_client.le_order where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE)
                union all
                select id, pretty_type_name from lh_pretty_client.le_sell
                where id in
                (select sell_id from lh_pretty_client.le_order where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
                and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE)
                ) t2
                on t1.sell_id = t2.id
            '''

        search_name_sql = '''
                select phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from luke_sincerechat.user where phone = "%s"
            '''

        order_df_8 = pd.read_sql(sell_order_sql_8, conn_lh)
        if order_df_8.shape[0] > 0:
            order_df_8['sub_time'] = round(order_df_8['sub_time'], 0).astype(int)
            sell_phone_list = order_df_8['phone'].tolist()
            sell_df_list = []
            for phone in set(sell_phone_list):
                sell_df_list.append(pd.read_sql(search_name_sql % phone, conn_crm))
            sell_df = pd.concat(sell_df_list, axis=0)
            sell_fina_df = order_df_8.merge(sell_df, how='left', on='phone')
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
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh or not conn_crm:
            return {"code": "10002", "status": "failed", "message": message["10002"]}

        # 用户名称搜索
        search_name_sql = '''
                select phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) username from luke_sincerechat.user where phone = "%s"
            '''



        publish_order_sql = '''
        (select TIMESTAMPDIFF(second,up_time,now())/60 sub_time, sell_phone phone, pretty_type_name
        from lh_pretty_client.le_sell
        where del_flag=0 and (sell_phone is not null or sell_phone != '')
        and DATE_FORMAT(up_time,"%Y-%m-%d") = CURRENT_DATE
        order by up_time desc
        limit 10)
        union all
        (select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, sell_phone phone, pretty_type_name
        from lh_pretty_client.le_second_hand_sell
        where del_flag=0 and (sell_phone is not null or sell_phone != '')
        and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE
        order by create_time desc
        limit 10)
        order by sub_time
        limit 3'''

        publish_order_df = pd.read_sql(publish_order_sql, conn_lh)
        if publish_order_df.shape[0] > 0:
            publish_order_df['sub_time'] = round(publish_order_df['sub_time'], 0).astype(int)

            publish_phone_list = publish_order_df['phone'].to_list()
            publish_df_list = []
            for phone in set(publish_phone_list):
                publish_df_list.append(pd.read_sql(search_name_sql % phone, conn_crm))
            publish_df = pd.concat(publish_df_list, axis=0)
            publish_fina_df = publish_order_df.merge(publish_df, how='left', on='phone')
            publish_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            publish_list = publish_fina_df.to_dict("records")
        else:
            publish_list = []

        for pl in publish_list:
            if pl["phone"]:
                pl["phone"] = pl["phone"][0:4]+len(pl["phone"][4:])*"*"
            if pl["username"]:
                pl["username"] = pl["username"][0]+len(pl["username"][1:])*"*"

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
            conn_crm.close()
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
            select id, start_time, end_time, remarks, filter_phone from sys_activity
        '''
        data = pd.read_sql(search_sql, conn_lh)
        data['start_time'] = data['start_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data['end_time'] = data['end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data['filter_phone'] = data['filter_phone'].apply(lambda x: json.loads(x) if x else [])
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
            if len(request.json) != 6:
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
            if not start_time or not end_time:
                return {"code": "10016", "status": "failed", "msg": message["10016"]}

        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_lh = direct_get_conn(analyze_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        update_sql = '''
            update lh_analyze.sys_activity set start_time=%s, end_time=%s, remarks=%s, filter_phone=%s where id = %s
        '''
        if len(filter_phone)==0:
            filter_phone = None
        else:
            filter_phone = json.dumps(filter_phone)

        with conn_lh.cursor() as cursor:
            cursor.execute(update_sql, (start_time, end_time, remarks, filter_phone, activity_id))
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


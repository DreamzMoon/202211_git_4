# -*- coding: utf-8 -*-

# @Time : 2021/12/6 9:11

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : lh_home_page.py
import sys
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


# 交易数据中心
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


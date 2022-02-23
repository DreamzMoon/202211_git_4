# -*- coding: utf-8 -*-
# @Time : 2022/2/23  15:14
# @Author : shihong
# @File : .py
# --------------------------------------
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

clmhomebp = Blueprint('clmhome', __name__, url_prefix='/clmhome')

'''个人排行版'''
@clmhomebp.route("person/top",methods=["GET"])
def person_top():
    try:
        # conn_clg = ssh_get_conn(clg_ssh_conf,clg_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        sql = ''''''

        datas = pd.read_sql(sql, conn_clg)
        phone_lists = datas["phone"].tolist()

        logger.info(phone_lists)
        if not phone_lists:
            return {"code": "0000", "status": "success", "msg": []}
        sql = '''select phone,if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username from crm_user where phone in ({})'''.format(
            ",".join(phone_lists))
        logger.info(sql)
        user_data = pd.read_sql(sql, conn_analyze)
        datas = datas.merge(user_data, on="phone", how="left")
        logger.info(datas)
        datas["username"].fillna("", inplace=True)
        datas = datas.to_dict("records")

        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

'''交易中心'''
@clmhomebp.route("datacenter", methods=["GET"])
def data_center():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        '''今日'''
        # sql = '''
        #     select count(distinct unionid) person_count, count(distinct shop_id) shop_count, count(*) order_count, sum(money)+sum(freight) total_money from luke_marketing.orders
        #     where is_del=0 and `status` in (1,2,4,6)
        #     and from_unixtime(addtime, "%Y-%m-%d")=current_date
        # '''
        '''近6个月'''
        sql = '''
            select count(distinct unionid) person_count, count(distinct shop_id) shop_count, count(*) order_count, sum(money)+sum(freight) total_money from luke_marketing.orders
            where is_del=0 and `status` in (1,2,4,6)
            and from_unixtime(addtime, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month)
        '''
        data = pd.read_sql(sql,conn_crm).to_dict("records")
        return {"code":"0000","status":"success","msg":data}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
        except:
            pass

'''店铺排行版'''
@clmhomebp.route("shop/top",methods=["GET"])
def shop_top():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)

        '''今日'''
        # sql = '''
        #     select t2.name, t1.total_money from
        #     (select shop_id, sum(money) + sum(freight) total_money from luke_marketing.orders
        #     where is_del=0 and `status` in (1,2,4,6)
        #     and from_unixtime(addtime, "%Y-%m-%d")>=current_date
        #     group by shop_id
        #     order by total_money desc
        #     limit 9) t1
        #     left join
        #     (select id, name from luke_marketing.shop) t2
        #     on t1.shop_id=t2.id
        # '''
        '''近6个月'''
        sql = '''
            select t2.name, t1.total_money from
            (select shop_id, sum(money) + sum(freight) total_money from luke_marketing.orders
            where is_del=0 and `status` in (1,2,4,6)
            and from_unixtime(addtime, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month)
            group by shop_id
            order by total_money desc
            limit 9) t1
            left join
            (select id, name from luke_marketing.shop) t2
            on t1.shop_id=t2.id
        '''
        datas = pd.read_sql(sql, conn_crm).to_dict("records")
        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
        except:
            pass

'''区域成交统计列表'''
@clmhomebp.route('/area/list', methods=["POST"])
def area_list():
    try:
        province_code = request.json['province_code']
        city_code = request.json['city_code']
        page = request.json['page']
        size = request.json['size']

        conn_clg = direct_get_conn(clg_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_clg or not conn_analyze:
            return {"code": "10002", "status": "failed", "message": message["10002"]}
        analyze_cursor = conn_analyze.cursor()

        # area_list_sql = '''
        #     select count(distinct user_id) person_count, sum(order_count) order_count, sum(total_money) total_money, sum(total_count) total_count, area_name from
        #     (select t1.user_id, count(*) order_count, sum(t1.pay_money) total_money, sum(t2.buy_num) total_count{t_area_name} from
        #     (select order_sn, user_id, date_format(create_time, "%Y-%m-%d") create_time, pay_money{area_name} from trade_order_info
        #     where date_format(create_time, "%Y-%m-%d")=current_date and order_status in (3,4,5,6,10,15) and del_flag=0 and voucherMoneyType=1{condition}) t1
        #     left join
        #     (select order_sn, sum(buy_num) buy_num from trade_order_item where date_format(create_time, "%Y-%m-%d")=current_date group by order_sn) t2
        #     on t1.order_sn=t2.order_sn
        #     {group}
        #     union all
        #     select t1.user_id, count(*) order_count, sum(t1.pay_money) total_money, sum(t2.buy_num) total_count{t_area_name} from
        #     (select order_sn, user_id, date_format(create_time, "%Y-%m-%d") create_time, voucherPayMoney pay_money{area_name} from trade_order_info
        #     where date_format(create_time, "%Y-%m-%d")=current_date and order_status in (3,4,5,6,10,15) and del_flag=0 and voucherMoneyType=2{condition}) t1
        #     left join
        #     (select order_sn, sum(buy_num) buy_num from trade_order_item where date_format(create_time, "%Y-%m-%d")=current_date group by order_sn) t2
        #     on t1.order_sn=t2.order_sn
        #     {group}) t
        #     group by area_name
        # '''
        area_list_sql = '''
            select count(distinct user_id) person_count, sum(order_count) order_count, sum(total_money) total_money, sum(total_count) total_count, area_name from
            (select t1.user_id, count(*) order_count, sum(t1.pay_money) total_money, sum(t2.buy_num) total_count{t_area_name} from 
            (select order_sn, user_id, date_format(create_time, "%Y-%m-%d") create_time, pay_money{area_name} from trade_order_info
            where date_format(create_time, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month) and order_status in (3,4,5,6,10,15) and del_flag=0 and voucherMoneyType=1{condition}) t1
            left join
            (select order_sn, sum(buy_num) buy_num from trade_order_item where date_format(create_time, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month) group by order_sn) t2
            on t1.order_sn=t2.order_sn
            {group}
            union all
            select t1.user_id, count(*) order_count, sum(t1.pay_money) total_money, sum(t2.buy_num) total_count{t_area_name} from 
            (select order_sn, user_id, date_format(create_time, "%Y-%m-%d") create_time, voucherPayMoney pay_money{area_name} from trade_order_info
            where date_format(create_time, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month) and order_status in (3,4,5,6,10,15) and del_flag=0 and voucherMoneyType=2{condition}) t1
            left join
            (select order_sn, sum(buy_num) buy_num from trade_order_item where date_format(create_time, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month) group by order_sn) t2
            on t1.order_sn=t2.order_sn
            {group}) t
            group by area_name
        '''

        if not province_code and not city_code:
            area_list_sql = area_list_sql.format(t_area_name=', t1.consignee_province area_name',area_name=', consignee_province', condition='', group=' group by consignee_province, user_id')
        else:
            # 查找省名称
            province_sql = '''select name from lh_analyze.province where code=%s'''
            analyze_cursor.execute(province_sql, province_code)
            province_name = analyze_cursor.fetchone()[0]
            logger.info(province_name)
            # # 几个直辖市处理----前端限制不展示后两级只传省编码，服务端直接返回区数据
            # municipality_province_code = ['11', '12', '31', '50']
            # # 查直辖市下市辖区编码
            # if province_code in municipality_province_code:
            #     municipality_city_sql = '''select code from city where province_code=%s'''
            #     analyze_cursor.execute(municipality_city_sql, province_code)
            #     city_code = analyze_cursor.fetchone()[0]
            if not city_code:
                # 查找省对应城市
                city_list_sql = '''select name from lh_analyze.city where province_code=%s''' % province_code
                city_list_df = pd.read_sql(city_list_sql, conn_analyze)
                city_name_list = ["'%s'" % city for city in city_list_df['name'].tolist()]
                logger.info(city_name_list)
                # 拼接sql
                area_list_sql = area_list_sql.format(t_area_name=', t1.consignee_city area_name', area_name=', consignee_city',
                                                     condition=' and consignee_province= "%s" and consignee_city in (%s)' % (province_name, ','.join(city_name_list)),
                                                     group=' group by consignee_city, user_id')
            else:
                # 查找市名称
                city_sql = '''select name from lh_analyze.city where code=%s''' % city_code
                analyze_cursor.execute(city_sql)
                city_name = analyze_cursor.fetchone()[0]
                logger.info(city_name)
                # 查找市对应区
                region_list_sql = '''select name from lh_analyze.region where city_code=%s''' % city_code
                region_list_df = pd.read_sql(region_list_sql, conn_analyze)
                region_name_list = ["'%s'" % region for region in region_list_df['name'].tolist()]
                logger.info(region_name_list)
                # 拼接sql
                area_list_sql = area_list_sql.format(t_area_name=', t1.consignee_county area_name', area_name=', consignee_county',
                                                     condition=' and consignee_province="%s" and consignee_city="%s" and consignee_county in (%s)' % (
                                                     province_name, city_name, ','.join(region_name_list)),
                                                     group=' group by consignee_county, user_id')
        logger.info(area_list_sql)
        area_list_df = pd.read_sql(area_list_sql, conn_clg)
        area_list_df['proportion'] = area_list_df['order_count'] / area_list_df['order_count'].sum()
        area_list_df['proportion'] = area_list_df['proportion'].round(2)
        # 按照订单数倒序排序
        logger.info(area_list_df)
        area_list_df.sort_values(['order_count','total_count','total_money'], ascending=[False,False,False], inplace=True)
        logger.info(area_list_df)
        count = area_list_df.shape[0]
        start_index = (page - 1) * size
        end_index = page * size
        area_list_data = area_list_df[start_index:end_index].to_dict("records")

        return_data = {
            'count': count,
            "area_list": area_list_data
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
            conn_analyze.close()
        except:
            pass

'''区域排名地图'''
@clmhomebp.route("area/statis")
def area_statis():
    try:
        # conn_clg = ssh_get_conn(clg_ssh_conf,clg_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()

        # sql = '''select consignee_province,count(*) count from trade_order_info o where del_flag = 0 and DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE() and o.order_status in (4,5,6,10,15) group by consignee_province'''
        sql = '''
            select consignee_province,count(*) count from trade_order_info o where del_flag = 0 and DATE_FORMAT(o.create_time,"%Y-%m_%d")>=date_sub(curdate(), interval 6 month) and o.order_status in (4,5,6,10,15) group by consignee_province
        '''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clg)
        datas = datas.to_dict("records")


        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()
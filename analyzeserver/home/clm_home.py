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
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_crm or not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        '''今日'''
        # sql = '''
        #     select t1.unionid, t2.nickname clmname, t2.phone, t1.total_money from
        #     (select unionid, sum(money)+sum(freight) total_money from luke_marketing.orders
        #     where is_del=0 and `status` in (1,2,4,6)
        #     and from_unixtime(addtime, "%Y-%m-%d")=current_date
        #     group by unionid
        #     order by total_money desc
        #     limit 9) t1
        #     left join
        #     (select unionid, nickname,phone from luke_marketing.user) t2
        #     on t1.unionid=t2.unionid
        # '''
        '''近1个月'''
        sql = '''
            select t1.unionid, t2.nickname clmname, t2.phone, t1.total_money from
            (select unionid, sum(money)+sum(freight) total_money from luke_marketing.orders
            where is_del=0 and `status` in (1,2,4,6)
            and from_unixtime(addtime, "%Y-%m-%d")>=date_sub(curdate(), interval 1 month)
            group by unionid
            order by total_money desc
            limit 9) t1
            left join
            (select unionid, nickname, phone from luke_marketing.user) t2
            on t1.unionid=t2.unionid
        '''
        datas = pd.read_sql(sql, conn_crm)
        phone_lists = datas[(datas["phone"].notna()) & (datas["phone"] != '')]['phone'].tolist()
        logger.info(phone_lists)
        if datas.shape[0] == 0: # 如果数据为空
            return {"code": "0000", "status": "success", "msg": []}
        if len(phone_lists) == 0:
            datas.rename(columns={"clmname": "username"}, inplace=True)
            datas['username'].fillna('', inplace=True)
            datas['phone'].fillna('', inplace=True)
        else:
            sql = '''select phone,if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username from crm_user where phone in ({})'''.format(
                ",".join(phone_lists))
            logger.info(sql)
            user_data = pd.read_sql(sql, conn_analyze)
            datas = datas.merge(user_data, on="phone", how="left")
            datas["username"].fillna("", inplace=True)
            for index, values in datas.iterrows():
                logger.info(values['username'])
                if not values['username'] or values['username'] == '':
                    logger.info('11111')
                    datas.loc[index, 'username'] = values['clmname']
            logger.info(datas)
            datas.drop('clmname', axis=1, inplace=True)
        datas.drop('unionid', axis=1, inplace=True)
        datas = datas.to_dict("records")

        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_crm.close()
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
        '''近1个月'''
        sql = '''
            select count(distinct unionid) person_count, count(distinct shop_id) shop_count, count(*) order_count, sum(money)+sum(freight) total_money from luke_marketing.orders
            where is_del=0 and `status` in (1,2,4,6)
            and from_unixtime(addtime, "%Y-%m-%d")>=date_sub(curdate(), interval 1 month)
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
        #     select t2.name shop_name, t1.total_money from
        #     (select shop_id, sum(money) + sum(freight) total_money from luke_marketing.orders
        #     where is_del=0 and `status` in (1,2,4,6)
        #     and from_unixtime(addtime, "%Y-%m-%d")=current_date
        #     group by shop_id
        #     order by total_money desc
        #     limit 9) t1
        #     left join
        #     (select id, name from luke_marketing.shop) t2
        #     on t1.shop_id=t2.id
        # '''
        '''近1个月'''
        sql = '''
            select t2.name shop_name, t1.total_money from
            (select shop_id, sum(money) + sum(freight) total_money from luke_marketing.orders
            where is_del=0 and `status` in (1,2,4,6)
            and from_unixtime(addtime, "%Y-%m-%d")>=date_sub(curdate(), interval 1 month)
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

        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_crm or not conn_analyze:
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
        # area_list_sql = '''
        #     select{area}
        #     count(distinct orders.unionid) person_count, count(*) order_count, sum(orders.number) total_count, sum(orders.money)+sum(freight) total_money
        #     from luke_marketing.orders
        #     left join luke_marketing.shop on orders.shop_id = shop.id
        #     left join luke_marketing.address_area on address_area.AREA_CODE = shop.area
        #     left join luke_marketing.address_city on address_city.CITY_CODE = address_area.CITY_CODE
        #     left join luke_marketing.address_province on address_province.PROVINCE_CODE = address_city.PROVINCE_CODE
        #     where luke_marketing.orders.is_del = 0 and luke_marketing.orders.`status` in (1,2,4,6) and FROM_UNIXTIME(luke_marketing.orders.addtime,'%Y-%m-%d')>=date_sub(curdate(), interval 1 month)
        #     {condition}
        #     group by {group}
        #     having{having}
        # '''
        '''今日'''
        # area_list_sql = '''
        #     select address_province.PROVINCE_NAME province_name, address_city.CITY_NAME city_name, address_area.AREA_NAME region_name,
        #     orders.unionid, orders.number total_count, orders.money, orders.freight
        #     from luke_marketing.orders
        #     left join luke_marketing.shop on orders.shop_id = shop.id
        #     left join luke_marketing.address_area on address_area.AREA_CODE = shop.area
        #     left join luke_marketing.address_city on address_city.CITY_CODE = address_area.CITY_CODE
        #     left join luke_marketing.address_province on address_province.PROVINCE_CODE = address_city.PROVINCE_CODE
        #     where luke_marketing.orders.is_del = 0 and luke_marketing.orders.`status` in (1,2,4,6) and FROM_UNIXTIME(luke_marketing.orders.addtime,'%Y-%m-%d')=current_date
        # '''
        '''近1个月'''
        area_list_sql = '''
            select address_province.PROVINCE_NAME province_name, address_city.CITY_NAME city_name, address_area.AREA_NAME region_name,
            orders.unionid, orders.number total_count, orders.money, orders.freight
            from luke_marketing.orders
            left join luke_marketing.shop on orders.shop_id = shop.id
            left join luke_marketing.address_area on address_area.AREA_CODE = shop.area
            left join luke_marketing.address_city on address_city.CITY_CODE = address_area.CITY_CODE
            left join luke_marketing.address_province on address_province.PROVINCE_CODE = address_city.PROVINCE_CODE
            where luke_marketing.orders.is_del = 0 and luke_marketing.orders.`status` in (1,2,4,6) and FROM_UNIXTIME(luke_marketing.orders.addtime,'%Y-%m-%d')>=date_sub(curdate(), interval 1 month)
        '''
        area_list_df = pd.read_sql(area_list_sql, conn_crm)
        area_list_df = area_list_df[area_list_df['province_name'].notna()]
        area_list_df['total_money'] = area_list_df['money'] + area_list_df['freight']
        area_list_df.drop(['money', 'freight'], axis=1, inplace=True)
        # if not province_code and not city_code:
        #     area_list_sql = area_list_sql.format(area=' address_province.PROVINCE_NAME area_name,',
        #                                          condition='',
        #                                          group='area_name',
        #                                          having=' area_name is not null')
        # else:
        #     # 查找省名称
        #     province_sql = '''select name from lh_analyze.province where code=%s'''
        #     analyze_cursor.execute(province_sql, province_code)
        #     province_name = analyze_cursor.fetchone()[0]
        #     if not city_code:
        #         # 查找省对应城市
        #         city_list_sql = '''select name from lh_analyze.city where province_code=%s''' % province_code
        #         city_list_df = pd.read_sql(city_list_sql, conn_analyze)
        #         city_name_list = ["'%s'" % city for city in city_list_df['name'].tolist()]
        #         logger.info(city_name_list)
        #         # 拼接sql
        #         area_list_sql = area_list_sql.format(area=' address_city.CITY_NAME area_name,',
        #                                              condition=' and address_province.PROVINCE_NAME="%s" and address_city.CITY_NAME in (%s)' % (province_name, ','.join(city_name_list)),
        #                                              group='area_name',
        #                                              having=' area_name is not null')
        #     else:
        #         # 查找市名称
        #         city_sql = '''select name from lh_analyze.city where code=%s''' % city_code
        #         analyze_cursor.execute(city_sql)
        #         city_name = analyze_cursor.fetchone()[0]
        #         logger.info(city_name)
        #         # 查找市对应区
        #         region_list_sql = '''select name from lh_analyze.region where city_code=%s''' % city_code
        #         region_list_df = pd.read_sql(region_list_sql, conn_analyze)
        #         region_name_list = ["'%s'" % region for region in region_list_df['name'].tolist()]
        #         logger.info(region_name_list)
        #         # 拼接sql
        #         area_list_sql = area_list_sql.format(area=' address_city.CITY_NAME area_name,',
        #                                              condition=' and address_province.PROVINCE_NAME="%s" and address_city.CITY_NAME="%s" and address_area.AREA_NAME in (%s)' % (
        #                                              province_name, city_name, ','.join(region_name_list)),
        #                                              group='area_name',
        #                                              having=' area_name is not null')
        # logger.info(area_list_sql)
        # area_list_df = pd.read_sql(area_list_sql, conn_crm)
        if not province_code and not city_code:
            area_list_df.drop(['city_name', 'region_name'], axis=1, inplace=True)
            area_list_df.rename(columns={"province_name": "area_name"}, inplace=True)
        else:
        # 查找省名称
            province_sql = '''select name from lh_analyze.province where code=%s'''
            analyze_cursor.execute(province_sql, province_code)
            province_name = analyze_cursor.fetchone()[0]
            if not city_code:
                # 查找省对应城市
                city_list_sql = '''select name from lh_analyze.city where province_code=%s''' % province_code
                city_list_df = pd.read_sql(city_list_sql, conn_analyze)
                # city_name_list = ["'%s'" % city for city in city_list_df['name'].tolist()]
                city_name_list = city_list_df['name'].tolist()
                area_list_df = area_list_df[(area_list_df['province_name'] == province_name) & (area_list_df['city_name'].isin(city_name_list))]
                logger.info(area_list_df.shape)
                area_list_df.drop(['province_name', 'region_name'], axis=1, inplace=True)
                area_list_df.rename(columns={"city_name": "area_name"}, inplace=True)
            else:
                # 查找市名称
                city_sql = '''select name from lh_analyze.city where code=%s''' % city_code
                analyze_cursor.execute(city_sql)
                city_name = analyze_cursor.fetchone()[0]
                logger.info(city_name)
                # 查找市对应区
                region_list_sql = '''select name from lh_analyze.region where city_code=%s''' % city_code
                region_list_df = pd.read_sql(region_list_sql, conn_analyze)
                # region_name_list = ["'%s'" % region for region in region_list_df['name'].tolist()]
                region_name_list = region_list_df['name'].tolist()
                area_list_df = area_list_df[(area_list_df['province_name'] == province_name)
                                            & (area_list_df['city_name'] == city_name)
                                            & (area_list_df['region_name'].isin(region_name_list))]
                area_list_df.drop(['province_name', 'city_name'], axis=1, inplace=True)
                area_list_df.rename(columns={"region_name": "area_name"}, inplace=True)
        # 人数统计
        area_person_count_df = pd.DataFrame(area_list_df.groupby('area_name')['unionid'].nunique()).reset_index().rename(columns={"unionid": "person_count"})
        area_list_df = area_list_df.groupby('area_name').agg(
            {"total_count": "sum", "total_money": "sum", "area_name": "count"}).rename(columns={"area_name": "order_count"}).reset_index()
        area_list_df = area_list_df.merge(area_person_count_df, how='left', on='area_name')
        # 占比（订单数）
        area_list_df['proportion'] = area_list_df['order_count'] / area_list_df['order_count'].sum()
        area_list_df['proportion'] = area_list_df['proportion'].round(2)
        # 按照订单数倒序排序
        area_list_df.sort_values(['order_count','total_count','total_money'], ascending=[False,False,False], inplace=True)
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
            conn_crm.close()
            conn_analyze.close()
        except:
            pass

'''区域排名地图'''
@clmhomebp.route("area/statis")
def area_statis():
    try:
        conn_clm = direct_get_conn(crm_mysql_conf)
        '''今日'''
        # sql = '''
        #     select address_province.PROVINCE_NAME pro_name,count(*) count from luke_marketing.orders
        #     left join luke_marketing.shop on orders.shop_id = shop.id
        #     left join luke_marketing.address_area on address_area.AREA_CODE = shop.area
        #     left join luke_marketing.address_city on address_city.CITY_CODE = address_area.CITY_CODE
        #     left join luke_marketing.address_province on address_province.PROVINCE_CODE = address_city.PROVINCE_CODE
        #     where luke_marketing.orders.is_del = 0 and luke_marketing.orders.`status` in (1,2,4,6) and FROM_UNIXTIME(luke_marketing.orders.addtime,'%Y-%m-%d') = CURRENT_DATE
        #     group by address_province.PROVINCE_NAME
        #     HAVING address_province.PROVINCE_NAME is not null
        #     order by count desc
        # '''
        '''近1个月'''
        sql = '''
            select address_province.PROVINCE_NAME pro_name,count(*) count from luke_marketing.orders
            left join luke_marketing.shop on orders.shop_id = shop.id
            left join luke_marketing.address_area on address_area.AREA_CODE = shop.area
            left join luke_marketing.address_city on address_city.CITY_CODE = address_area.CITY_CODE
            left join luke_marketing.address_province on address_province.PROVINCE_CODE = address_city.PROVINCE_CODE
            where luke_marketing.orders.is_del = 0 and luke_marketing.orders.`status` in (1,2,4,6) and FROM_UNIXTIME(luke_marketing.orders.addtime,'%Y-%m-%d')>=date_sub(curdate(), interval 1 month)
            group by address_province.PROVINCE_NAME
            HAVING address_province.PROVINCE_NAME is not null
            order by count desc
        '''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clm)
        datas = datas.to_dict("records")

        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clm.close()

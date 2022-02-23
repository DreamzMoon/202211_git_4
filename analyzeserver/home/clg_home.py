# -*- coding: utf-8 -*-

# @Time : 2022/1/25 15:05

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : clg_home.py


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

clghomebp = Blueprint('clghome', __name__, url_prefix='/clghome')

'''个人排行版'''
@clghomebp.route("person/top",methods=["GET"])
def person_top():
    try:
        # conn_clg = ssh_get_conn(clg_ssh_conf,clg_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()


        # sql = '''select phone,sum(pay_total_money) pay_total_money from (
        # select phone,sum(pay_money) pay_total_money from trade_order_info where DATE_FORMAT(create_time,"%Y-%m_%d") = CURRENT_DATE and order_status in (4,5,6,10,15)
        # and del_flag = 0 and voucherMoneyType = 1
        # group by phone
        # union all
        # select phone,sum(voucherPayMoney) pay_total_money from trade_order_info where DATE_FORMAT(create_time,"%Y-%m_%d") = CURRENT_DATE and order_status in (4,5,6,10,15)
        # and del_flag = 0 and voucherMoneyType = 2
        # group by phone) t group by phone order by pay_total_money desc limit 3
        # '''
        sql = '''
            select phone,sum(pay_total_money) pay_total_money from (
            select phone,sum(pay_money) pay_total_money from trade_order_info where DATE_FORMAT(create_time,"%Y-%m_%d")>=date_sub(curdate(), interval 6 month) and order_status in (4,5,6,10,15)
            and del_flag = 0 and voucherMoneyType = 1
            group by phone
            union all
            select phone,sum(voucherPayMoney) pay_total_money from trade_order_info where DATE_FORMAT(create_time,"%Y-%m_%d")>=date_sub(curdate(), interval 6 month) and order_status in (4,5,6,10,15)
            and del_flag = 0 and voucherMoneyType = 2
            group by phone) t group by phone order by pay_total_money desc limit 3
        '''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clg)
        # datas = datas.to_dict("records")
        # logger.info(len(datas))
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
@clghomebp.route("datacenter", methods=["GET"])
def data_center():
    try:
        # conn_clg = ssh_get_conn(clg_ssh_conf,clg_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        if not conn_clg:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # sql = '''
        # select sum(person_count) person_count,sum(order_count) order_count,sum(total_count) total_count,sum(total_money) total_money from (
        # select count(*) person_count,sum(count) order_count,sum(buy_num) total_count,sum(pay_money) total_money from (
        # select trade_order_info.user_id,count(*) count,sum(buy_num) buy_num,sum(pay_money) pay_money from trade_order_info
        # left join trade_order_item on trade_order_info.order_sn = trade_order_item.order_sn
        # where trade_order_info.voucherMoneyType = 1 and date_format(trade_order_info.create_time, "%Y-%m-%d")=CURRENT_DATE()
        # and trade_order_info.order_status in (3,4,5,6,10,15)
        # group by trade_order_info.user_id
        # union all
        # select trade_order_info.user_id,count(*) count,sum(buy_num) buy_num,sum(voucherPayMoney) pay_money from trade_order_info
        # left join trade_order_item on trade_order_info.order_sn = trade_order_item.order_sn
        # where trade_order_info.voucherMoneyType = 2 and date_format(trade_order_info.create_time, "%Y-%m-%d")=CURRENT_DATE()
        # and trade_order_info.order_status in (3,4,5,6,10,15)
        # group by trade_order_info.user_id ) t group by user_id
        # ) t2
        # '''
        sql = '''
            select sum(person_count) person_count,sum(order_count) order_count,sum(total_count) total_count,sum(total_money) total_money from (
            select count(*) person_count,sum(count) order_count,sum(buy_num) total_count,sum(pay_money) total_money from (
            select trade_order_info.user_id,count(*) count,sum(buy_num) buy_num,sum(pay_money) pay_money from trade_order_info
            left join trade_order_item on trade_order_info.order_sn = trade_order_item.order_sn
            where trade_order_info.voucherMoneyType = 1 and date_format(trade_order_info.create_time, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month)
            and trade_order_info.order_status in (3,4,5,6,10,15)
            group by trade_order_info.user_id
            union all 
            select trade_order_info.user_id,count(*) count,sum(buy_num) buy_num,sum(voucherPayMoney) pay_money from trade_order_info
            left join trade_order_item on trade_order_info.order_sn = trade_order_item.order_sn
            where trade_order_info.voucherMoneyType = 2 and date_format(trade_order_info.create_time, "%Y-%m-%d")>=date_sub(curdate(), interval 6 month)
            and trade_order_info.order_status in (3,4,5,6,10,15)
            group by trade_order_info.user_id ) t group by user_id
            ) t2
        '''

        data = (pd.read_sql(sql,conn_clg)).to_dict("records")
        return {"code":"0000","status":"success","msg":data}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
        except:
            pass

'''商品排行版'''
@clghomebp.route("product/top",methods=["GET"])
def product_top():
    try:
        # conn_clg = ssh_get_conn(clg_ssh_conf,clg_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()


        # sql = '''select goods_name,sum(pay_total_money) pay_total_money from (
        # select od.goods_name,sum(o.pay_money) pay_total_money from trade_order_info o
        # left join trade_order_item od on o.order_sn = od.order_sn
        # where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0 and
        # o.voucherMoneyType = 1
        # group by od.goods_id
        # union all
        # select od.goods_name,sum(o.voucherPayMoney) pay_total_money from trade_order_info o
        # left join trade_order_item od on o.order_sn = od.order_sn
        # where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0 and
        # o.voucherMoneyType = 2
        # group by od.goods_id
        # ) t group by goods_name order by pay_total_money desc limit 3'''
        sql = '''
            select goods_goods_info.goods_name,ooo.pay_total_money from (
						select goods_id,sum(pay_total_money) pay_total_money from (
            select od.goods_id,sum(o.pay_money) pay_total_money from trade_order_info o
            left join trade_order_item od on o.order_sn = od.order_sn
            where DATE_FORMAT(o.create_time,"%Y-%m_%d")>=date_sub(curdate(), interval 6 month) and o.order_status in (4,5,6,10,15) and o.del_flag = 0 and 
            o.voucherMoneyType = 1
            group by od.goods_id
            union all 
            select od.goods_id,sum(o.voucherPayMoney) pay_total_money from trade_order_info o
            left join trade_order_item od on o.order_sn = od.order_sn
            where DATE_FORMAT(o.create_time,"%Y-%m_%d")>=date_sub(curdate(), interval 6 month) and o.order_status in (4,5,6,10,15) and o.del_flag = 0 and 
            o.voucherMoneyType = 2
            group by od.goods_id
            ) t group by goods_id order by pay_total_money desc limit 3
						) ooo left join goods_goods_info on ooo.goods_id = goods_goods_info.goods_id
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

'''店铺排行版'''
@clghomebp.route("shop/top",methods=["GET"])
def shop_top():
    try:
        # conn_clg = ssh_get_conn(clg_ssh_conf,clg_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()

        # sql = '''select shop_name,sum(pay_total_money) pay_total_money from (
        #     select o.shop_name,sum(o.pay_money) pay_total_money from trade_order_info o
        #     where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0
        #     and o.voucherMoneyType = 1
        #     group by o.shop_id
        #     union all
        #     select o.shop_name,sum(o.voucherPayMoney) pay_total_money from trade_order_info o
        #     where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0
        #     and o.voucherMoneyType = 2
        #     group by o.shop_id
        #     ) t group by shop_name order by pay_total_money desc limit 7'''
        sql = '''						select ms.name,sum(pay_total_money) pay_total_money from (
select o.shop_id,sum(o.pay_money) pay_total_money from trade_order_info o
where DATE_FORMAT(o.create_time,"%Y-%m_%d")>=date_sub(curdate(), interval 6 month) and o.order_status in (4,5,6,10,15) and o.del_flag = 0 
and o.voucherMoneyType = 1
group by o.shop_id
union all 
select o.shop_id,sum(o.voucherPayMoney) pay_total_money from trade_order_info o
where DATE_FORMAT(o.create_time,"%Y-%m_%d")>=date_sub(curdate(), interval 6 month) and o.order_status in (4,5,6,10,15) and o.del_flag = 0 
and o.voucherMoneyType = 2
group by o.shop_id
) t 
left join
(select id, name from member_shop_info where del_flag=0) ms
on t.shop_id = ms.id
group by shop_id order by pay_total_money desc limit 7'''
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

'''今日新增用户动态'''
@clghomebp.route('/today/dynamic/newuser', methods=["GET"])
def today_dynamic_newuser():
    try:


        conn_an = direct_get_conn(analyze_mysql_conf)
        # conn_clg = ssh_get_conn(clg_ssh_conf,clg_mysql_conf)
        conn_clg = direct_get_conn(clg_mysql_conf)
        if not conn_clg or not conn_an:
            return {"code": "10002", "status": "failed", "message": message["10002"]}

        search_name_sql = '''
            select phone, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username from crm_user where phone = "%s"
        '''
        # 新注册用户
        new_user_sql = '''
            select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, phone from mall.member_user
            where del_flag=0 and (phone is not null or phone != '')
            and create_time is not null
            and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE
            order by create_time desc
            limit 3
        '''

        new_user_df = pd.read_sql(new_user_sql, conn_clg)
        if new_user_df.shape[0] > 0:
            new_user_df['sub_time'] = round(new_user_df['sub_time'], 0).astype(int)

            new_user_phone_list = new_user_df['phone'].to_list()
            new_user_df_list = []
            for phone in set(new_user_phone_list):
                new_user_df_list.append(pd.read_sql(search_name_sql % phone, conn_an))
            user_df = pd.concat(new_user_df_list, axis=0)
            new_user_fina_df = new_user_df.merge(user_df, how='left', on='phone')
            new_user_fina_df["username"].fillna("", inplace=True)
            new_user_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            new_user_list = new_user_fina_df.to_dict("records")
        else:
            new_user_list = []


        return_data = {
            "new_user": new_user_list
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
            conn_an.close()
        except:
            pass

'''今日商品实时动态'''
@clghomebp.route('/today/dynamic/goods', methods=["GET"])
def today_dynamic_goods():
    try:
        conn_clg = direct_get_conn(clg_mysql_conf)
        if not conn_clg:
            return {"code": "10002", "status": "failed", "message": message["10002"]}

        shop_goods_sql = '''
            select t2.shop_name, t1.item_pay_money sell_money, t1.goods_name, t2.sub_time from
            (select order_sn,goods_name, item_pay_money from trade_order_item where order_sn in (select order_sn from trade_order_info where date_format(create_time, "%Y-%m-%d")=current_date and order_status in (3,4,5,6,10,15))) t1
            left join
            (select order_sn, shop_name, TIMESTAMPDIFF(second,create_time,now())/60 sub_time from trade_order_info where date_format(create_time, "%Y-%m-%d")=current_date and order_status in (3,4,5,6,10,15)) t2
            on t1.order_sn=t2.order_sn
            order by sub_time asc
            limit 3
        '''
        shop_goods_data = pd.read_sql(shop_goods_sql, conn_clg)
        if shop_goods_data.shape[0] > 0:
            shop_goods_data['sub_time'] = round(shop_goods_data['sub_time'], 0).astype(int)
            shop_goods_data.sort_values('sub_time', ascending=False, inplace=True)
            shop_goods_data = shop_goods_data.to_dict("records")
        else:
            shop_goods_data = []

        return_data = {
            "shop_goods": shop_goods_data
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
        except:
            pass

'''今日交易实时动态'''
@clghomebp.route("order/status",methods=["GET"])
def order_status():
    try:
        conn_clg = direct_get_conn(clg_mysql_conf)
        logger.info(conn_clg)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()

        sql = '''
            select phone, pay_money, goods_name, sub_time from
            (select o.phone,o.pay_money,od.goods_name,TIMESTAMPDIFF(second,o.create_time,now())/60 sub_time from trade_order_info o
            left join trade_order_item od on o.order_sn = od.order_sn
            where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0 and o.voucherMoneyType=1
            union all
            select o.phone,o.voucherPayMoney pay_money,od.goods_name,TIMESTAMPDIFF(second,o.create_time,now())/60 sub_time from trade_order_info o
            left join trade_order_item od on o.order_sn = od.order_sn
            where DATE_FORMAT(o.create_time,"%Y-%m_%d") = CURRENT_DATE and o.order_status in (4,5,6,10,15) and o.del_flag = 0 and o.voucherMoneyType=2) t
            order by sub_time asc limit 3
        '''
        logger.info(sql)
        datas = pd.read_sql(sql, conn_clg)
        if datas.shape[0] > 0:
            datas['sub_time'] = round(datas['sub_time'], 0).astype(int)
            datas.sort_values('sub_time', ascending=False, inplace=True)
        # datas = datas.to_dict("records")
        # logger.info(len(datas))
            phone_lists = datas["phone"].tolist()

            logger.info(phone_lists)
            if not phone_lists:
                return {"code": "0000", "status": "success", "msg": []}
            sql = '''select phone,if(`name` is not null and `name`!='',name, if(nickname is not null,nickname,"")) username from crm_user where phone in ({})'''.format(
                ",".join(phone_lists))
            logger.info(sql)
            user_data = pd.read_sql(sql, conn_analyze)
            datas = datas.merge(user_data, on="phone", how="left")
            logger.info(datas)
            datas["username"].fillna("", inplace=True)
            datas = datas.to_dict("records")
        else:
            datas = []

        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_clg.close()
        conn_analyze.close()

'''区域成交统计列表'''
@clghomebp.route('/area/list', methods=["POST"])
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
@clghomebp.route("area/statis")
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
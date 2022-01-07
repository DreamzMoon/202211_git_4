# -*- coding: utf-8 -*-

# @Time : 2022/1/5 15:49

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : user_storage_value_todayH.py
'''用户个人已转让、已使用、转让中按小时统计'''
import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
from datetime import timedelta,date
from functools import reduce
from analyzeserver.common import *
import time


# 已转让数量与价值
def transferred_count_and_value():
    try:
        # 数据库连接
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh:
            return False, message['10002']
        # 读取持有表
        hold_table_type_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        hold_df_list = []
        for hold_table_type in hold_table_type_list:
            logger.info(hold_table_type)
            # # 第一次
            # pretty_hold_sql = '''
            # select if(hold_phone,hold_phone,hold_user_id) hold_phone, sell_order_sn order_sn, date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d %%H') day_time
            # from lh_pretty_hold_%s
            # where del_flag=0 and `status`=3 and date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d') = current_date
            # and date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d %%H:%%i:%%s') <= now()
            # ''' % hold_table_type
            # 更新
            pretty_hold_sql = '''
                select if(hold_phone,hold_phone,hold_user_id) hold_phone, sell_order_sn order_sn, date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d %%H') day_time
                from lh_pretty_hold_%s
                where del_flag=0 and `status`=3 and date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d %%H') = date_format(date_sub(now(), interval 1 hour), '%%Y-%%m-%%d %%H')
            ''' % hold_table_type
            hold_df = pd.read_sql(pretty_hold_sql, conn_lh)
            hold_df_list.append(hold_df)
        hold_all_df = pd.concat(hold_df_list, axis=0)
        # 订单表
        order_sql = '''
            select order_sn, total_price transferred_price
            from lh_pretty_client.lh_order
            where del_flag=0
        '''
        lh_order_df = pd.read_sql(order_sql, conn_lh)
        # 订单为空
        hold_all_df_na = hold_all_df.loc[hold_all_df['order_sn'].isna(), ['hold_phone', 'day_time']]
        hold_all_df_notna = hold_all_df.loc[hold_all_df['order_sn'].notna(), :]

        # 订单不为空数据去重
        hold_all_df_notna = pd.DataFrame(
            hold_all_df_notna.groupby(['day_time', 'hold_phone', 'order_sn'])['hold_phone'].count()).rename(
            columns={"hold_phone": "transferred_count"}).reset_index()
        # 合并订单不为空数据
        hold_all_df_notna = hold_all_df_notna.merge(lh_order_df, how='left', on='order_sn')
        hold_all_df_notna.drop('order_sn', axis=1, inplace=True)

        # 订单为价格填充为19，再根据日期和手机号进行分组统计
        hold_all_df_na['total_price'] = 19
        hold_all_df_na = hold_all_df_na.groupby(['day_time', 'hold_phone']).agg(
            {"hold_phone": "count", "total_price": "sum"}).rename(
            columns={"hold_phone": "transferred_count", "total_price": "transferred_price"}).reset_index()

        # 合并订单为空与不为空数据
        fina_hold_all_df = pd.concat([hold_all_df_na, hold_all_df_notna], axis=0)

        fina_hold_all_df = fina_hold_all_df.groupby(['day_time', 'hold_phone'])[
            'transferred_count', 'transferred_price'].sum().reset_index()
        return True, fina_hold_all_df
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        try:
            conn_lh.close()
        except:
            pass

# 发布对应转让中
def public_lh():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        sql = '''
        select day_time,hold_phone,sum(public_count) public_count,sum(public_price) public_price from (select DATE_FORMAT(create_time,"%Y-%m-%d %H") day_time,sell_phone hold_phone, sum(count) public_count,sum(total_price) public_price from lh_sell where del_flag = 0 and `status` != 1 group by day_time, hold_phone
        union all
        select DATE_FORMAT(lsrd.update_time,"%Y-%m-%d %H") day_time,lsr.retail_user_phone hold_phone,count(*) public_count,sum(lsrd.unit_price) public_price from lh_sell_retail lsr left join lh_sell_retail_detail lsrd
        on lsr.id = lsrd.retail_id where lsr.del_flag = 0 and lsrd.retail_status != 1
        group by day_time,hold_phone ) t group by day_time,hold_phone having day_time = date_format(date_sub(now(), interval 1 hour),"%Y-%m-%d %H") order by day_time desc
        '''
        public_data = pd.read_sql(sql,conn_lh)
        return True,public_data
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()

# 已使用
def use_lh():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        sql = '''
        select DATE_FORMAT(b.statistic_time,"%Y-%m-%d %H") day_time,hold_phone,sum(unit_price) use_total_price,count(*) use_count from (
        (
        select if(hold_0.hold_phone,hold_0.hold_phone,hold_0.hold_user_id) hold_phone,if(hold_0.unit_price,hold_0.unit_price,0) unit_price,hold_0.pretty_id pretty_id from lh_pretty_hold_0 hold_0 where hold_0.del_flag = 0 and (hold_0.`status` = 1 or hold_0.is_open_vip=1) union all 
        select if(hold_1.hold_phone,hold_1.hold_phone,hold_1.hold_user_id) hold_phone,if(hold_1.unit_price,hold_1.unit_price,0) unit_price,hold_1.pretty_id pretty_id from lh_pretty_hold_1 hold_1 where hold_1.del_flag = 0 and (hold_1.`status` = 1 or hold_1.is_open_vip=1) union all 
        select if(hold_2.hold_phone,hold_2.hold_phone,hold_2.hold_user_id) hold_phone,if(hold_2.unit_price,hold_2.unit_price,0) unit_price,hold_2.pretty_id pretty_id from lh_pretty_hold_2 hold_2 where hold_2.del_flag = 0 and (hold_2.`status` = 1 or hold_2.is_open_vip=1)  union all 
        select if(hold_3.hold_phone,hold_3.hold_phone,hold_3.hold_user_id) hold_phone,if(hold_3.unit_price,hold_3.unit_price,0) unit_price,hold_3.pretty_id pretty_id from lh_pretty_hold_3 hold_3 where hold_3.del_flag = 0 and (hold_3.`status` = 1 or hold_3.is_open_vip=1)  union all 
        select if(hold_4.hold_phone,hold_4.hold_phone,hold_4.hold_user_id) hold_phone,if(hold_4.unit_price,hold_4.unit_price,0) unit_price,hold_4.pretty_id pretty_id from lh_pretty_hold_4 hold_4 where hold_4.del_flag = 0 and (hold_4.`status` = 1 or hold_4.is_open_vip=1)  union all 
        select if(hold_5.hold_phone,hold_5.hold_phone,hold_5.hold_user_id) hold_phone,if(hold_5.unit_price,hold_5.unit_price,0) unit_price,hold_5.pretty_id pretty_id from lh_pretty_hold_5 hold_5 where hold_5.del_flag = 0 and (hold_5.`status` = 1 or hold_5.is_open_vip=1)  union all 
        select if(hold_6.hold_phone,hold_6.hold_phone,hold_6.hold_user_id) hold_phone,if(hold_6.unit_price,hold_6.unit_price,0) unit_price,hold_6.pretty_id pretty_id from lh_pretty_hold_6 hold_6 where hold_6.del_flag = 0 and (hold_6.`status` = 1 or hold_6.is_open_vip=1)  union all 
        select if(hold_7.hold_phone,hold_7.hold_phone,hold_7.hold_user_id) hold_phone,if(hold_7.unit_price,hold_7.unit_price,0) unit_price,hold_7.pretty_id pretty_id from lh_pretty_hold_7 hold_7 where hold_7.del_flag = 0 and (hold_7.`status` = 1 or hold_7.is_open_vip=1)  union all 
        select if(hold_8.hold_phone,hold_8.hold_phone,hold_8.hold_user_id) hold_phone,if(hold_8.unit_price,hold_8.unit_price,0) unit_price,hold_8.pretty_id pretty_id from lh_pretty_hold_8 hold_8 where hold_8.del_flag = 0 and (hold_8.`status` = 1 or hold_8.is_open_vip=1)  union all 
        select if(hold_9.hold_phone,hold_9.hold_phone,hold_9.hold_user_id) hold_phone,if(hold_9.unit_price,hold_9.unit_price,0) unit_price,hold_9.pretty_id pretty_id from lh_pretty_hold_9 hold_9 where hold_9.del_flag = 0 and (hold_9.`status` = 1 or hold_9.is_open_vip=1)  union all 
        select if(hold_a.hold_phone,hold_a.hold_phone,hold_a.hold_user_id) hold_phone,if(hold_a.unit_price,hold_a.unit_price,0) unit_price,hold_a.pretty_id pretty_id from lh_pretty_hold_a hold_a where hold_a.del_flag = 0 and (hold_a.`status` = 1 or hold_a.is_open_vip=1)  union all 
        select if(hold_b.hold_phone,hold_b.hold_phone,hold_b.hold_user_id) hold_phone,if(hold_b.unit_price,hold_b.unit_price,0) unit_price,hold_b.pretty_id pretty_id from lh_pretty_hold_b hold_b where hold_b.del_flag = 0 and (hold_b.`status` = 1 or hold_b.is_open_vip=1)  union all 
        select if(hold_c.hold_phone,hold_c.hold_phone,hold_c.hold_user_id) hold_phone,if(hold_c.unit_price,hold_c.unit_price,0) unit_price,hold_c.pretty_id pretty_id from lh_pretty_hold_c hold_c where hold_c.del_flag = 0 and (hold_c.`status` = 1 or hold_c.is_open_vip=1)  union all 
        select if(hold_d.hold_phone,hold_d.hold_phone,hold_d.hold_user_id) hold_phone,if(hold_d.unit_price,hold_d.unit_price,0) unit_price,hold_d.pretty_id pretty_id from lh_pretty_hold_d hold_d where hold_d.del_flag = 0 and (hold_d.`status` = 1 or hold_d.is_open_vip=1)  union all 
        select if(hold_e.hold_phone,hold_e.hold_phone,hold_e.hold_user_id) hold_phone,if(hold_e.unit_price,hold_e.unit_price,0) unit_price,hold_e.pretty_id pretty_id from lh_pretty_hold_e hold_e where hold_e.del_flag = 0 and (hold_e.`status` = 1 or hold_e.is_open_vip=1)  union all 
        select if(hold_f.hold_phone,hold_f.hold_phone,hold_f.hold_user_id) hold_phone,if(hold_f.unit_price,hold_f.unit_price,0) unit_price,hold_f.pretty_id pretty_id from lh_pretty_hold_f hold_f where hold_f.del_flag = 0 and (hold_f.`status` = 1 or hold_f.is_open_vip=1)
        ) a
        left join 
        (select min(use_time) statistic_time,pretty_id  from lh_bind_pretty_log  group by pretty_id) b on a.pretty_id = b.pretty_id
        ) 
        group by day_time,hold_phone
        having day_time = date_format(date_sub(now(), interval 1 hour),"%Y-%m-%d %H")
        '''
        use_data = pd.read_sql(sql,conn_lh)
        return True,use_data
    except :
        logger.info(traceback.format_exc())
        return False,""
    finally:
        conn_lh.close()

# 转让持有总
def tran_hold():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        current_time = datetime.datetime.now()
        ergodic_time = (current_time).strftime("%Y-%m-%d %H")

        #可转让 持有
        sql = '''
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_0 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_1 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_2 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_3 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_4 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_5 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_6 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_7 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_8 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_9 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_a where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_b where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_c where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_d where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_e where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))  union all
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, pretty_type_id, date_format(thaw_time,"%Y-%m-%d %H:%i:%S") thaw_time,date_format(if(pay_time is not null,pay_time,create_time),"%Y-%m-%d %H:%i:%S") pay_time, is_sell, pay_type, date_format(create_time,"%Y-%m-%d %H:%i:%S") create_time, status from lh_pretty_hold_f where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0 and (thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") or create_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H"))
        '''
        data = pd.read_sql(sql,conn_lh)

        logger.info("持有转让数据查询结束")

        # tran 转让
        tran_datas = data[(data["status"] == 0) & (data["is_sell"] == 1) & (data["pay_type"] != 0)]
        # hold持有
        hold_datas = data.loc[:, ['hold_phone', 'pretty_type_id', 'pay_time']]

        # 满足当前时间的可转让数据
        current_tran_datas = tran_datas[tran_datas["thaw_time"] < ergodic_time]
        # 满足当前时间的持有数据
        current_hold_datas = hold_datas[hold_datas['pay_time'] < ergodic_time]
        logger.info(current_hold_datas.shape)
        #匹配当前的价格表
        price_sql = '''select pretty_type_id,max(guide_price) guide_price from lh_config_guide where  del_flag = 0  and "%s">=date group by pretty_type_id ''' % ergodic_time
        price_data = pd.read_sql(price_sql,conn_lh)

        logger.info("价格表读取完成")

        # 转让
        current_tran_datas = pd.merge(current_tran_datas,price_data,on="pretty_type_id",how="left")
        # 找不到的话就19
        current_tran_datas["guide_price"] = current_tran_datas["guide_price"].fillna(19)

        # 持有
        current_hold_datas = current_hold_datas.merge(price_data, how='left', on='pretty_type_id')
        current_hold_datas["guide_price"] = current_hold_datas["guide_price"].fillna(19)

        # 转让
        current_tran_datas["tran_count"] = 0
        transfer_data = current_tran_datas[["hold_phone","guide_price","tran_count"]]
        # transfer_data = transfer_data.groupby('hold_phone')['guide_price'].sum().reset_index()
        transfer_data = transfer_data.groupby("hold_phone").agg({"guide_price": "sum", "tran_count": "count"}).reset_index().rename(columns={"guide_price": "tran_price"})
        transfer_data["day_time"] = (current_time + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H")

        logger.info("转让分组ok")

        # 持有
        current_hold_datas["hold_count"] = 0
        # hold_grouped = current_hold_datas.groupby('hold_phone')['guide_price'].sum().reset_index()
        hold_grouped = current_hold_datas.groupby("hold_phone").agg({"guide_price": "sum", "hold_count": "count"}).reset_index().rename(columns={"guide_price": "hold_price"})
        hold_grouped['day_time'] = (current_time + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H")
        logger.info("持有分组ok")

        logger.info(hold_grouped)
        fina_data = hold_grouped.merge(transfer_data, how='outer', on=['day_time', 'hold_phone'])
        fina_data.fillna(0, inplace=True)
        return True, fina_data
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()

#不可转让
def no_tran_lh():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        end_time = datetime.datetime.now()
        # end_time = datetime.datetime.now()
        ergodic_time = end_time.strftime("%Y-%m-%d")

        # 可转让 持有
        sql = '''
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_0 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_0 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all 
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_1 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_1 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_2 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_2 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_3 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_3 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_4 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_4 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_5 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_5 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_6 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_6 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_7 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_7 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_8 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_8 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_9 WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_9 WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_a WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_a WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_b WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_b WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_c WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_c WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_d WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_d WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_e WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_e WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) union all
            SELECT if(hold_phone,hold_phone,hold_user_id) hold_phone,pretty_id,pretty_type_id,if(update_time,update_time,create_time) update_time FROM lh_pretty_hold_f WHERE del_flag=0 AND is_open_vip=0 AND STATUS=0 and pretty_id not in (SELECT pretty_id FROM lh_pretty_hold_f WHERE STATUS=0 AND is_open_vip=0 AND thaw_time<=date_format(date_sub(now(), interval 0 hour),"%Y-%m-%d %H") AND del_flag=0 AND is_sell=1 AND pay_type !=0) 
        '''
        no_tran_data = pd.read_sql(sql, conn_lh)

        logger.info("不可转数据ok")

        # 满足当前时间的可转让数据
        # current_no_tran_datas = no_tran_data[no_tran_data["update_time"] <= ergodic_time]

        # 直接等于当前的
        current_no_tran_datas = no_tran_data
        # 匹配当前的价格表
        price_sql = '''select pretty_type_id,max(guide_price) guide_price from lh_config_guide where  del_flag = 0  and "%s">=date group by pretty_type_id ''' % ergodic_time
        price_data = pd.read_sql(price_sql, conn_lh)
        logger.info("价格表ok")
        # 转让
        current_no_tran_datas = pd.merge(current_no_tran_datas, price_data, on="pretty_type_id", how="left")
        # 找不到的话就19
        current_no_tran_datas["guide_price"] = current_no_tran_datas["guide_price"].fillna(19)

        current_no_tran_datas["no_tran_count"] = 0
        # 转让
        no_tran_datas = current_no_tran_datas[["hold_phone", "guide_price","no_tran_count"]]
        no_tran_datas = no_tran_datas.groupby("hold_phone").agg({"guide_price": "sum", "no_tran_count": "count"}).reset_index()

        logger.info("不可转分组ok")
        # no_tran_datas["day_time"] = ergodic_time
        no_tran_datas["day_time"] = (end_time + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d %H")
        no_tran_datas.rename(columns={"guide_price": "no_tran_price"}, inplace=True)
        return True,no_tran_datas
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()

#用户信息
def user_mes():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        sql = '''
        select if(`name`,`name`,nickname) nickname,phone hold_phone,unionid,operate_id, operatename,leader,bus_phone leader_phone,leader_unionid,parentid, parent_phone from crm_user where del_flag = 0 and phone != "" and phone is not null
        '''
        user_data = pd.read_sql(sql,conn_analyze)
        return True,user_data
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_analyze.close()

def delete_yes_data():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_analyze = conn_analyze.cursor()
        delete_sql = '''delete from user_storage_value_hour where date_format(day_time, "%Y-%m-%d") != CURRENT_DATE'''
        cursor_analyze.execute(delete_sql)
        conn_analyze.commit()
        logger.info("删除成功了")
        return True
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_analyze.close()

if __name__ == "__main__":
    run_start = time.time()
    # 汇总数据
    while True:
        df_list = []
        # 用户信息
        user_mes_result = user_mes()
        if not user_mes_result[0]:
            logger.info('用户信息获取失败')
            logger.info(user_mes_result[1])
            break
        logger.info('user_mes_shape：')
        logger.info(user_mes_result[1].shape)
        # 已转让
        transferred_result = transferred_count_and_value()
        if not transferred_result[0]:
            logger.info('已转让数量与价值获取失败')
            logger.info(transferred_result[1])
            break
        logger.info('transferred_shape：')
        logger.info(transferred_result[1].shape)
        df_list.append(transferred_result[1])
        # 发布
        public_result = public_lh()
        if not public_result[0]:
            logger.info('发布获取失败')
            logger.info(public_result[1])
            break
        logger.info('public_shape：')
        logger.info(public_result[1].shape)
        df_list.append(public_result[1])
        # 已使用
        use_result = use_lh()
        if not use_result[0]:
            logger.info('已使用获取失败')
            logger.info(use_result[1])
            break
        logger.info('use_shape：')
        logger.info(use_result[1].shape)
        df_list.append(use_result[1])
        # 转让持有
        tran_hold_result = tran_hold()
        if not tran_hold_result[0]:
            logger.info('已使用获取失败')
            logger.info(tran_hold_result[1])
            break
        logger.info('tran_hold_shape：')
        logger.info(tran_hold_result[1].shape)
        df_list.append(tran_hold_result[1])
        # 不可转让
        no_tran_lh_result = no_tran_lh()
        if not no_tran_lh_result[0]:
            logger.info('不可转让获取失败')
            logger.info(no_tran_lh_result[1])
            break
        logger.info('no_tran_lh_shape：')
        logger.info(no_tran_lh_result[1].shape)
        df_list.append(no_tran_lh_result[1])
        df_merge = reduce(lambda left, right: pd.merge(left, right, how='outer', on=['day_time', 'hold_phone']), df_list)
        logger.info('df_merege_shape：')
        logger.info(df_merge.shape)
        # 数据为0 填充
        temp = df_merge.isnull().any()
        is_null_df = pd.DataFrame(data={"colnames": temp.index, "isnulls": temp.values})
        # 为空的列名
        null_columns_list = is_null_df.loc[is_null_df['isnulls'] == True, 'colnames'].tolist()
        for null_columns in null_columns_list:
            df_merge[null_columns].fillna(0, inplace=True)
        df_merge = df_merge.merge(user_mes_result[1], how='left', on='hold_phone')
        logger.info('fina_df_shape：')
        logger.info(df_merge.shape)
        # 数据入库
        logger.info('写入数据')
        conn_analyze = pd_conn(analyze_mysql_conf)
        df_merge.to_sql('user_storage_value_hour', con=conn_analyze, if_exists="append", index=False)
        run_end = time.time()
        logger.info(run_start - run_end)
        break

    delete_yes_data()
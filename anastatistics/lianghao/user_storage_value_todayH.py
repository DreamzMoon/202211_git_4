# -*- coding: utf-8 -*-

# @Time : 2022/1/5 15:49

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : user_storage_value_todayH.py
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
            # pretty_hold_sql = '''
            #     select hold_phone, sell_order_sn order_sn, date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d %%H:%%i:%%s') day_time from lh_pretty_hold_%s
            #     where del_flag=0 and `status`=3 and date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d %%H:%%i:%%s') = date_sub(current_date, interval 0 day)
            # ''' % hold_table_type
            pretty_hold_sql = '''
            select if(hold_phone,hold_phone,hold_user_id) hold_phone, sell_order_sn order_sn, date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d') day_time from lh_pretty_hold_%s
where del_flag=0 and `status`=3 and date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d') = current_date and date_format(if(update_time!='8888-08-08',update_time,create_time), '%%Y-%%m-%%d %%H:%%i:%%s') <= now()
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
        select day_time,hold_phone,sum(public_count) public_count,sum(public_price) public_price from (select DATE_FORMAT(create_time,"%Y-%m-%d") day_time,sell_phone hold_phone, sum(count) public_count,sum(total_price) public_price from lh_sell where del_flag = 0 and `status` = 0 group by day_time, hold_phone
        union all
        select DATE_FORMAT(lsrd.update_time,"%Y-%m-%d") day_time,lsr.retail_user_phone hold_phone,count(*) public_count,sum(lsrd.unit_price) public_price from lh_sell_retail lsr left join lh_sell_retail_detail lsrd
        on lsr.id = lsrd.retail_id where lsr.del_flag = 0 and lsrd.retail_status = 0
        group by day_time,hold_phone ) t group by day_time,hold_phone having day_time = date_sub(current_date, interval 0 day) order by day_time desc
        '''
        # day_time = date_sub(current_date, interval 1 day)
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
        select DATE_FORMAT(b.statistic_time,"%Y-%m-%d") day_time,hold_phone,sum(unit_price) use_total_price,count(*) use_count from (
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
        having day_time = date_sub(current_date, interval 0 day)
        '''
        # having day_time = date_sub(current_date, interval 1 day)
        use_data = pd.read_sql(sql,conn_lh)
        return True,use_data
    except :
        logger.info(traceback.format_exc())
        return False,""
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
        delete_sql = '''delete from user_storage_value_today where day_time != CURRENT_DATE'''
        cursor_analyze.execute(delete_sql)
        conn_analyze.commit()
        logger.info("删除成功了")
        return True
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_analyze.close()


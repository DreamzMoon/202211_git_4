# -*- coding: utf-8 -*-
# @Time : 2021/12/15  10:18
# @Author : shihong
# @File : .py
# --------------------------------------

import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback
import pandas as pd
from analyzeserver.common import *
import numpy as np
import time


# 持有数量与持有价值
def hold_count_and_value():
    try:
        start_time = time.time()
        # 数据库连接
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh:
            return False, message['10002']
        # 读取持有表
        hold_table_type_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        hold_df_list = []
        for hold_table_type in hold_table_type_list:
            print(hold_table_type)
            pretty_hold_sql = '''
                select hold_phone, sell_order_sn order_sn, date_format(update_time, '%%Y-%%m-%%d') day_time from lh_pretty_hold_%s where del_flag=0 and `status`=3
            ''' % hold_table_type
            hold_df = pd.read_sql(pretty_hold_sql, conn_lh)
            hold_df_list.append(hold_df)
        hold_all_df = pd.concat(hold_df_list, axis=0)
        # 订单表
        order_sql = '''
            select order_sn, sum(count) pretty_count, sum(total_price) total_price, date_format(create_time, '%Y-%m-%d') day_time
            from lh_pretty_client.lh_order
            where del_flag=0
            group by day_time, order_sn
        '''
        lh_order_df = pd.read_sql(order_sql, conn_lh)
        # 有订单号的数据根据订单表匹配day_time
        hold_all_df_notna = hold_all_df.loc[hold_all_df['order_sn'].notna(), ['hold_phone', 'order_sn']]
        # 订单号不为空进行分组
        hold_all_df_notna = pd.DataFrame(
        hold_all_df_notna.groupby(['hold_phone', 'order_sn'])['hold_phone'].count()).rename(
        columns={"hold_phone": "transferred"}).reset_index()
        hold_all_df_notna = hold_all_df_notna.merge(lh_order_df, how='left', on='order_sn')

        # 没有订单号的数据如果没有时间或者时间为8888的数据则使用最早的填充
        hold_all_df_na = hold_all_df.loc[hold_all_df['order_sn'].isna(), ['hold_phone', 'day_time']]
        # 将8888-08-08 替换成空
        hold_all_df_na.loc[hold_all_df_na['day_time'] == '8888-08-08', 'day_time'] = None

        # 取出总表时间不为空每个用户最早的时间
        day_time_notna = hold_all_df.loc[
            (hold_all_df['day_time'].notna()) & (hold_all_df['day_time'] != '8888-08-08'), ['hold_phone', 'day_time']]
        day_time_notna['day_time'] = pd.to_datetime(day_time_notna['day_time'])
        min_day_time = pd.DataFrame(day_time_notna.groupby('hold_phone')['day_time'].min()).reset_index()
        min_day_time['day_time'] = min_day_time['day_time'].dt.strftime('%Y-%m-%d')

        # 订单号为空中时间为空与不为空
        hold_all_df_na_day_time_na = hold_all_df_na[hold_all_df_na['day_time'].isna()]
        hold_all_df_na_day_time_notna = hold_all_df_na[hold_all_df_na['day_time'].notna()]
        hold_all_df_na_day_time_na.drop('day_time', axis=1, inplace=True)
        hold_all_df_na_day_time_na = hold_all_df_na_day_time_na.merge(min_day_time, how='left', on='hold_phone')

        # 用户持有最早时间-->用于对订单为空，时间为空或8888-08-08进行填充
        early_time = pd.to_datetime(
            hold_all_df[(hold_all_df['day_time'].notna()) & (hold_all_df['day_time'] != '8888-08-08')][
                'day_time']).min().strftime('%Y-%m-%d')

        # 填充时间，合并为空与非空表
        hold_all_df_na_day_time_na.loc[hold_all_df_na_day_time_na['day_time'].isna(), 'day_time'] = early_time

        hold_all_df_na = pd.concat([hold_all_df_na_day_time_na, hold_all_df_na_day_time_notna], axis=0)

        hold_all_df_na = pd.DataFrame(hold_all_df_na.groupby(['day_time', 'hold_phone'])['hold_phone'].count())
        hold_all_df_na = hold_all_df_na.rename(columns={"hold_phone": "transferred"}).reset_index()

        # 合并订单为空与非空表
        fina_hold_all_df = pd.concat([hold_all_df_na, hold_all_df_notna], axis=0, ignore_index=True)
        fina_hold_all_df = fina_hold_all_df.groupby(['day_time', 'hold_phone'])[
            'transferred', 'total_price'].sum().reset_index()
        end_time = time.time()
        logger.info(end_time - start_time)
        return True, fina_hold_all_df
    except Exception as e:
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
        group by day_time,hold_phone ) t group by day_time,hold_phone having day_time != current_date order by day_time desc
        '''
        public_data = pd.read_sql(sql,conn_lh)
        return True,public_data
    except:
        return False,""
    finally:
        conn_lh.close()

def use_lh():
    try:
        conn_lh = direct_get_conn()
        sql = '''
                select DATE_FORMAT(b.statistic_time,"%Y-%m-%d") day_time,hold_phone,sum(unit_price) total_price from (
        (
        select hold_0.hold_phone hold_phone,if(hold_0.unit_price,hold_0.unit_price,0) unit_price,hold_0.pretty_id pretty_id from lh_pretty_hold_0 hold_0 where hold_0.del_flag = 0 and (hold_0.`status` = 1 or hold_0.is_open_vip=1) union all 
        select hold_1.hold_phone hold_phone,if(hold_1.unit_price,hold_1.unit_price,0) unit_price,hold_1.pretty_id pretty_id from lh_pretty_hold_1 hold_1 where hold_1.del_flag = 0 and (hold_1.`status` = 1 or hold_1.is_open_vip=1) union all 
        select hold_2.hold_phone hold_phone,if(hold_2.unit_price,hold_2.unit_price,0) unit_price,hold_2.pretty_id pretty_id from lh_pretty_hold_2 hold_2 where hold_2.del_flag = 0 and (hold_2.`status` = 1 or hold_2.is_open_vip=1)  union all 
        select hold_3.hold_phone hold_phone,if(hold_3.unit_price,hold_3.unit_price,0) unit_price,hold_3.pretty_id pretty_id from lh_pretty_hold_3 hold_3 where hold_3.del_flag = 0 and (hold_3.`status` = 1 or hold_3.is_open_vip=1)  union all 
        select hold_4.hold_phone hold_phone,if(hold_4.unit_price,hold_4.unit_price,0) unit_price,hold_4.pretty_id pretty_id from lh_pretty_hold_4 hold_4 where hold_4.del_flag = 0 and (hold_4.`status` = 1 or hold_4.is_open_vip=1)  union all 
        select hold_5.hold_phone hold_phone,if(hold_5.unit_price,hold_5.unit_price,0) unit_price,hold_5.pretty_id pretty_id from lh_pretty_hold_5 hold_5 where hold_5.del_flag = 0 and (hold_5.`status` = 1 or hold_5.is_open_vip=1)  union all 
        select hold_6.hold_phone hold_phone,if(hold_6.unit_price,hold_6.unit_price,0) unit_price,hold_6.pretty_id pretty_id from lh_pretty_hold_6 hold_6 where hold_6.del_flag = 0 and (hold_6.`status` = 1 or hold_6.is_open_vip=1)  union all 
        select hold_7.hold_phone hold_phone,if(hold_7.unit_price,hold_7.unit_price,0) unit_price,hold_7.pretty_id pretty_id from lh_pretty_hold_7 hold_7 where hold_7.del_flag = 0 and (hold_7.`status` = 1 or hold_7.is_open_vip=1)  union all 
        select hold_8.hold_phone hold_phone,if(hold_8.unit_price,hold_8.unit_price,0) unit_price,hold_8.pretty_id pretty_id from lh_pretty_hold_8 hold_8 where hold_8.del_flag = 0 and (hold_8.`status` = 1 or hold_8.is_open_vip=1)  union all 
        select hold_9.hold_phone hold_phone,if(hold_9.unit_price,hold_9.unit_price,0) unit_price,hold_9.pretty_id pretty_id from lh_pretty_hold_9 hold_9 where hold_9.del_flag = 0 and (hold_9.`status` = 1 or hold_9.is_open_vip=1)  union all 
        select hold_a.hold_phone hold_phone,if(hold_a.unit_price,hold_a.unit_price,0) unit_price,hold_a.pretty_id pretty_id from lh_pretty_hold_a hold_a where hold_a.del_flag = 0 and (hold_a.`status` = 1 or hold_a.is_open_vip=1)  union all 
        select hold_b.hold_phone hold_phone,if(hold_b.unit_price,hold_b.unit_price,0) unit_price,hold_b.pretty_id pretty_id from lh_pretty_hold_b hold_b where hold_b.del_flag = 0 and (hold_b.`status` = 1 or hold_b.is_open_vip=1)  union all 
        select hold_c.hold_phone hold_phone,if(hold_c.unit_price,hold_c.unit_price,0) unit_price,hold_c.pretty_id pretty_id from lh_pretty_hold_c hold_c where hold_c.del_flag = 0 and (hold_c.`status` = 1 or hold_c.is_open_vip=1)  union all 
        select hold_d.hold_phone hold_phone,if(hold_d.unit_price,hold_d.unit_price,0) unit_price,hold_d.pretty_id pretty_id from lh_pretty_hold_d hold_d where hold_d.del_flag = 0 and (hold_d.`status` = 1 or hold_d.is_open_vip=1)  union all 
        select hold_e.hold_phone hold_phone,if(hold_e.unit_price,hold_e.unit_price,0) unit_price,hold_e.pretty_id pretty_id from lh_pretty_hold_e hold_e where hold_e.del_flag = 0 and (hold_e.`status` = 1 or hold_e.is_open_vip=1)  union all 
        select hold_f.hold_phone hold_phone,if(hold_f.unit_price,hold_f.unit_price,0) unit_price,hold_f.pretty_id pretty_id from lh_pretty_hold_f hold_f where hold_f.del_flag = 0 and (hold_f.`status` = 1 or hold_f.is_open_vip=1)
        ) a
        left join 
        (select min(use_time) statistic_time,pretty_id  from lh_bind_pretty_log  group by pretty_id) b on a.pretty_id = b.pretty_id
        ) 
        group by day_time,hold_phone
        order by day_time desc
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
        #判断开始时间 脚本从开始时间--结束时间
        time_sql = '''select min(create_time) start from lh_order where del_flag = 0'''
        start_time = pd.read_sql(time_sql,conn_lh)["start"][0]
        end_time = datetime.datetime.now().strftime("%Y-%m-%d")
        ergodic_time = start_time.strftime("%Y-%m-%d")

        time_sql = '''select min(date) price_start_time from lh_config_guide where del_flag = 0'''
        price_start_time = pd.read_sql(time_sql,conn_lh)["price_start_time"][0]
        price_start_time = price_start_time

        #可转让 持有
        sql = '''
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_0 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0  union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_1 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_2 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_3 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_4 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_5 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_6 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_7 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_8 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_9 where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_a where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_b where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_c where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_d where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_e where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0    union all
        select hold_phone,pretty_type_id,date_format(thaw_time,"%Y-%m-%d") thaw_time,is_sell,pay_type,status from lh_pretty_hold_f where del_flag=0 and `status` in (0,1,2) and is_open_vip = 0
        '''
        data = pd.read_sql(sql,conn_lh)

        #tran 转让
        tran_datas = data[(data["status"] == 0) & (data["is_sell"] == 1) & (data["pay_type"] != 0)]
        days = 1
        last_tran = []
        while ergodic_time != end_time:
            # 满足当前时间的可转让数据
            current_tran_datas = tran_datas[tran_datas["thaw_time"] <= ergodic_time]
            #查价格前先判断当前时间有没有到指导价的时间 小于 按照19算
            if price_start_time > ergodic_time:
                current_tran_datas["guide_price"] = 19
            else:
                #匹配当前的价格表
                price_sql = '''select pretty_type_id,max(guide_price) guide_price from lh_config_guide where  del_flag = 0  and "%s">=date group by pretty_type_id ''' %ergodic_time
                price_data = pd.read_sql(price_sql,conn_lh)
                current_tran_datas = pd.merge(current_tran_datas,price_data,on="pretty_type_id",how="left")
                # 找不到的话就19
                current_tran_datas["guide_price"] = current_tran_datas["guide_price"].fillna(19)
            transfer_data = current_tran_datas[["hold_phone","guide_price"]]
            transfer_data = transfer_data.groupby(["hold_phone"])['guide_price'].sum().reset_index()
            transfer_data["day_time"] = ergodic_time
            ergodic_time = (start_time + datetime.timedelta(days=days)).strftime("%Y-%m-%d")
            days += 1
            last_tran.append(transfer_data)
        last_tran_data = pd.concat(last_tran,axis=0,ignore_index=True)

    except:
        logger.info(traceback.format_exc())
        return False, ""
    finally:
        conn_lh.close()



if __name__ == "__main__":
    # logger.info(public_lh())
    use_lh()
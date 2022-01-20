# -*- coding: utf-8 -*-

# @Time : 2022/1/20 10:23

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : user_daily_order_82.py
import sys, os, time
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from functools import reduce
from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback

#每天凌晨02分跑

# 数据库连接
def user_daily_order_data(mode='update'):
    try:
        conn_an = pd_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh or not conn_an:
            return False, '数据库连接错误'

        # 用户数据
        user_info_sql = '''
            select unionid, parentid, phone, parent_phone, name, nickname, operate_id, operatename, leader, bus_phone leader_phone, leader_unionid
            from crm_user
            where phone is not null or phone != '' and del_flag=0
        '''
        user_info_data = pd.read_sql(user_info_sql, conn_an)
        logger.info(user_info_data.shape)

        data_df_list = []
        if mode == 'create':
            # 采购数据
            buy_sql = '''
                select date_format(create_time, "%Y-%m-%d") day_time, phone, count(*) buy_count, sum(count) buy_pretty_count, sum(total_price) buy_total_price
                from lh_pretty_client.le_order
                where del_flag=0 and type = 4 and `status`=1 and (phone is not null or phone != '') and date_format(create_time, "%Y-%m-%d") < CURDATE() and date_format(create_time, "%Y-%m-%d") >= "2021-11-28"
                group by day_time, phone
            '''
            # 出售收据
            sell_sql = '''
                select date_format(create_time, "%Y-%m-%d") day_time, sell_phone phone, count(*) sell_count, sum(count) sell_pretty_count, sum(total_price) sell_total_price,
                sum(total_price) - sum(sell_fee) truth_price, sum(sell_fee) sell_fee
                from lh_pretty_client.le_order
                where del_flag=0 and type = 4 and `status`=1 and (sell_phone is not null or sell_phone != '') and date_format(create_time, "%Y-%m-%d") < CURDATE() and date_format(create_time, "%Y-%m-%d") >= "2021-11-28"
                group by day_time, sell_phone
            '''
            # 发布数据
            publish_sql = '''
                select date_format(create_time, "%Y-%m-%d") day_time, sell_phone phone, count(*) publish_count, sum(count) publish_pretty_count, sum(total_price) publish_total_price
                from lh_pretty_client.le_second_hand_sell
                where del_flag=0 and `status`!=1 and (sell_phone is not null or sell_phone != '') and date_format(create_time, "%Y-%m-%d") < CURDATE() and date_format(create_time, "%Y-%m-%d") >= "2021-11-28"
                group by day_time, sell_phone
            '''
        elif mode == 'update':
            # 采购数据
            buy_sql = '''
                select date_format(create_time, "%Y-%m-%d") day_time, phone, count(*) buy_count, sum(count) buy_pretty_count, sum(total_price) buy_total_price
                from lh_pretty_client.le_order
                where del_flag=0 and type = 4 and `status`=1 and (phone is not null or phone != '') and date_format(create_time, "%Y-%m-%d") = date_sub(curdate(), interval 1 day)
                group by day_time, phone
            '''
            # 出售收据
            sell_sql = '''
                select date_format(create_time, "%Y-%m-%d") day_time, sell_phone phone, count(*) sell_count, sum(count) sell_pretty_count, sum(total_price) sell_total_price,
                sum(total_price) - sum(sell_fee) truth_price, sum(sell_fee) sell_fee
                from lh_pretty_client.le_order
                where del_flag=0 and type = 4 and `status`=1 and (sell_phone is not null or sell_phone != '') and date_format(create_time, "%Y-%m-%d") = date_sub(curdate(), interval 1 day)
                group by day_time, sell_phone
            '''
            # 发布数据
            publish_sql = '''
                select date_format(create_time, "%Y-%m-%d") day_time, sell_phone phone, count(*) publish_count, sum(count) publish_pretty_count, sum(total_price) publish_total_price
                from lh_pretty_client.le_second_hand_sell
                where del_flag=0 and `status`!=1 and (sell_phone is not null or sell_phone != '') and date_format(create_time, "%Y-%m-%d") = date_sub(curdate(), interval 1 day)
                group by day_time, sell_phone
            '''
        else:
            return False, 'mode错误'

        order_data = pd.read_sql(buy_sql, conn_lh)
        data_df_list.append(order_data)

        sell_data = pd.read_sql(sell_sql, conn_lh)
        data_df_list.append(sell_data)

        publish_data = pd.read_sql(publish_sql, conn_lh)
        data_df_list.append(publish_data)

        # 合并靓号统计数据
        df_merge = reduce(lambda left, right: pd.merge(left, right, how='outer', on=['day_time', 'phone']), data_df_list)
        df_merge.fillna(0, inplace=True)

        # 合并用户数据
        fina_df = df_merge.merge(user_info_data, how='left', on='phone')
        logger.info(fina_df.shape)

        logger.info('插入数据')
        fina_df.to_sql('user_daily_order_82', con=conn_an, if_exists="append", index=False)
        return True, '插入成功'
    except Exception as e:
        logger.error(traceback.format_exc())
        conn_lh.close()
        return False, e


if __name__ == '__main__':
    result = user_daily_order_data('update')
    if not result:
        logger.error(result[1])
    else:
        logger.info(result[1])

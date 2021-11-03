# -*- coding: utf-8 -*-

# @Time : 2021/11/3 11:13

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : common.py

import sys
sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime

#通过禄可运营中心查询对应的手机号码
def get_lukebus_phone(bus_lists):
    try:
        phone_lists = []
        conn_crm = direct_get_conn(crm_mysql_conf)
        crm_cursor = conn_crm.cursor()
        sql = '''select * from luke_lukebus.operationcenter where find_in_set(operatename,%s)'''
        crm_cursor.execute(sql, (",".join(bus_lists)))
        operate_datas = crm_cursor.fetchall()
        logger.info("operate_datas:%s" % operate_datas)
        filter_phone_lists = []
        all_phone_lists = []
        for operate_data in operate_datas:
            below_person_sql = '''
            select a.*,b.operatename from 
            (WITH RECURSIVE temp as (
                SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                UNION ALL
                SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id)
            SELECT * FROM temp
            )a left join luke_lukebus.operationcenter b
            on a.id = b.unionid where a.phone != ""
            '''
            logger.info(operate_data)
            crm_cursor.execute(below_person_sql, operate_data["telephone"])
            below_datas = crm_cursor.fetchall()
            logger.info(len(below_datas))
            # 找运营中心
            other_operatecenter_phone_list = []


            # 直接下级的运营中心
            all_phone_lists.append(below_datas[0]["phone"])

            for i in range(0, len(below_datas)):
                if i == 0:
                    continue
                if below_datas[i]["operatename"]:
                    other_operatecenter_phone_list.append(below_datas[i]["phone"])
                all_phone_lists.append(below_datas[i]["phone"])

            logger.info("other_operatecenter_phone_list:%s" % other_operatecenter_phone_list)
            # 对这些手机号码进行下级查询
            for center_phone in other_operatecenter_phone_list:
                logger.info(center_phone)
                if center_phone in filter_phone_lists:
                    continue
                filter_sql = '''
                select a.*,b.operatename from 
                (WITH RECURSIVE temp as (
                    SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                    UNION ALL
                    SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.pid = temp.id)
                SELECT * FROM temp
                )a left join luke_lukebus.operationcenter b
                on a.id = b.unionid 
                    where a.phone != "" and phone != %s
                '''
                crm_cursor.execute(filter_sql, (center_phone, center_phone))
                filter_data = crm_cursor.fetchall()
                for k in range(0, len(filter_data)):
                    filter_phone_lists.append(filter_data[k]["phone"])

        phone_lists = list(set(all_phone_lists) - set(filter_phone_lists))
        logger.info(len(phone_lists))
        args_phone_lists = ",".join(phone_lists)

        return 1,args_phone_lists
    except Exception as e:
        logger.exception(e)
        return 0,e
    finally:
        conn_crm.close()

# 通过运营中心手机号查询对应运营中心数据
def get_operationcenter_data(cursor, operate_df, user_order_df):
    '''
    :param cursor: crm数据库游标
    :param operate_df:  运营中心DataFrame
    :param user_order_df: 用户订单DataFrame
    :return:
    '''
    try:
        # 运营中心关系sql
        supervisor_sql = '''
                select a.*,b.operatename,b.crm from 
                (WITH RECURSIVE temp as (
                    SELECT t.id,t.pid,t.phone,t.nickname,t.name FROM luke_sincerechat.user t WHERE phone = %s
                    UNION ALL
                    SELECT t1.id,t1.pid,t1.phone, t1.nickname,t1.name FROM luke_sincerechat.user t1 INNER JOIN temp ON t1.pid = temp.id
                )
                SELECT * FROM temp
                )a left join luke_lukebus.operationcenter b
                on a.id = b.unionid
                '''
        # 运营中心手机号列表
        operate_telephone_list = operate_df['telephone'].to_list()

        fina_center_data_list = []
        for phone in operate_telephone_list:
            cursor.execute(supervisor_sql, phone)
            all_data = cursor.fetchall()
            # 总数据
            all_data = pd.DataFrame(all_data)
            all_data.dropna(subset=['phone'], axis=0, inplace=True)
            all_data_phone = all_data['phone'].tolist()
            # 运营中心名称
            operate_data = operate_df.loc[operate_df['telephone'] == phone, :]
            operatename = operate_data['operatename'].values[0]
            operate_leader_unionid = operate_data['unionid'].values[0]
            operate_leader_name = operate_data['name'].values[0]
            # 子运营中心
            center_phone_list = all_data.loc[all_data['operatename'].notna(), :]['phone'].tolist()
            child_center_phone_list = []
            # 第一级别
            first_child_center = []
            for i in center_phone_list[1:]:
                # 剔除下级的下级运营中心
                if i in child_center_phone_list:
                    continue
                first_child_center.append(i)
                cursor.execute(supervisor_sql, i)
                center_data = cursor.fetchall()
                center_df = pd.DataFrame(center_data)
                center_df.dropna(subset=['phone'], axis=0, inplace=True)
                child_center_phone_list.extend(center_df['phone'].tolist())
            ret = list(set(all_data_phone) - set(child_center_phone_list))
            ret.extend(first_child_center)
            # 靓号数据
            child_df = user_order_df.loc[user_order_df['phone'].isin(ret), :]
            notice_data = {
                'operatename': operatename,  # 运营中心名
                'operate_leader_name': operate_leader_name,  # 运营中心负责人
                'operate_leader_phone': phone,  # 手机号
                'operate_leader_unionid': str(int(operate_leader_unionid)),  # unionID
                'buy_order': int(child_df['buy_order'].sum()),  # 采购订单数量
                'buy_count': int(child_df['buy_count'].sum()),  # 采购靓号数量
                'buy_price': str(round(child_df['buy_price'].sum(), 2)),  # 采购金额
                'publish_total_count': int(child_df['publish_total_count'].sum()),  # 发布靓号
                'publish_sell_count': int(child_df['publish_sell_count'].sum()),  # 发布订单
                'publish_total_price': str(round(child_df['publish_total_price'].sum(), 2)),  # 发布金额
                'sell_order': int(child_df['sell_order'].sum()),  # 出售订单数
                'sell_price': str(round(child_df['sell_price'].sum(), 2)),  # 出售金额
                'sell_count': int(child_df['sell_count'].sum()),  # 出售靓号数
                'true_price': str(round(child_df['true_price'].sum(), 2)),  # 出售时实收金额
                'sell_fee': str(round(child_df['sell_fee'].sum(), 2)),  # 出售手续费
            }
            fina_center_data_list.append(notice_data)
            logger.info(notice_data)
        return True, fina_center_data_list
    except Exception as e:
        return False, e

if __name__ == "__main__":
    result = get_lukebus_phone(["浙江金华永康运营中心","福州高新区测试运营中心，请勿选择"])
    logger.info(result)
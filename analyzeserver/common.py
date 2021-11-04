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
    '''
    :param bus_lists: 传入运营中心的列表
    :return:返回手机号码
    '''
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

# 返回所有用户运营中心
def get_all_user_operationcenter():
    '''

    :param crm_cursor: crm数据库游标。需再调用方法后手动关闭数据库连接
    :return: crm手机不为空的用户对应运营中心
    '''
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return False, '数据库连接失败'
        crm_cursor = conn_crm.cursor()
        operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1'
        crm_cursor.execute(operate_sql)
        operate_data = crm_cursor.fetchall()
        operate_df = pd.DataFrame(operate_data)

        # crm用户数据
        crm_user_sql = 'select id, pid, phone from luke_sincerechat.user where phone is not null'
        crm_cursor.execute(crm_user_sql)
        crm_user_df = pd.DataFrame(crm_cursor.fetchall())

        # 运营中心手机列表
        operate_telephone_list = operate_df['telephone'].to_list()

        # 关系查找ql
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
        child_df_list = []
        for phone in operate_telephone_list:
            # 1、获取运营中心所有下级数据
            crm_cursor.execute(supervisor_sql, phone)
            all_data = crm_cursor.fetchall()
            # 总数据
            all_data = pd.DataFrame(all_data)
            all_data.dropna(subset=['phone'], axis=0, inplace=True)
            all_data_phone = all_data['phone'].tolist()
            # 运营中心名称
            operatename = operate_df.loc[operate_df['telephone'] == phone, 'operatename'].values[0]
            # 子运营中心-->包含本身
            center_phone_list = all_data.loc[all_data['operatename'].notna(), :]['phone'].tolist()
            child_center_phone_list = []  # 子运营中心所有下级
            # 2、得到运营中心下所有归属下级
            first_child_center = []  # 第一级运营中心
            for i in center_phone_list[1:]:
                # 剔除下级的下级运营中心
                if i in child_center_phone_list:
                    continue
                # 排除运营中心重复统计
                #         if i not in center_phone_list:
                #         first_child_center.append(i)
                crm_cursor.execute(supervisor_sql, i)
                center_data = crm_cursor.fetchall()
                center_df = pd.DataFrame(center_data)
                center_df.dropna(subset=['phone'], axis=0, inplace=True)
                child_center_phone_list.extend(center_df['phone'].tolist())
            ret = list(set(all_data_phone) - set(child_center_phone_list))
            #     ret.extend(first_child_center)
            # 3、取得每个运营中心下级df合并
            child_df = crm_user_df.loc[crm_user_df['phone'].isin(ret), :]
            child_df['operatename'] = operatename
            child_df_list.append(child_df)
        # 用户数据拼接
        exist_center_df = pd.concat(child_df_list)
        fina_df = crm_user_df.merge(exist_center_df.loc[:, ['phone', 'operatename']], how='left', on='phone')
        conn_crm.close()
        logger.info('返回用户数据成功')
        return True, fina_df
    except Exception as e:
        logger.info(e)
        return False, e


def user_belong_bus(need_data):
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)

        crm_user_sql = '''select sex,id unionid,pid parentid,phone,nickname from luke_sincerechat.user where phone is not null or phone != ""'''
        crm_user_data = pd.read_sql(crm_user_sql, conn_crm)

        user_data = need_data.merge(crm_user_data, how="left", on="phone")

        phone_list = user_data.to_dict('list')['phone']
        logger.info(len(phone_list))

        # 查运营中心
        all_operate = []
        for pl in phone_list:
            logger.info("phone:%s" % pl)
            pl_op_dict = {}
            operate_sql = '''
                    select a.*,b.operatename,b.crm from 
                    (WITH RECURSIVE temp as (
                            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
                            UNION ALL
                            SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.id = temp.pid
                    )
                    SELECT * FROM temp 
                    )a left join luke_lukebus.operationcenter b
                    on a.id = b.unionid
                    ''' % pl
            operate_data = pd.read_sql(operate_sql, conn_crm)
            logger.info(operate_data)
            logger.info("----------------")

            if len(operate_data) > 0:
                # pandas可以保留排序 取出运营中心不为空的 并且 crm支持等于1的 第一个
                current_operate_data = operate_data[(operate_data["operatename"] != "") & (operate_data["crm"] == 1)].iloc[0, :]
                pl_op_dict["operate_name"] = current_operate_data["operatename"]
                pl_op_dict["phone"] = pl
                all_operate.append(pl_op_dict)
            else:
                pl_op_dict["operate_name"] = ""
                pl_op_dict["phone"] = pl
                all_operate.append(pl_op_dict)
        # logger.info(all_operate)

        all_operate = pd.DataFrame(all_operate)
        # logger.info(all_operate)

        user_data = user_data.merge(all_operate, how="left", on="phone")
        # logger.info(user_data.loc[0])
        last_data = user_data.to_dict("records")
        logger.info(last_data)
        return 1, last_data
    except Exception as e:
        return 0,e
    finally:
        conn_crm.close()

if __name__ == "__main__":
    result = get_lukebus_phone(["四川成都天府新区运营中心"])
    logger.info(result)
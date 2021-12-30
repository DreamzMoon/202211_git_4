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

#通过禄可运营中心查询对应的手机号码 算钱的 包含运营中心底下的人 包含云心中心
def get_lukebus_phone(bus_lists):
    '''
    :param bus_lists: 传入运营中心的列表
    :return:返回手机号码
    '''
    try:
        phone_lists = []
        conn_crm = direct_get_conn(crm_mysql_conf)
        crm_cursor = conn_crm.cursor()
        sql = '''select * from luke_lukebus.operationcenter where find_in_set(operatename,%s) and crm =1 and capacity=1'''
        crm_cursor.execute(sql, (",".join(bus_lists)))
        operate_datas = crm_cursor.fetchall()
        logger.info("operate_datas:%s" % operate_datas)
        filter_phone_lists = []
        all_phone_lists = []
        for operate_data in operate_datas:
            below_person_sql = '''
            select a.*,if (crm =0, Null, b.operatename) operatename from 
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
                select a.*,if (crm =0, Null, b.operatename) operatename from 
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
        if args_phone_lists:
            return 1,args_phone_lists
        else:
            return 0,"暂无数据"
    except Exception as e:
        logger.exception(e)
        return 0,e
    finally:
        conn_crm.close()



#通过禄可运营中心id查询对应的手机号码 不包含运营中心
def get_busphne_by_id(bus_id):
    '''
    :param bus_lists: 传入运营中心的列表
    :return:返回手机号码
    '''
    try:
        logger.info(bus_id)
        phone_lists = []
        # conn_crm = direct_get_conn(crm_mysql_conf)
        # crm_cursor = conn_crm.cursor()
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        analyze_cursor = conn_analyze.cursor()
        sql = '''select * from lh_analyze.operationcenter where id = %s and crm=1 and capacity=1'''
        analyze_cursor.execute(sql, (bus_id))
        operate_datas = analyze_cursor.fetchall()
        logger.info("operate_datas:%s" % operate_datas)
        filter_phone_lists = []
        all_phone_lists = []
        for operate_data in operate_datas:
            below_person_sql = '''
            select a.*,if (crm =0, Null, b.operatename) operatename from 
            (WITH RECURSIVE temp as (
                SELECT t.unionid id,t.parentid pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM lh_analyze.crm_user t WHERE phone = %s
                UNION ALL
                SELECT t.unionid id,t.parentid pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM lh_analyze.crm_user t INNER JOIN temp ON t.parentid = temp.id)
            SELECT * FROM temp
            )a left join lh_analyze.operationcenter b
            on a.id = b.unionid where a.phone != ""
            '''
            analyze_cursor.execute(below_person_sql, operate_data[7])
            below_datas = pd.DataFrame(analyze_cursor.fetchall())
            coll_lists = ["id", "pid", "phone", "nickname", "name", "sex", "status", "operatename"]
            below_datas.columns = coll_lists
            all_data_1 = below_datas[below_datas['phone'] == operate_data[7]]
            all_data_2 = below_datas[below_datas['phone'] != operate_data[7]]
            below_datas = pd.concat([all_data_1, all_data_2], axis=0, ignore_index=True)
            # 找运营中心
            other_operatecenter_phone_list = []


            # 直接下级的运营中心
            all_phone_lists.append(below_datas.loc[0]["phone"])

            for i in range(0, len(below_datas)):
                if i == 0:
                    continue
                if below_datas.loc[i]["operatename"]:
                    other_operatecenter_phone_list.append(below_datas.loc[i]["phone"])
                all_phone_lists.append(below_datas.loc[i]["phone"])

            logger.info("other_operatecenter_phone_list:%s" % other_operatecenter_phone_list)
            # 对这些手机号码进行下级查询
            for center_phone in other_operatecenter_phone_list:
                logger.info(center_phone)
                if center_phone in filter_phone_lists:
                    continue
                filter_sql = '''
                select a.*,if (crm =0, Null, b.operatename) operatename from 
                (WITH RECURSIVE temp as (
                    SELECT t.unionid id,t.parentid pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM lh_analyze.crm_user t WHERE phone = %s
                    UNION ALL
                    SELECT t.unionid id,t.parentid pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM lh_analyze.crm_user t INNER JOIN temp ON t.parentid = temp.id)
                SELECT * FROM temp
                )a left join lh_analyze.operationcenter b
                on a.id = b.unionid 
                where a.phone != ""
                '''
                analyze_cursor.execute(filter_sql, center_phone)
                filter_data = pd.DataFrame(analyze_cursor.fetchall())
                filter_data.columns = coll_lists
                filter_data = filter_data[filter_data['phone'] != center_phone].reset_index(drop=True)
                for k in range(filter_data.shape[0]):
                    filter_phone_lists.append(filter_data.loc[k]["phone"])
        # 不包含底下直属运营中心
        phone_lists = list(set(all_phone_lists) - set(filter_phone_lists) - set(other_operatecenter_phone_list))
        args_phone_lists = ",".join(phone_lists)
        if args_phone_lists:
            return 1,args_phone_lists
        else:
            return 0,"暂无数据"
    except Exception as e:
        logger.exception(e)
        return 0,e
    finally:
        conn_analyze.close()



# 运营中心统计结果数据
def get_notice_data(child_df, operatename, name, phone, unionid, operateid):
    '''

    :param child_df: 运营中心下级DataFrame
    :param operatename: 运营中心名称
    :param name: 运营中心负责人名称
    :param phone: 运营中心负责人手机号
    :param unionid: 运营中心负责人unionid
    :return: 运营中心统计结果数据
    '''
    notice_data = {
        'operatename': operatename,  # 运营中心名
        'operateid': int(operateid),
        'operate_leader_name': name,  # 运营中心负责人
        'operate_leader_phone': phone,  # 手机号
        'operate_leader_unionid': str(int(unionid)),  # unionID
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
    return notice_data



# 订单流水数据处理
def order_and_user_merge(order_df, user_df):
    '''

    :param order_df: 订单表
    :param user_df: 用户表
    :return:
    '''
    try:
        # 买方
        fina_df = order_df.merge(
            user_df.loc[:, ['buyer_unionid', 'buyer_phone', 'parentid', 'buyer_name', 'parent_phone', 'operate_id', 'operatename']], how='left',
            on='buyer_phone')
        # 卖方
        fina_df = fina_df.merge(user_df.loc[:, ['sell_unionid', 'sell_phone', 'sell_name']], how='left',
                                on='sell_phone')
        fina_df.reset_index(drop=True, inplace=True)

        # 类型转换、填补空值
        fina_df['buyer_unionid'] = fina_df['buyer_unionid'].astype(str)
        fina_df['buyer_unionid'] = fina_df['buyer_unionid'].apply(lambda x: del_point(x))
        fina_df['parentid'] = fina_df['parentid'].astype(str)
        fina_df['parentid'] = fina_df['parentid'].apply(lambda x: del_point(x))
        fina_df['sell_unionid'] = fina_df['sell_unionid'].astype(str)
        fina_df['sell_unionid'] = fina_df['sell_unionid'].apply(lambda x: del_point(x))
        fina_df['transfer_type'].fillna(3, inplace=True)
        fina_df['transfer_type'] = (fina_df['transfer_type'].astype(int)).astype(str)
        fina_df['pay_type'] = fina_df['pay_type'].astype(str)

        return True,fina_df
    except Exception as e:
        logger.error(traceback.format_exc())
        return False, '10000'

# 订单数据框匹配
def match_attribute(data_df, request, mode):
    '''

    :param data_df: 需要匹配的DataFrame
    :param request: 请求
    :param mode: order为订单流水， publish为发布出售， publish_data为转卖市场发布
    :return: bool与匹配数据或者错误码
    '''
    try:
        if mode == 'order':
            match_df = data_df.loc[
                ((data_df['buyer_name'].str.contains(request.json['buyer_info'], regex=False)) | (data_df['buyer_phone'].str.contains(request.json['buyer_info'], regex=False)) | (data_df['buyer_unionid'].str.contains(request.json['buyer_info'], regex=False)))  # 购买人信息
                & (data_df['order_sn'].str.contains(request.json['order_sn'], regex=False))  # 订单编号
                & ((data_df['sell_name'].str.contains(request.json['sell_info'], regex=False)) | (data_df['sell_phone'].str.contains(request.json['sell_info'], regex=False)) | (data_df['sell_unionid'].str.contains(request.json['sell_info'], regex=False)))  # 出售人信息
                & (data_df['pay_type'].str.contains(request.json['pay_id'], regex=False))  # 支付类型
                & (data_df['transfer_type'].str.contains(request.json['transfer_id'], regex=False))  # 转让类型
                ]
            if request.json['parent']:
                match_df = match_df.loc[(match_df['parentid']==request.json['parent']) | (match_df['parent_phone']==request.json['parent']), :]  # 归属上级]
            if request.json['start_order_time']:
                match_df = match_df.loc[(match_df['order_time']>=request.json['start_order_time']) & (match_df['order_time']<=(request.json['end_order_time'])), :]
        elif mode == 'publish':
            match_df = data_df.loc[((data_df['sell_name'].str.contains(request.json['sell_info'], regex=False)) | (data_df['sell_phone'].str.contains(request.json['sell_info'], regex=False)) | (data_df['sell_unionid'].str.contains(request.json['sell_info'], regex=False)))  # 出售人信息
                   & (data_df['status'].str.contains(request.json['status'], regex=False))  # 支付类型
                   & (data_df['transfer_type'].str.contains(request.json['transfer_id'], regex=False))  # 转让类型
            , :]
            if request.json['parent']:
                match_df = match_df.loc[(match_df['parentid'] == request.json['parent']) | (match_df['parent_phone'] == request.json['parent']), :]  # 归属上级
            if request.json['start_publish_time']:
                match_df = match_df.loc[(match_df['publish_time'] >= request.json['start_publish_time']) & (match_df['publish_time'] <= request.json['end_publish_time']), :]
            if request.json['start_up_time']:
                match_df = match_df.loc[(match_df['up_time'] >= request.json['start_up_time']) & (match_df['up_time'] <= request.json['end_up_time']), :]
            if request.json['start_sell_time']:
                match_df = match_df.loc[(match_df['sell_time'] >= request.json['start_sell_time']) & (match_df['sell_time'] <= request.json['end_sell_time']), :]
        else:
            match_df = data_df.loc[((data_df['publish_name'].str.contains(request.json['keyword'], regex=False)) | (
                data_df['publish_unionid'].str.contains(request.json['keyword'], regex=False)) | (data_df['publish_phone'].str.contains(request.json['keyword'], regex=False)))]
            if request.json['parent']:
                match_df = match_df.loc[(match_df['parentid'] == request.json['parent']) | (
                        match_df['parent_phone'] == request.json['parent']), :]  # 归属上级
            if request.json['start_first_time']:
                match_df = match_df.loc[(match_df['first_time'] >= request.json['start_first_time']) & (
                        match_df['near_time'] <= (request.json['end_first_time'])), :]
            if request.json['start_near_time']:
                match_df = match_df.loc[(match_df['near_time'] >= request.json['start_near_time']) & (
                        match_df['near_time'] <= (request.json['end_near_time'])), :]
        return True, match_df
    except Exception as e:
        logger.error(traceback.format_exc())
        return False, "10011"


# 根据运营中心id及request返回匹配数据
def if_exist_operate_match_data(fina_df, operateid, request, match_field, mode):
    try:
        # flag_1, child_phone_list = get_operationcenter_child(operateid)
        # if not flag_1:
        #     return False, child_phone_list # 10011
        # part_user_df = fina_df.loc[fina_df[match_field].isin(child_phone_list[:-1]), :].reset_index(drop=True)
        part_user_df = fina_df[fina_df['operate_id'] == operateid]
        flag_3, match_attribute_df = match_attribute(part_user_df, request, mode=mode)
        if not flag_3:
            return False, match_attribute_df # 10011
        # match_attribute_df['operatename'] = child_phone_list[-1]
        if request.json['page'] and request.json['size']:
            start_index = (request.json['page'] - 1) * request.json['size']
            end_index = request.json['page'] * request.json['size']
            match_df = match_attribute_df[start_index:end_index]
        else:
            match_df = match_attribute_df.copy()
        match_df.drop(['parent_phone'], axis=1, inplace=True)
        return True, match_df, match_attribute_df
    except Exception as e:
        logger.error(traceback.format_exc())
        return False, '10000'

# 不存在运营中心时根据request返回匹配数据
def not_exist_operate_match_data(fina_df, request, match_field, mode):
    try:
        # 匹配数据
        flag_1, match_attribute_df = match_attribute(fina_df, request, mode=mode)
        if not flag_1:
            return False, match_attribute_df # 10011
        if request.json['page'] and request.json['size']:
            start_index = (request.json['page'] - 1) * request.json['size']
            end_index = request.json['page'] * request.json['size']
            match_df = match_attribute_df[start_index:end_index]
        else:
            match_df = match_attribute_df.copy()
        # 以1500条数据为界限，以上调用get_all_user_operationcenter,以下直接进行运营中心匹配
        # if cut_data.shape[0] > 1500:
        #     # 调用get_all_user_operationcenter
        #     all_user_operate_result = get_all_user_operationcenter()
        #     if not all_user_operate_result[0]:
        #         return False, "10000"
        #     all_user_operate_result[1].rename(columns={"phone": match_field}, inplace=True)
        #     match_df = cut_data.merge(all_user_operate_result[1].loc[:, [match_field, 'operatename']], how='left',
        #                               on=match_field)
        # else:
        #     # 匹配用户运营中心
        #     flag_2, match_df = match_user_operate(cut_data, field=match_field)
        #     if not flag_2:
        #         return False, match_df # 10011
        match_df.drop(['parent_phone'], axis=1, inplace=True)
        return True, match_df, match_attribute_df
    except Exception as e:
        logger.error(traceback.format_exc())
        return False, '10000'

# 用户首次，二次，最近发布数据
def user_first_second_near_publish(data_df):
    # 判断数据条数。如果为三则三种数据都显示，如果为2只显示首次与最近，如果为一只显示首次
    try:
        # 存储所有时间数据
        publish_info = {}
        # 首次发布数据
        first_time = {}
        # 二次发布数据
        second_time = {}
        # 最近发布数据
        near_time = {}
        publish_mode_data = {
            'publish_time': '',
            'total_price': '',
            'pay_type': '',
            'pretty_type': '',
            'count': ''
        }

        if data_df.shape[0] >= 1:
            first_df = data_df.loc[0, ['count', 'total_price', 'pretty_type', 'create_time', 'pay_type']]
            first_df.fillna('', inplace=True)
            first_time['publish_time'] = first_df['create_time'].strftime('%Y-%m-%d %H:%M:%S')
            first_time['total_price'] = first_df['total_price']
            try:
                first_time['pay_type'] = int(first_df['pay_type'])
            except:
                first_time['pay_type'] = first_df['pay_type']
            first_time['pretty_type'] = first_df['pretty_type']
            first_time['count'] = int(first_df['count'])
        else:
            first_time = publish_mode_data
        publish_info['first_time'] = first_time

        if data_df.shape[0] >= 2:
            second_df = data_df.loc[1, ['count', 'total_price', 'pretty_type', 'create_time', 'pay_type']]
            second_df.fillna('', inplace=True)
            second_time['publish_time'] = second_df['create_time'].strftime('%Y-%m-%d %H:%M:%S')
            second_time['total_price'] = second_df['total_price']
            try:
                second_time['pay_type'] = int(second_df['pay_type'])
            except:
                second_time['pay_type'] = second_df['pay_type']
            second_time['pretty_type'] = second_df['pretty_type']
            second_time['count'] = int(second_df['count'])
        else:
            second_time = publish_mode_data
        publish_info['second_time'] = second_time

        if data_df.shape[0] >= 3:
            near_df = data_df[-1:].reset_index(drop=True).loc[
                0, ['count', 'total_price', 'pretty_type', 'create_time', 'pay_type']]
            near_df.fillna('', inplace=True)
            logger.info(near_df)
            near_time['publish_time'] = near_df['create_time'].strftime('%Y-%m-%d %H:%M:%S')
            near_time['total_price'] = near_df['total_price']
            try:
                near_time['pay_type'] = int(near_df['pay_type'])
            except:
                near_time['pay_type'] = near_df['pay_type']
            near_time['pretty_type'] = near_df['pretty_type']
            near_time['count'] = int(near_df['count'])
        else:
            near_time = publish_mode_data
        publish_info['near_time'] = near_time

        return True, publish_info
    except Exception as e:
        logger.error(traceback.format_exc())
        return False, "10000"

# 根据时间标签，返回数据
def match_time_type_data(data_df, request):
    try:
        time_type = request.json['time_type']
        time_info = {}
        to_day = datetime.date.today() # 当前时间
        if time_type == 1: # 今日
            time_data_df = data_df.loc[data_df['create_time'].dt.date == to_day, :]
            # 同比
            to_day_ratio = to_day + timedelta(days=-1)
            time_data_ration_df = data_df.loc[data_df['create_time'].dt.date == to_day_ratio, :]
        elif time_type == 2: # 周
            to_week = to_day + timedelta(days=-6)
            time_data_df = data_df.loc[(data_df['create_time'].dt.date >= to_week) & (
                    data_df['create_time'].dt.date <= to_day), :]
            # 同比
            to_week_end_ratio = to_week + timedelta(days=-1)
            to_week_start_ratio = to_week_end_ratio + timedelta(days=-6)
            time_data_ration_df = data_df.loc[(data_df['create_time'].dt.date >= to_week_start_ratio) & (
                    data_df['create_time'].dt.date <= to_week_end_ratio), :]
        elif time_type == 3: # 月
            to_month = to_day + timedelta(days=-29)
            time_data_df = data_df.loc[(data_df['create_time'].dt.date >= to_month) & (
                    data_df['create_time'].dt.date <= to_day), :]

            # 同比
            to_month_end_ratio = to_month + timedelta(days=-1)
            to_month_start_ratio = to_month_end_ratio + timedelta(days=-29)
            # 同比数据
            time_data_ration_df = data_df.loc[(data_df['create_time'].dt.date >= to_month_start_ratio)
                                                    & (data_df['create_time'].dt.date <= to_month_end_ratio), :]
        else: # 自定义时间
            time_data_df = data_df.loc[(data_df['create_time'] >= request.json['start_time']) & (
                    data_df['create_time'] <= request.json['end_time']), :]

            # 同比
            # 自定义时间间隔
            sub_day = request.json['end_time'].day - request.json['start_time'].day + 1
            # 同比时间
            custom_end_ratio = request.json['start_time'] + timedelta(days=-sub_day)
            custom_start_ratio = request.json['end_time'] + timedelta(days=-sub_day)
            # 同比数据
            time_data_ration_df = data_df.loc[(data_df['create_time'] >= custom_start_ratio)
                                              & (data_df['create_time'] <= custom_end_ratio), :]

        # 如果匹配到数据大于0，再进行统计
        if time_data_df.shape[0] > 0:
            if time_type == 1 or (time_type == 4 and request.json['start_time'].date() == request.json['end_time'].date()):
                time_data_df['strf_time'] = time_data_df['create_time'].dt.strftime('%Y-%m-%d %H')
            else:
                time_data_df['strf_time'] = time_data_df['create_time'].dt.strftime('%Y-%m-%d')
            time_info['total_price'] = round(time_data_df['total_price'].sum(), 2)
            time_info['publish_count'] = int(time_data_df['sell_id'].count())
            # 中间数据
            grouped = time_data_df.groupby('strf_time').agg(
                {"total_price": "sum", "sell_id": "count", "count": "sum"}).reset_index()
            grouped.columns = ['day', 'day_total_price', 'day_count', 'day_pretty_count']
            grouped['day_total_price'] = grouped['day_total_price'].apply(lambda x: round(float(x), 2))
            grouped['day_count'] = grouped['day_count'].astype(int)
            grouped['day_pretty_count'] = grouped['day_pretty_count'].astype(int)
            day_data = grouped.to_dict('records')
            time_info['day_data'] = day_data

            if time_data_ration_df.shape[0] == 0:
                time_info['total_price_ration'] = 0
                time_info['publish_count_ration'] = 0
            else:
                time_info['total_price_ration'] = round(time_data_ration_df['total_price'].sum(), 2)
                time_info['publish_count_ration'] = int(time_data_ration_df['sell_id'].count())
        return True, time_info
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, "10000"

# 关系映射
def map_type(df):
    map_pay_type = {
        "-1": "未知",
        "0": "信用点支付",
        "1": "诚聊余额支付",
        "2": "诚聊通余额支付",
        "3": "微信支付",
        "4": "支付宝支付",
        "5": "后台系统支付",
        "6": "银行卡支付",
        "7": "诚聊通佣金支付",
        "8": "诚聊通红包支付"
    }

    # 转让类型映射
    map_transfer_type = {
        "0": "自主",
        "1": "市场",
        "3": "未知"
    }
    map_status = {
        "0": "上架",
        "1": "下架",
        "2": "已出售",
        "3": "已下单"
    }
    df['transfer_type'] = df['transfer_type'].map(map_transfer_type)
    try:
        df['pay_type'] = df['pay_type'].map(map_pay_type)
    except:
        df['status'] = df['status'].map(map_status)
    return df

# id去小数点
def del_point(data):
    try:
        return data.split(".")[0]
    except:
        return data

# 时间前后判断
def judge_start_and_end_time(start, end):
    try:
        start_time = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        if start_time > end_time:
            return False, "10012"
        else:
            return start_time, end_time
    except:
        logger.error(traceback.format_exc())
        return False, "10013"



def get_phone_by_keyword(keyword):
    '''
    :param keyword:根据关键词找手机号
    :return:
    '''
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        with conn_analyze.cursor() as cursor:
            sql = '''
            select * from (
            select if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname
            ,phone,unionid from crm_user where phone like %s or unionid like %s or `name` like %s or nickname like %s) t 
            where t.phone is not null and (t.nickname like %s or phone like %s or unionid like %s)
            '''
            cursor.execute(sql,("%"+keyword+"%","%"+keyword+"%","%"+keyword+"%","%"+keyword+"%","%"+keyword+"%","%"+keyword+"%","%"+keyword+"%"))

            # sql = '''select * from crm_user where phone like %s'''
            # cursor.execute(sql, ("%" + keyword + "%"))

            datas = cursor.fetchall()
            logger.info(datas)
            if datas:
                phone_list = [data[1] for data in datas]
                return 1,phone_list
            else:
                return 0,"暂无数据"
    except Exception as e:
        return 0,e
    finally:
        conn_analyze.close()


def get_parent_by_phone(phone):
    '''
    :param keyword:根据关键词找手机号
    :return:
    '''
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = '''select * from crm_user where phone = %s'''
            cursor.execute(sql,(phone))
            data = cursor.fetchone()
            if data:
                return 1,data["id"]
            else:
                return 0,"暂无该用户"
    except Exception as e:
        return 0,e
    finally:
        conn_crm.close()


# 返回所有用户运营中心
# 返回所有用户运营中心
def get_all_user_operationcenter(crm_user_data=""):
    '''
    crm_user_data 必须是dataframe
    :param crm_cursor: crm数据库游标。需再调用方法后手动关闭数据库连接
    :return: crm手机不为空的用户对应运营中心
    '''
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return False, '数据库连接失败'
        crm_cursor = conn_crm.cursor()
        operate_sql = 'select id operate_id,unionid, name leader, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and crm = 1'
        crm_cursor.execute(operate_sql)
        operate_data = crm_cursor.fetchall()
        operate_df = pd.DataFrame(operate_data)

        # crm用户数据
        crm_user_df = ""
        if len(crm_user_data) > 0:
            crm_user_df = crm_user_data
        else:
            crm_user_sql = 'select id unionid, pid parentd, phone from luke_sincerechat.user where phone is not null'
            crm_cursor.execute(crm_user_sql)
            crm_user_df = pd.DataFrame(crm_cursor.fetchall())

        # 运营中心手机列表
        operate_telephone_list = operate_df['telephone'].to_list()

        # 关系查找ql
        supervisor_sql = '''
                select a.*,if (crm =0,Null,b.operatename) operatename , b.crm from 
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
            operate_id = operate_df.loc[operate_df['telephone'] == phone, 'operate_id'].values[0]
            bus_phone = operate_df.loc[operate_df['telephone'] == phone, 'telephone'].values[0]
            leader = operate_df.loc[operate_df['telephone'] == phone, 'leader'].values[0]
            leader_unionid = operate_df.loc[operate_df['telephone'] == phone, 'unionid'].values[0]

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
            child_df['operate_id'] = operate_id
            child_df["bus_phone"] = bus_phone
            child_df["leader"] = leader
            child_df["leader_unionid"] = leader_unionid
            child_df_list.append(child_df)
        # 用户数据拼接
        exist_center_df = pd.concat(child_df_list)
        fina_df = crm_user_df.merge(
            exist_center_df.loc[:, ['phone', 'operatename', 'operate_id', 'bus_phone', 'leader', 'leader_unionid']],
            how='left', on='phone')
        conn_crm.close()
        return True, fina_df
    except Exception as e:
        logger.exception(traceback.format_exc())
        return False, "10000"


if __name__ == "__main__":
    # result = get_all_user_operationcenter()
    # result[1].to_csv("e:/20211230.csv")
    result = get_phone_by_keyword("13559436425")
    logger.info(result)
# -*- coding: utf-8 -*-
# @Time : 2021/11/3  10:25
# @Author : shihong
# @File : .py
# --------------------------------------
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from config import *
from analyzeserver.common import *
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime
from functools import reduce
from analyzeserver.common import *

pmbp = Blueprint('personal', __name__, url_prefix='/lh/personal')

# 个人转卖市场发布数据分析
@pmbp.route('/publish', methods=["POST"])
def personal_publish():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 9:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # 表单选择operateid
            operateid = request.json['operateid']
            # 出售人信息
            search_key = request.json['key'].strip()
            # 归属上级
            parent = request.json['parent'].strip()
            # 首次发布时间
            start_first_time = request.json['start_first_time']
            end_first_time = request.json['end_first_time']
            # 最近发布时间
            start_near_time = request.json['start_near_time']
            end_near_time = request.json['end_near_time']

            page = request.json['page']
            num = request.json['num']

            # 时间判断
            if start_first_time or end_first_time:
                first_time_result = judge_start_and_end_time(start_first_time, end_first_time)
                if not first_time_result[0]:
                    return {"code": first_time_result[1], "status": "failed", "msg": message[first_time_result[1]]}
                request.json['start_publish_time'] = first_time_result[0]
                request.json['end_publish_time'] = first_time_result[1]
            if start_near_time or end_near_time:
                near_time_result = judge_start_and_end_time(start_near_time, end_near_time)
                if not near_time_result[0]:
                    return {"code": near_time_result[1], "status": "failed", "msg": message[near_time_result[1]]}
                request.json['start_near_publish_time'] = near_time_result[0]
                request.json['end_near_publish_time'] = near_time_result[1]
            if start_first_time and end_first_time and start_near_time and end_near_time:
                # 最近发布结束 > 首次发布起始
                if start_first_time > end_near_time:
                    return {"code": "10013", "status": "failed", "msg": message["10013"]}
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        # 发布数据
        conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        data_sql = '''
            select sell_phone publish_phone, total_price, create_time from lh_pretty_client.lh_sell where del_flag = 0 and (sell_phone is not null or sell_phone != '')
        '''
        publish_df = pd.read_sql(data_sql, conn_lh)

        df_list = []
        first_df = publish_df.sort_values("create_time", ascending=True).groupby("publish_phone").first().reset_index()
        first_df.columns = ['publish_phone', 'first_price', 'first_time']
        last_df = publish_df.sort_values("create_time", ascending=True).groupby("publish_phone").last().reset_index()
        last_df.columns = ['publish_phone', 'near_price', 'near_time']
        price_count_df = publish_df.groupby("publish_phone")['total_price'].sum().reset_index()
        publish_count_df = publish_df.groupby("publish_phone")['total_price'].count().reset_index()
        publish_count_df.rename(columns={"total_price": "publish_count"}, inplace=True)

        df_list.append(first_df)
        df_list.append(last_df)
        df_list.append(price_count_df)
        df_list.append(publish_count_df)

        df_merge = reduce(lambda left, right: pd.merge(left, right, how='left', on='publish_phone'), df_list)

        # 用户数据
        conn_crm = direct_get_conn(crm_mysql_conf)
        crm_user_sql = '''select id publish_unionid, pid parentid, phone publish_phone, nickname publish_name from luke_sincerechat.user where phone is not null or phone != ""'''
        crm_user_df = pd.read_sql(crm_user_sql, conn_crm)
        crm_user_df = crm_user_df.merge(crm_user_df.loc[:, ["publish_unionid", "publish_phone"]].rename(columns={"publish_unionid":"parentid", "publish_phone":"parent_phone"}), how='left', on='parentid')
        crm_user_df['publish_unionid'] = crm_user_df['publish_unionid'].astype(str)
        crm_user_df['parentid'] = crm_user_df['parentid'].astype(str)

        if operateid:
            flag_1, child_phone_list = get_operationcenter_child(conn_crm, operateid)
            if not flag_1:
                return {"code": child_phone_list, "status": "failed", "msg": message[child_phone_list]}
            part_user_df = df_merge.loc[df_merge['publish_phone'].isin(child_phone_list[:-1]), :]
            part_user_df['operatename'] = child_phone_list[-1]
            part_user_df = part_user_df.merge(crm_user_df, how='left', on='publish_phone')
            # 匹配数据
            match_df = part_user_df.loc[((part_user_df['publish_name'].str.contains(search_key)) | (part_user_df['publish_unionid'].str.contains(search_key)) | (part_user_df['publish_phone'].str.contains(search_key)))
                                        & ((part_user_df['parentid']).str.contains(parent) | (part_user_df['parent_phone']).str.contains(parent))]
            if request.json['start_first_time']:
                match_df = match_df.loc[(part_user_df['first_time'] >= request.json['start_first_time']) & (
                            part_user_df['near_time'] <= (request.json['end_first_time'])), :]
            if request.json['start_near_time']:
                match_df = match_df.loc[(part_user_df['near_time'] >= request.json['start_near_time']) & (
                            part_user_df['near_time'] <= (request.json['end_near_time'])), :]
        else:
            fina_df = df_merge.merge(crm_user_df, how='left', on='publish_phone')
            if not search_key and not parent and not start_first_time and not end_first_time and not start_near_time and not end_near_time and not page and not num:
                all_user_operate_result = get_all_user_operationcenter()
                if not all_user_operate_result[0]:
                    return {"code": "10000", "status": "success", "msg": message["10000"]}
                all_user_operate_result[1].rename(columns={"phone": 'publish_phone'}, inplace=True)
                match_df = fina_df.merge(all_user_operate_result[1].loc[:, ['publish_phone', 'operatename']], how='left', on='publish_phone')
                # match_df.drop('parent_phone', axis=1, inplace=True)
            elif not search_key and not parent and not start_first_time and not end_first_time and not start_near_time and not end_near_time and page and num:
                start_index = (page - 1) * num
                end_index = page * num
                if end_index > len(fina_df):
                    end_index = len(fina_df)
                child_df = fina_df.loc[start_index:end_index, :]
                child_phone_list = child_df['publish_phone'].tolist()
                match_result = match_user_operate1(child_phone_list, conn_crm, 'publish_phone')
                if not match_result[0]:
                    return {"code": match_result[1], "status": "failed", "msg": message[match_result[1]]}
                match_df = child_df.merge(match_result[1], how='left', on='publish_phone')
            else:
                match_df = fina_df.loc[((fina_df['publish_name'].str.contains(search_key)) | (fina_df['publish_unionid'].str.contains(search_key)) | (fina_df['publish_phone'].str.contains(search_key)))
                                        & ((fina_df['parentid']).str.contains(parent) | (fina_df['parent_phone']).str.contains(parent))]
                if request.json['start_first_time']:
                    match_df = match_df.loc[(fina_df['first_time'] >= request.json['start_first_time']) & (
                            fina_df['near_time'] <= (request.json['end_first_time'])), :]
                if request.json['start_near_time']:
                    match_df = match_df.loc[(fina_df['near_time'] >= request.json['start_near_time']) & (
                            fina_df['near_time'] <= (request.json['end_near_time'])), :]
                child_phone_list = match_df['publish_phone'].tolist()
                match_result = match_user_operate1(child_phone_list, conn_crm, 'publish_phone')
                if not match_result[0]:
                    return {"code": match_result[1], "status": "success", "msg": message[match_result[1]]}
                match_df = match_df.merge(match_result[1], how='left', on='publish_phone')
                # 如果没有页码
                # match_df.drop('parent_phone', axis=1, inplace=True)
        if match_df.shape[0] == 0:
            return {"code": "10010", "status": "failed", "msg": message["10010"]}
        match_df.drop('parent_phone', axis=1, inplace=True)

        if page and num:
            start_index = (page - 1) * num
            if num > match_df.shape[0]:
                end_index = len(match_df)
            else:
                end_index = page * num
        else:
            start_index = 0
            end_index = len(match_df)
        match_df['first_time'] = match_df['first_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        match_df['near_time'] = match_df['near_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # 价钱圆整
        match_df['total_price'] = match_df['total_price'].apply(lambda x: round(float(x), 2))
        match_df.fillna("", inplace=True)
        match_dict_list = match_df.loc[start_index: end_index - 1, :].to_dict('records')
        return_data = {
            'count': len(match_dict_list),
            'search_data': match_dict_list
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
        except:
            pass


# 个人转卖市场订单流水
@pmbp.route('/orderflow', methods=["POST"])
def personal_order_flow():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) !=11:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}
            # 购买人信息
            buyer_info = request.json['buyer_info'].strip()
            # 表单选择operateid
            operateid = request.json['operateid']
            # 归属上级
            parent = request.json['parent'].strip()
            # 订单编码
            order_sn = request.json['order_sn'].strip()
            # 出售人信息
            sell_info = request.json['sell_info'].strip()
            # 交易时间
            start_order_time = request.json['start_order_time']
            end_order_time = request.json['end_order_time']
            # 支付类型
            pay_id = request.json['pay_id']
            # 转让类型
            transfer_id = request.json['transfer_id']

            # 每页显示条数
            num = request.json['num']
            # 页码
            page = request.json['page']
            if start_order_time or end_order_time:
                order_time_result = judge_start_and_end_time(start_order_time, end_order_time)
                if not order_time_result[0]:
                    return {"code": order_time_result[1], "status": "failed", "msg": message[order_time_result[1]]}
                request.json['start_order_time'] = order_time_result[0]
                request.json['end_order_time'] = order_time_result[1]
        except:
            # 参数名错误
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        # crm用户数据
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        crm_user_sql = '''select t1.*, t2.parent_phone from 
            (select id buyer_unionid, id sell_unionid, pid parentid,phone buyer_phone, phone sell_phone, nickname buyer_name, nickname sell_name from luke_sincerechat.user where phone is not null or phone != "") t1
            left join
            (select id, phone parent_phone from luke_sincerechat.user where phone is not null or phone != "") t2
            on t1.parentid = t2.id'''
        crm_user_df = pd.read_sql(crm_user_sql, conn_crm)

        # 订单流水数据
        order_flow_sql = '''
            select t1.order_sn, t1.buyer_phone, t1.pay_type, t1.count, t1.buy_price, t1.sell_phone, t1.sell_price, t1.true_price, t1.sell_fee, t1.order_time, t2.transfer_type from
            (select order_sn, phone buyer_phone, pay_type, count, total_price buy_price, sell_phone, total_price sell_price, total_price - sell_fee true_price, sell_fee, order_time, sell_id
            from lh_pretty_client.lh_order
            where `status`  = 1
            and type in (1, 4)
            and phone is not null) t1
            left join
            (select id, price_status transfer_type from lh_pretty_client.lh_sell) t2
            on t1.sell_id = t2.id
            '''
        conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        order_flow_df = pd.read_sql(order_flow_sql, conn_lh)

        flag_1, fina_df = order_and_user_merge(order_flow_df, crm_user_df)
        if not flag_1:
            # 10000
            return {"code": fina_df, "status": "failed", "msg": message[fina_df]}

        # 如果存在运营中心参数
        if operateid:
            flag_2, child_phone_list = get_operationcenter_child(conn_crm, operateid)
            if not flag_2:
                return {"code": child_phone_list, "status": "failed", "msg": message[child_phone_list]}
            flag_3, match_df = order_exist_operationcenter(fina_df, child_phone_list, request)
            if not flag_3:
                return {"code": match_df, "status": "failed", "msg": message[match_df]}

            match_dict_list = match_df.to_dict('records')
            logger.info(match_dict_list)
            if num and page:
                start_index= (page - 1) * num
                end_index = page * num
                # 如果num超过数据条数
                if end_index > len(match_dict_list):
                    end_index = len(match_dict_list)
            else:
                start_index = 0
                end_index = len(match_dict_list)
            return_data = {
                "count": match_df.shape[0],
                "data": match_dict_list[start_index: end_index]
            }
            return {"code": "0000", "status": "success", "msg": return_data}
        # 如果不存在运营中心参数
        else:
            # 判断是否为无参
            if not buyer_info and not parent and not sell_info and not start_order_time and not end_order_time and not order_sn and not transfer_id and not pay_id:
                # 根据页码和显示条数返回数据
                if num and page:
                    start_index = (page - 1) * num
                    end_index = page * num
                    # 如果num超过数据条数
                    if end_index > len(fina_df):
                        end_index = len(fina_df)
                    flag_4, match_df = match_user_operate(conn_crm, fina_df.iloc[start_index:end_index, :])
                    if not flag_4:
                        return {"code": match_df, "status": "failed", "msg": message[match_df]}
                else:
                    all_user_operate_result = get_all_user_operationcenter()
                    if not all_user_operate_result[0]:
                        return {"code": "10000", "status": "success", "msg": message["10000"]}
                    all_user_operate_result[1].rename(columns={"phone": 'buyer_phone'}, inplace=True)
                    match_df = fina_df.merge(all_user_operate_result[1].loc[:, ['buyer_phone', 'operatename']], how='left', on='buyer_phone')
                    match_df.drop(['parent_phone', 'sell_name'], axis=1, inplace=True)
                # flag_4, match_df = match_user_operate(conn_crm, fina_df.iloc[start_index:end_index, :])
                match_df['order_time'] = match_df['order_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                match_dict_list = match_df.to_dict('records')
                return_data = {
                    "count": match_df.shape[0],
                    "data": match_dict_list
                }
                return {"code": "0000", "status": "success", "msg": return_data}

            else:
                flag_5, match_df = match_attribute(fina_df, request)
                if not flag_5:
                    return {"code": match_df, "status": "failed", "msg": message[match_df]}
                if num and page:
                    start_index = (page - 1) * num
                    end_index = page * num
                    # 如果num超过数据条数
                    if end_index > len(match_df):
                        end_index = len(match_df)
                else:
                    start_index = 0
                    end_index = len(match_df)
                flag_6, match_df_1 = match_user_operate(conn_crm, match_df.iloc[start_index:end_index, :])
                if not flag_6:
                    return {"code": match_df_1, "status": "failed", "msg": message[match_df_1]}
                match_df_1['order_time'] = match_df_1['order_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                match_dict_list = match_df_1.to_dict('records')
                return_data = {
                    "count": match_df.shape[0],
                    "data": match_dict_list
                }
                return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
        except:
            pass

# 个人转卖市场发布出售订单流水
@pmbp.route('/publishflow', methods=["POST"])
def personal_publish_order_flow():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 13:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # 表单选择operateid
            operateid = request.json['operateid']
            # 出售人信息
            sell_info = request.json['sell_info'].strip()
            # 归属上级
            parent = request.json['parent'].strip()
            # 交易时间
            start_publish_time = request.json['start_publish_time']
            end_publish_time = request.json['end_publish_time']
            # 上架时间
            start_up_time = request.json['start_up_time']
            end_up_time = request.json['end_up_time']
            # 出售时间
            start_sell_time = request.json['start_sell_time']
            end_sell_time = request.json['end_sell_time']
            # 状态
            status = request.json['status']
            # 转让类型
            transfer_id = request.json['transfer_id']

            # 每页显示条数
            num = request.json['num']
            # 页码
            page = request.json['page']
            # isdigit()可以判断是否为正整数
            # if not num.isdigit() or int(num) < 1:
            #     return {"code": "10009", "status": "failed", "msg": message["10009"]}
            # elif not page.isdigit() or int(page) < 1:
            #     return {"code": "10009", "status": "failed", "msg": message["10009"]}
            # else:
            #     num = int(num)
            #     page = int(page)

            # 时间判断
            if start_publish_time or end_publish_time:
                order_time_result = judge_start_and_end_time(start_publish_time, end_publish_time)
                if not order_time_result[0]:
                    return {"code": order_time_result[1], "status": "failed", "msg": message[order_time_result[1]]}
                request.json['start_publish_time'] = order_time_result[0]
                request.json['end_publish_time'] = order_time_result[1]
            if start_up_time or end_up_time:
                order_time_result = judge_start_and_end_time(start_up_time, end_up_time)
                if not order_time_result[0]:
                    return {"code": order_time_result[1], "status": "failed", "msg": message[order_time_result[1]]}
                request.json['start_order_time'] = order_time_result[0]
                request.json['end_order_time'] = order_time_result[1]
            if start_sell_time or end_sell_time:
                order_time_result = judge_start_and_end_time(start_sell_time, end_sell_time)
                if not order_time_result[0]:
                    return {"code": order_time_result[1], "status": "failed", "msg": message[order_time_result[1]]}
                request.json['start_order_time'] = order_time_result[0]
                request.json['end_order_time'] = order_time_result[1]
        except:
            # 参数名错误
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        publish_sql = '''select sell_phone, count, pretty_type_name, total_price/count unit_price, total_price, price_status transfer_type, `status`, create_time publish_time, up_time, sell_time
        from lh_pretty_client.lh_sell
        where del_flag = 0'''

        conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        publish_order_df = pd.read_sql(publish_sql, conn_lh)

        conn_crm = direct_get_conn(crm_mysql_conf)
        crm_user_sql = '''select t1.*, t2.parent_phone from 
            (select id sell_unionid, pid parentid, phone sell_phone, nickname sell_name from luke_sincerechat.user where phone is not null or phone != "") t1
            left join
            (select id, phone parent_phone from luke_sincerechat.user where phone is not null or phone != "") t2
            on t1.parentid = t2.id'''
        crm_user_df = pd.read_sql(crm_user_sql, conn_crm)
        fina_df = publish_order_df.merge(crm_user_df, how='left', on='sell_phone')
        fina_df['status'] = fina_df['status'].astype(str)
        fina_df['transfer_type'] = fina_df['transfer_type'].astype(str)
        fina_df['transfer_type'] = fina_df['transfer_type'].astype(str)
        fina_df['sell_unionid'] = fina_df['sell_unionid'].astype(str)
        fina_df['sell_unionid'] = fina_df['sell_unionid'].apply(lambda x: del_point(x))
        fina_df['parentid'] = fina_df['parentid'].astype(str)
        fina_df['parentid'] = fina_df['parentid'].apply(lambda x: del_point(x))
        # 最终返回结果时处理
        # fina_df['publish_time'] = fina_df['publish_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        # fina_df['up_time'] = fina_df['up_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        # fina_df['sell_time'] = fina_df['sell_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        # fina_df['sell_time'] = fina_df['sell_time'].astype(str)


        if operateid:
            flag_2, child_phone_list = get_operationcenter_child(conn_crm, operateid)
            if not flag_2:
                return {"code": child_phone_list, "status": "failed", "msg": message[child_phone_list]}
            flag_3, match_df = publish_exist_operationcenter(fina_df, child_phone_list, request)
            if not flag_3:
                return {"code": match_df, "status": "failed", "msg": message[match_df]}

            match_dict_list = match_df.to_dict('records')
            logger.info(match_dict_list)
            if num and page:
                start_index = (page - 1) * num
                end_index = page * num
                # 如果num超过数据条数
                if end_index > len(match_df):
                    end_index = len(match_df)
            else:
                start_index = 0
                end_index = len(match_df)
            return_data = {
                "count": match_df.shape[0],

                "data": match_dict_list[start_index: end_index]
            }
            return {"code": "0000", "status": "success", "msg": return_data}
        # 如果不存在运营中心参数
        else:
            # 判断是否为无参
            if not parent and not sell_info and not start_publish_time and not start_up_time and not transfer_id and not start_sell_time:
                # 根据页码和显示条数返回数据
                if num and page:
                    start_index = (page - 1) * num
                    end_index = page * num
                    # 如果num超过数据条数
                    if end_index > len(fina_df):
                        end_index = len(fina_df)
                    flag_4, match_df = match_user_operate(conn_crm, fina_df.iloc[start_index:end_index, :], mode="publish")
                    if not flag_4:
                        return {"code": match_df, "status": "failed", "msg": message[match_df]}
                else:
                    all_user_operate_result = get_all_user_operationcenter()
                    if not all_user_operate_result[0]:
                        return {"code": "10000", "status": "failed", "msg": message["10000"]}
                    all_user_operate_result[1].rename(columns={"phone": 'sell_phone'}, inplace=True)
                    match_df = fina_df.merge(all_user_operate_result[1].loc[:, ['sell_phone', 'operatename']], how='left', on='sell_phone')
                    match_df.drop(['parent_phone'], axis=1, inplace=True)
                match_df['publish_time'] = match_df['publish_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
                match_df['up_time'] = match_df['up_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
                try:
                    match_df['sell_time'] = match_df['sell_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
                match_df['sell_time'] = match_df['sell_time'].astype(str)
                match_df['sell_time'] = match_df['sell_time'].apply(lambda x: x.replace("NaT", ""))
                match_dict_list = match_df.to_dict('records')
                return_data = {
                    "count": match_df.shape[0],
                    "data": match_dict_list
                }
                return {"code": "0000", "status": "success", "msg": return_data}

            else:
                flag_5, match_df = match_attribute(fina_df, request, mode="publish")
                if not flag_5:
                    return {"code": match_df, "status": "failed", "msg": message[match_df]}
                if num and page:
                    start_index = (page - 1) * num
                    end_index = page * num
                    # 如果num超过数据条数
                    if end_index > len(match_df):
                        end_index = len(match_df)
                else:
                    start_index = 0
                    end_index = len(match_df)
                flag_6, match_df_1 = match_user_operate(conn_crm, match_df.iloc[start_index:end_index, :], mode="publish")
                if not flag_6:
                    return {"code": match_df_1, "status": "failed", "msg": message[match_df_1]}
                match_df_1['publish_time'] = match_df_1['publish_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
                match_df_1['up_time'] = match_df_1['up_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
                try:
                    match_df_1['sell_time'] = match_df_1['sell_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
                match_df_1['sell_time'] = match_df_1['sell_time'].astype(str)
                match_df_1['sell_time'] = match_df_1['sell_time'].apply(lambda x: x.replace("NaT", ""))
                match_dict_list = match_df_1.to_dict('records')
                logger.info(match_dict_list)
                return_data = {
                    "count": match_df.shape[0],
                    "data": match_dict_list
                }
                return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
        except:
            pass

# 个人转卖市场订单数据统计分析
@pmbp.route("total",methods=["POST"])
def personal_total():
    try:
        conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)

        logger.info(request.json)
        page = request.json["page"]
        size = request.json["size"]

        # 可以是用户名称 手机号 unionid 模糊的
        keyword = request.json["keyword"]

        # 查询归属上级 精准的
        parent = request.json["parent"]
        # bus = request.json["bus"]
        bus_id = request.json["bus_id"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_phone = []
        bus_phone = []

        # 模糊查询
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
            else:
                return {"code":"11016","status":"failed","msg":message["11016"]}

        # 只查一个
        if parent:
            if len(parent) == 11:
                parent_phone.append(parent)
            else:
                result = get_phone_by_unionid(parent)
                if result[0] == 1:
                    parent_phone.append(result[1])
                else:
                    return {"code":"11014","status":"failed","msg":message["code"]}
        # 查禄可商务的
        # if bus:
        #     result = get_lukebus_phone([bus])
        #     if result[0] == 1:
        #         bus_phone = result[1].split(",")
        #     else:
        #         return {"code":"11015","status":"failed","msg":message["11015"]}

        if bus_id:
            result = get_busphne_by_id(bus_id)
            if result[0] == 1:
                bus_phone = result[1].split(",")
            else:
                return {"code":"11015","status":"failed","msg":message["11015"]}

        # 对手机号码差交集
        if keyword_phone and parent_phone and bus_phone:
            query_phone = list((set(keyword_phone).intersection(set(parent_phone))).intersection(set(bus_phone)))
        elif keyword_phone and parent_phone:
            query_phone = list(set(keyword_phone).intersection(set(parent_phone)))
        elif keyword_phone and bus_phone:
            query_phone = list(set(keyword_phone).intersection(set(bus_phone)))
        elif parent_phone and bus_phone:
            query_phone = list(set(parent_phone).intersection(set(bus_phone)))
        elif keyword_phone:
            query_phone = keyword_phone
        elif parent_phone:
            query_phone = parent_phone
        elif bus_phone:
            query_phone = bus_phone
        else:
            query_phone = ""

        if page and size:
            code_page = (page - 1) * 10
            code_size = page * size


        # 如果没有手机号码可以一起查 如果有手机号码要分成三个sql
        # sql = '''select count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price,
        #         count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,
        #         sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee
        #         from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) '''
        # logger.info(sql)
        # last_all_data = pd.read_sql(sql,conn_read).to_dict("records")


        order_sql = '''select phone,count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
        group_sql = ''' group by phone'''
        if query_phone:
            condition_sql = ''' and phone in (%s)''' % (",".join(query_phone))
            order_sql = order_sql+condition_sql+group_sql
        else:
            order_sql = order_sql +group_sql
        logger.info("order_sql:%s" %order_sql)
        order_data = pd.read_sql(order_sql,conn_read)
        logger.info(order_data.shape)

        sell_sql = '''select sell_phone phone,count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
        group_sql = ''' group by sell_phone'''
        if query_phone:
            condition_sql = ''' and sell_phone in (%s)''' % (",".join(query_phone))
            sell_sql = sell_sql + condition_sql + group_sql
        else:
            sell_sql = sell_sql + group_sql
        logger.info("sell_sql:%s" %sell_sql)
        sell_order = pd.read_sql(sell_sql,conn_read)
        logger.info(sell_order.shape)

        public_sql = '''select sell_phone phone,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1'''
        group_sql = ''' group by sell_phone '''
        if query_phone:
            condition_sql = ''' and sell_phone in (%s)''' % (",".join(query_phone))
            public_sql = public_sql + condition_sql + group_sql
        else:
            public_sql = public_sql + group_sql
        logger.info("public_sql:%s" %public_sql)
        public_order = pd.read_sql(public_sql,conn_read)
        logger.info(public_order.shape)



        df_list = []
        df_list.append(order_data)
        df_list.append(sell_order)
        df_list.append(public_order)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        all_data = {"buy_count": 0, "buy_total_count": 0, "buy_total_price": 0, "sell_count": 0, "sell_fee": 0,
                    "sell_real_money": 0, "sell_total_count": 0, "sell_total_price": 0}


        #把nan都填充0
        df_merged["buy_count"].fillna(0,inplace=True)
        df_merged["buy_total_count"].fillna(0,inplace=True)
        df_merged["buy_total_price"].fillna(0,inplace=True)
        df_merged["sell_count"].fillna(0,inplace=True)
        df_merged["sell_fee"].fillna(0,inplace=True)
        df_merged["sell_real_money"].fillna(0,inplace=True)
        df_merged["sell_total_count"].fillna(0,inplace=True)
        df_merged["sell_total_count"].fillna(0,inplace=True)

        all_df = df_merged.to_dict("records")

        for i in range(0,len(all_df)):

            all_data["buy_count"] = all_data["buy_count"] + all_df[i]["buy_count"]
            all_data["buy_total_count"] = all_data["buy_total_count"] + all_df[i]["buy_total_count"]
            all_data["buy_total_price"] = all_data["buy_total_price"] + all_df[i]["buy_total_price"]
            all_data["sell_count"] = all_data["sell_count"] + all_df[i]["sell_count"]
            all_data["sell_fee"] = all_data["sell_fee"] + all_df[i]["sell_fee"]
            all_data["sell_real_money"] = all_data["sell_real_money"] + all_df[i]["sell_real_money"]
            all_data["sell_total_count"] = all_data["sell_total_count"] + all_df[i]["sell_total_count"]
            all_data["sell_total_price"] = all_data["sell_total_price"] + all_df[i]["sell_total_price"]

        all_data["buy_total_price"] = round(all_data["buy_total_price"],2)
        all_data["sell_fee"] = round(all_data["sell_fee"],2)
        all_data["sell_real_money"] = round(all_data["sell_real_money"],2)
        all_data["sell_total_price"] = round(all_data["sell_total_price"],2)


        logger.info("code_page:%s" %code_page)
        logger.info("code_size:%s" %code_size)
        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()
        logger.info(need_data)

        result = user_belong_bus(need_data)

        if result[0] == 1:
            last_data = result[1]
            logger.info(last_data)
        else:
            return {"code":"10006","status":"failed","msg":message["10006"]}
        msg_data = {"data":last_data,"all_data":all_data}
        logger.info("msg_data:%s" %msg_data)
        return {"code":"0000","status":"success","msg":msg_data,"count":len(df_merged)}
    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


'''个人转卖市场采购数据分析总'''
@pmbp.route("buy",methods=["POST"])
def personal_buy():
    try:
        conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)

        logger.info(request.json)
        page = request.json["page"]
        size = request.json["size"]

        # 可以是用户名称 手机号 unionid 模糊的
        keyword = request.json["keyword"]

        # 查询归属上级 精准的
        parent = request.json["parent"]

        bus_id = request.json["bus_id"]
        first_start_time = request.json["first_start_time"]
        first_end_time = request.json["first_end_time"]
        last_start_time = request.json["last_start_time"]
        last_end_time = request.json["last_end_time"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_phone = []
        bus_phone = []

        time_condition_sql = ""
        if first_start_time and first_end_time and last_start_time and last_end_time:
            if first_start_time >= last_start_time and last_end_time >= last_start_time:
                time_condition_sql = ''' and create_time>=%s and create_time <= %s''' %(first_start_time,first_end_time)
                pass
            else:
                return {"code":"11019","status":"failed","msg":message[["11019"]]}


        # 模糊查询
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
            else:
                return {"code":"11016","status":"failed","msg":message["11016"]}

        # 只查一个
        if parent:
            if len(parent) == 11:
                parent_phone.append(parent)
            else:
                result = get_phone_by_unionid(parent)
                if result[0] == 1:
                    parent_phone.append(result[1])
                else:
                    return {"code":"11014","status":"failed","msg":message["code"]}


        if bus_id:
            result = get_busphne_by_id(bus_id)
            if result[0] == 1:
                bus_phone = result[1].split(",")
            else:
                return {"code":"11015","status":"failed","msg":message["11015"]}

        # 对手机号码差交集
        if keyword_phone and parent_phone and bus_phone:
            query_phone = list((set(keyword_phone).intersection(set(parent_phone))).intersection(set(bus_phone)))
        elif keyword_phone and parent_phone:
            query_phone = list(set(keyword_phone).intersection(set(parent_phone)))
        elif keyword_phone and bus_phone:
            query_phone = list(set(keyword_phone).intersection(set(bus_phone)))
        elif parent_phone and bus_phone:
            query_phone = list(set(parent_phone).intersection(set(bus_phone)))
        elif keyword_phone:
            query_phone = keyword_phone
        elif parent_phone:
            query_phone = parent_phone
        elif bus_phone:
            query_phone = bus_phone
        else:
            query_phone = ""

        if page and size:
            code_page = (page - 1) * 10
            code_size = page * size

        buy_sql = '''select phone,total_price,create_time from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
        if time_condition_sql:
            buy_sql = buy_sql+time_condition_sql

        group_sql = ''' group by phone'''
        # limit_sql = ''' limit %s,%s''' %(code_page,code_size)
        if query_phone:
            condition_sql = ''' and phone in (%s)''' % (",".join(query_phone))
            # order_sql = buy_sql+condition_sql+group_sql + limit_sql
            order_sql = buy_sql+condition_sql
        else:
            # order_sql = buy_sql +group_sql + limit_sql
            order_sql = buy_sql

        #返回条数


        logger.info("order_sql:%s" %order_sql)
        order_data = pd.read_sql(order_sql,conn_read)
        order_data_group = order_data.groupby("phone")

        #排序取出按时间第一条和最后一条的
        first_data = order_data.sort_values("create_time", ascending=True).groupby("phone").first().reset_index()
        first_data.rename(columns={"phone":"phone","create_time":"first_time","total_price":"first_total_price"},inplace=True)
        first_data["first_time"] = first_data['first_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))


        last_data = order_data.sort_values("create_time", ascending=True).groupby("phone").last().reset_index()
        last_data.rename(columns={"phone": "phone", "create_time": "last_time", "total_price": "last_total_price"},inplace=True)
        # last_data["last_time"] = last_data['last_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        last_data["last_time"] = last_data['last_time'].dt.strftime("%Y-%m-%d %H:%M:%S")

        sum_data = order_data.sort_values("create_time", ascending=True).groupby("phone").sum("total_price").reset_index()
        count_data = order_data.sort_values("create_time", ascending=True).groupby("phone").count().reset_index().drop("create_time",axis=1)
        count_data.rename(columns={"phone":"phone","total_price":"count"},inplace=True)

        df_list = []
        df_list.append(first_data)
        df_list.append(last_data)
        df_list.append(sum_data)
        df_list.append(count_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)
        # df_merged["unionid"] = df_merged["unionid"].astype(int)

        logger.info(df_merged.shape)
        if page and size:
            df_merged = df_merged[code_page:code_size]

        crm_data_result = get_all_user_operationcenter()
        if crm_data_result[0] ==  True:
            crm_data = crm_data_result[1]

            result = df_merged.merge(crm_data,how="left",on="phone")
            # result["id"] = result[(result["id"] != "") | (result["id"].notna())].astype(int)
            last_data = result.to_dict("records")
        else:
            return {"code":"10006","status":"failed","msg":message["10006"]}


        for d in last_data:
            # logger.info(d)
            if not pd.isnull(d["id"]):
                d["id"] = int(d["id"])

        return {"code":"0000","status":"success","msg":last_data,"count":len(order_data_group)}
    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


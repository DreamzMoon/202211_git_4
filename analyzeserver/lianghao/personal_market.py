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
from datetime import timedelta
from functools import reduce
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
import numpy as np

pmbp = Blueprint('personal', __name__, url_prefix='/lh/personal')

# 个人转卖市场发布数据分析
@pmbp.route('/publish', methods=["POST"])
def personal_publish():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 10:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            # 表单选择operateid
            operateid = request.json['operateid']
            # 出售人信息
            search_key = request.json['keyword'].strip()
            # 归属上级
            parent = request.json['parent'].strip()
            # 首次发布时间
            start_first_time = request.json['start_first_time']
            end_first_time = request.json['end_first_time']
            # 最近发布时间
            start_near_time = request.json['start_near_time']
            end_near_time = request.json['end_near_time']

            page = request.json['page']
            size = request.json['size']
            num = size

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
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_lh or not conn_crm:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
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
        crm_user_sql = '''select id publish_unionid, pid parentid, phone publish_phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) publish_name from luke_sincerechat.user where phone is not null or phone != ""'''
        crm_user_df = pd.read_sql(crm_user_sql, conn_crm)
        crm_user_df = crm_user_df.merge(crm_user_df.loc[:, ["publish_unionid", "publish_phone"]].rename(columns={"publish_unionid":"parentid", "publish_phone":"parent_phone"}), how='left', on='parentid')
        crm_user_df['publish_unionid'] = crm_user_df['publish_unionid'].astype(str)
        crm_user_df['parentid'] = crm_user_df['parentid'].astype(str)
        count_len = 0

        fina_df = df_merge.merge(crm_user_df, how='left', on='publish_phone')
        # fina_df.fillna("", inplace=True)
        if operateid:
            match_result = if_exist_operate_match_data(fina_df, operateid, request, 'publish_phone', 'publish_data')
            if not match_result[0]:
                return {"code": match_result[1], "status": "failed", "msg": message[match_result[1]]}
            match_df = match_result[1]
        else:
            match_result = not_exist_operate_match_data(fina_df, request, 'publish_phone', mode='publish_data')
            if not match_result[0]:
                return {"code": match_result[1], "status": "failed", "msg": message[match_result[1]]}
            match_df = match_result[1]

        match_df['first_time'] = match_df['first_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        match_df['near_time'] = match_df['near_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # 价钱圆整
        match_df['total_price'] = match_df['total_price'].apply(lambda x: round(float(x), 2))
        match_df.fillna("", inplace=True)
        match_dict_list = match_df.to_dict('records')
        return_data = {
            'data': match_dict_list
        }
        return {"code": "0000", "status": "success", "msg": return_data, "count":  match_result[2].shape[0]}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
            conn_crm.close()
        except:
            pass

# 个人转卖市场发布数据分析详情
@pmbp.route('/publish/detail', methods=["POST"])
def personal_publish_detail():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 5:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            # 手机号
            phone = request.json['phone'].strip()
            if not phone:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}
            # 1今日 2本周 3本月 4自定义-->必须传起始和结束时间
            time_type = request.json['time_type']
            # 首次发布时间
            start_time = request.json['start_time']
            end_time = request.json['end_time']

            if (time_type != 4 and start_time and end_time) or time_type not in range(1, 5) or (time_type == 4 and not start_time and not end_time):
                return {"code": "10014", "status": "failed", "msg": message["10014"]}
            # 时间判断
            elif start_time or end_time:
                judge_result = judge_start_and_end_time(start_time, end_time)
                if not judge_result[0]:
                    return {"code": judge_result[1], "status": "failed", "msg": message[judge_result[1]]}
                sub_day = judge_result[1] - judge_result[0]
                if sub_day.days + sub_day.seconds / (24.0 * 60.0 * 60.0) > 30:
                    return {"code": "11018", "status": "failed", "msg": message["11018"]}
                request.json['start_time'] = judge_result[0]
                request.json['end_time'] = judge_result[1]
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        # 1.根据手机号查找名称，id，归属运营中心，归属上级手机号
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_crm or not conn_lh:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        # 用户基础信息
        search_user_info_sql = '''select if(`name` is not null,`name`,if(nickname is not null,nickname,"")) name, id unionid, pid parentid from luke_sincerechat.user where phone=%s''' % phone
        user_info_df = pd.read_sql(search_user_info_sql, conn_crm)
        parent_info_sql = '''select id parentid, phone parent_phone from luke_sincerechat.user where id=%s''' % \
                          user_info_df['parentid'].values[0]
        parent_df = pd.read_sql(parent_info_sql, conn_crm)

        # 存储总数据
        fina_data = {}

        # 合并上级手机号
        user_base_info_df = user_info_df.merge(parent_df, how='left', on='parentid')
        # 查找运营中心
        operate_sql = '''
            select a.phone, if (crm =0, Null, b.operatename) operatename, b.crm from 
            (WITH RECURSIVE temp as (
                    SELECT t.id, t.pid, t.phone FROM luke_sincerechat.user t WHERE phone = %s
                    UNION ALL
                    SELECT t.id, t.pid, t.phone FROM luke_sincerechat.user t INNER JOIN temp ON t.id = temp.pid
            )
            SELECT * FROM temp 
            )a left join luke_lukebus.operationcenter b
            on a.id = b.unionid
            ''' % phone
        match_operate_data = pd.read_sql(operate_sql, conn_crm)
        match_operatename = match_operate_data.loc[
            (match_operate_data['operatename'].notna()) & (match_operate_data['crm'] == 1), 'operatename'].tolist()
        if match_operatename:
            match_operate_data.loc[0, 'operatename'] = match_operatename[0]
        match_user_data = match_operate_data.loc[:0, :]

        # 用户基本信息
        user_base_info_df['operatename'] = match_user_data['operatename']
        user_base_info_df.fillna("", inplace=True)
        user_base_info = user_base_info_df.to_dict('records')[0]
        user_base_info['phone'] = phone
        fina_data['user_base_info'] = user_base_info

        publish_sql = '''select id sell_id, count, total_price, pretty_type_name pretty_type, create_time from lh_pretty_client.lh_sell where del_flag=0 and sell_phone=%s''' % phone
        user_publish_base_df = pd.read_sql(publish_sql, conn_lh)

        order_sql = '''select sell_id, sell_phone publish_phone, pay_type from lh_pretty_client.lh_order where del_flag=0 and sell_phone= %s and `status`=1 and pay_type is not null and sell_id is not null''' % phone
        user_order_df = pd.read_sql(order_sql, conn_lh)
        # 此步合并会存在已发布但是未出售时的publish_phone与pay_type为空，导致二次或者最近出错，因此在进行首次、二次、最近计算时，需要去除为空的数据
        user_publish_df = user_order_df.merge(user_publish_base_df, how='left', on='sell_id')
        user_publish_df.sort_values('create_time', inplace=True)
        user_publish_df.reset_index(drop=True, inplace=True)
        # 数据分析---用户标题总数据
        title_data = {}

        pay_type_df = pd.DataFrame(user_publish_df.groupby('pay_type')['pay_type'].count()).rename(
            columns={'pay_type': "count"}).reset_index()
        pay_type_df['pay_type'] = pay_type_df['pay_type'].astype(int)
        title_data['pay_type_count'] = pay_type_df.to_dict('records')

        title_data_count = user_publish_df.groupby('publish_phone').agg(
            {"total_price": "sum", "count": "sum"}).reset_index()
        title_data_count.columns = ['publish_phone', 'total_price', 'pretty_count']
        title_data['total_price'] = round(title_data_count['total_price'].values[0], 2)
        title_data['pretty_count'] = int(title_data_count['pretty_count'].values[0])
        # title_data['publish_count'] = int(title_data_count['publish_count'].values[0])
        # 计算发布次数：是否成功出售都进行统计，保持与发布一致
        title_data['publish_count'] = int(user_publish_base_df['sell_id'].count())
        fina_data['title_data'] = title_data

        # 用户首次，二次，最近发布数据
        # 去除空值
        # user_publish_df = user_publish_df.dropna()
        publish_result = user_first_second_near_publish(user_publish_df)
        if not publish_result[0]:
            return {"code": publish_result[1], "status": "success", "msg": message[publish_result[1]]}

        fina_data['publish_info'] = publish_result[1]

        # 匹配时间类别
        time_result = match_time_type_data(user_publish_df, request)
        if not time_result[0]:
            return {"code": time_result[1], "status": "success", "msg": message[time_result[1]]}
        fina_data['time_data'] = time_result[1]
        return {"code": "0000", "status": "success", "msg": fina_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
            conn_lh.close()
        except:
            pass

# 个人转卖市场订单流水
@pmbp.route('/orderflow', methods=["POST"])
def personal_order_flow():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) !=12:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

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
            size = request.json['size']
            # 页码
            page = request.json['page']
            num = size
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
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_crm or not conn_lh:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        crm_user_sql = '''select t1.*, t2.parent_phone from 
            (select id buyer_unionid, id sell_unionid, pid parentid,phone buyer_phone, phone sell_phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) buyer_name, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) sell_name from luke_sincerechat.user where phone is not null or phone != "") t1
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
        order_flow_df = pd.read_sql(order_flow_sql, conn_lh)

        flag_1, fina_df = order_and_user_merge(order_flow_df, crm_user_df)
        if not flag_1:
            # 10000
            return {"code": fina_df, "status": "failed", "msg": message[fina_df]}

        # 如果存在运营中心参数
        if operateid:
        # 如果不存在运营中心参数
            match_result = if_exist_operate_match_data(fina_df, operateid, request, 'buyer_phone', 'order')
            if not match_result[0]:
                return {"code": match_result[1], "status": "failed", "msg": message[match_result[1]]}
            match_df = match_result[1]
        else:
            # 不管是否有参数，先进行数据匹配
            match_result = not_exist_operate_match_data(fina_df, request, 'buyer_phone', mode='order')
            if not match_result[0]:
                return {"code": match_result[1], "status": "failed", "msg": message[match_result[1]]}
            match_df = match_result[1]
        match_df.drop(['sell_name'], axis=1, inplace=True)
        match_df.fillna("", inplace=True)
        match_df = map_type(match_df)
        match_df['order_time'] = match_result[1]['order_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        match_dict_list = match_df.to_dict('records')
        return_data = {
            "data": match_dict_list
        }
        return {"code": "0000", "status": "success", "msg": return_data,"count": match_result[2].shape[0]}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
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
            if len(request.json) != 14:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

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
            transfer_id = str(request.json['transfer_id'])

            # 每页显示条数
            size = request.json['size']
            # 页码
            page = request.json['page']

            num = size
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

        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_lh or not conn_crm:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        publish_order_df = pd.read_sql(publish_sql, conn_lh)

        crm_user_sql = '''select t1.*, t2.parent_phone from 
            (select id sell_unionid, pid parentid, phone sell_phone, if(`name` is not null,`name`,if(nickname is not null,nickname,"")) sell_name from luke_sincerechat.user where phone is not null or phone != "") t1
            left join
            (select id, phone parent_phone from luke_sincerechat.user where phone is not null or phone != "") t2
            on t1.parentid = t2.id'''
        crm_user_df = pd.read_sql(crm_user_sql, conn_crm)
        fina_df = publish_order_df.merge(crm_user_df, how='left', on='sell_phone')
        fina_df['status'] = fina_df['status'].astype(str)
        fina_df['transfer_type'] = fina_df['transfer_type'].astype(str)
        fina_df['transfer_type'].fillna(3, inplace=True)
        fina_df['transfer_type'] = fina_df['transfer_type'].astype(str)
        fina_df['sell_unionid'] = fina_df['sell_unionid'].astype(str)
        fina_df['sell_unionid'] = fina_df['sell_unionid'].apply(lambda x: del_point(x))
        fina_df['parentid'] = fina_df['parentid'].astype(str)
        fina_df['parentid'] = fina_df['parentid'].apply(lambda x: del_point(x))

        if operateid:
        # 如果不存在运营中心参数
            ############################
            match_result = if_exist_operate_match_data(fina_df, operateid, request, 'sell_phone', 'publish')
            if not match_result[0]:
                return {"code": match_result[1], "status": "failed", "msg": message[match_result[1]]}
            match_df = match_result[1]
        else:
            match_result = not_exist_operate_match_data(fina_df, request, 'sell_phone', mode='publish')
            if not match_result[0]:
                return {"code": match_result[1], "status": "failed", "msg": message[match_result[1]]}
            match_df = match_result[1]
        match_df['publish_time'] = match_df['publish_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        match_df['up_time'] = match_df['up_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        try:
            match_df['sell_time'] = match_df['sell_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
        match_df['sell_time'] = match_df['sell_time'].astype(str)
        match_df['sell_time'] = match_df['sell_time'].apply(lambda x: x.replace("NaT", ""))
        match_df['sell_time'] = match_df['sell_time'].apply(lambda x: x.replace("nan", ""))
        match_df.fillna("", inplace=True)
        # 类型映射
        match_df = map_type(match_df)
        match_dict_list = match_df.to_dict('records')
        return_data = {
            "data": match_dict_list
        }
        return {"code": "0000", "status": "success", "msg": return_data, "count": match_result[2].shape[0]}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
            conn_crm.close()
        except:
            pass

# 个人转卖市场订单数据统计分析
@pmbp.route("total",methods=["POST"])
def personal_total():
    try:

        conn_read = direct_get_conn(lianghao_mysql_conf)

        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

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
        parent_id = ""
        bus_phone = []

        # 模糊查询
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
            else:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        # 只查一个
        if parent:
            if len(parent) == 11:
                result = get_parent_by_phone(parent)
                if result[0] == 1:
                    parent_id = result[1]
                else:
                    return {"code": "11014", "status": "failed", "msg": message["code"]}
            else:
                parent_id = parent

        if bus_id:
            result = get_busphne_by_id(bus_id)
            if result[0] == 1:
                bus_phone = result[1].split(",")
            else:
                return {"code":"11015","status":"failed","msg":message["11015"]}

        logger.info(len(bus_phone))

        # 对手机号码差交集
        if keyword_phone and bus_phone:
            query_phone = list(set(keyword_phone).intersection(set(bus_phone)))
            if not query_phone:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        elif keyword_phone:
            query_phone = keyword_phone
        elif bus_phone:
            query_phone = bus_phone
        else:
            query_phone = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        logger.info("--------------------")
        logger.info(query_phone)
        logger.info("-------------------")

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

        df_list = []
        df_list.append(order_data)
        df_list.append(sell_order)
        df_list.append(public_order)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        #无数据返回空
        if df_merged.empty:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        #这里要进行一个crm数据的合并
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        # sql = '''select id unionid,pid parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname from luke_sincerechat.user where phone is not null or phone != ""'''
        sql = '''select unionid,parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user_%s where phone != "" and phone is not null''' %current_time
        logger.info(sql)
        crm_data = pd.read_sql(sql, conn_analyze)
        conn_analyze.close()
        df_merged = df_merged.merge(crm_data, how="left", on="phone")

        df_merged["parentid"] = df_merged['parentid'].astype(str)
        df_merged["unionid"] = df_merged['unionid'].astype(str)
        df_merged['parentid'] = df_merged['parentid'].apply(lambda x: del_point(x))
        df_merged['unionid'] = df_merged['unionid'].apply(lambda x: del_point(x))
        if parent_id:
            df_merged = df_merged[df_merged["parentid"] == parent_id]

        # 算全部的钱
        all_data = {"buy_count": 0, "buy_total_count": 0, "buy_total_price": 0, "sell_count": 0, "sell_fee": 0,
                    "sell_real_money": 0, "sell_total_count": 0, "sell_total_price": 0}

        # #把nan都填充0
        df_merged["buy_count"].fillna(0,inplace=True)
        df_merged["buy_total_count"].fillna(0,inplace=True)
        df_merged["buy_total_price"].fillna(0,inplace=True)
        df_merged["sell_count"].fillna(0,inplace=True)
        df_merged["sell_fee"].fillna(0,inplace=True)
        df_merged["sell_real_money"].fillna(0,inplace=True)
        df_merged["sell_total_count"].fillna(0,inplace=True)
        df_merged["sell_total_price"].fillna(0,inplace=True)
        df_merged["publish_sell_count"].fillna(0,inplace=True)
        df_merged["publish_total_count"].fillna(0,inplace=True)
        df_merged["publish_total_price"].fillna(0,inplace=True)

        df_merged["buy_total_count"] = df_merged["buy_total_count"].astype(int)
        df_merged["publish_total_count"] = df_merged["publish_total_count"].astype("int")
        df_merged["sell_total_count"] = df_merged["sell_total_count"].astype("int")

        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()


        all_data["buy_count"] = int(df_merged["buy_count"].sum())
        all_data["buy_total_count"] = int(df_merged["buy_total_count"].sum())
        all_data["buy_total_price"] = round(df_merged["buy_total_price"].sum(), 2)
        all_data["sell_count"] = int(df_merged["sell_count"].sum())
        all_data["sell_fee"] = round(df_merged["sell_fee"].sum(), 2)
        all_data["sell_real_money"] = round(df_merged["sell_real_money"].sum(), 2)
        all_data["sell_total_count"] = int(df_merged["sell_total_count"].sum())
        all_data["sell_total_price"] = round(df_merged["sell_total_price"].sum(), 2)

        # need_data = need_data.fillna("",inplace=True)
        need_data.fillna("",inplace=True)
        msg_data = {"data": need_data.to_dict("records"), "all_data": all_data}
        return {"code": "0000", "status": "success", "msg": msg_data, "count": len(df_merged)}


    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()



'''个人转卖市场采购数据分析总'''
@pmbp.route("buy/all",methods=["POST"])
def personal_buy_all():
    try:
        conn_read = direct_get_conn(lianghao_mysql_conf)

        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result


        page = request.json["page"]
        size = request.json["size"]

        # 可以是用户名称 手机号 unionid 模糊的
        keyword = request.json["keyword"]

        # 查询归属上级 精准的
        parent = request.json["parent"]

        bus_id = request.json["bus_id"]
        # 必须传年月日时分秒
        first_start_time = request.json["first_start_time"]
        first_end_time = request.json["first_end_time"]
        last_start_time = request.json["last_start_time"]
        last_end_time = request.json["last_end_time"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_id = ""
        bus_phone = []

        time_condition_sql = ""

        if first_start_time and first_end_time:
            if first_start_time >= first_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
        if last_start_time and last_end_time:
            if last_start_time >= last_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}


        time_condition_sql = ""
        if first_start_time and first_end_time and last_start_time and last_end_time:

            # 11.2 11.5 10.31-11.1 no
            if last_end_time > first_start_time:
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
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        # 只查一个
        if parent:
            if len(parent) == 11:
                result = get_parent_by_phone(parent)
                if result[0] == 1:
                    parent_id = result[1]
                else:
                    return {"code": "11014", "status": "failed", "msg": message["code"]}
            else:
                parent_id = parent


        if bus_id:
            result = get_busphne_by_id(bus_id)
            if result[0] == 1:
                bus_phone = result[1].split(",")
            else:
                return {"code":"11015","status":"failed","msg":message["11015"]}

        # 对手机号码差交集
        if keyword_phone and bus_phone:
            query_phone = list(set(keyword_phone).intersection(set(bus_phone)))
            if not query_phone:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        elif keyword_phone:
            query_phone = keyword_phone
        elif bus_phone:
            query_phone = bus_phone
        else:
            query_phone = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        buy_sql = '''select phone,total_price,create_time from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
        if time_condition_sql:
            buy_sql = buy_sql+time_condition_sql

        group_sql = ''' group by phone'''
        if query_phone:
            condition_sql = ''' and phone in (%s)''' % (",".join(query_phone))
            order_sql = buy_sql+condition_sql
        else:
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
        last_data["last_time"] = last_data['last_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

        sum_data = order_data.sort_values("create_time", ascending=True).groupby("phone").sum("total_price").reset_index()
        logger.info(sum_data)
        count_data = order_data.sort_values("create_time", ascending=True).groupby("phone").count().reset_index().drop("create_time",axis=1)
        count_data.rename(columns={"phone":"phone","total_price":"count"},inplace=True)

        df_list = []
        df_list.append(first_data)
        df_list.append(last_data)
        df_list.append(sum_data)
        df_list.append(count_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        if first_start_time and first_end_time:
            df_merged = df_merged[(df_merged["first_time"] >= first_start_time) & (df_merged["first_time"] <= first_end_time)]
            logger.info(df_merged.shape)
        if last_start_time and last_end_time:
            df_merged = df_merged[(df_merged["last_time"] >= last_start_time) & (df_merged["last_time"] <= last_end_time)]

        # 无数据返回空
        if df_merged.empty:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        df_merged_count = len(df_merged)
        logger.info(df_merged.shape)


        conn_analyze = direct_get_conn(analyze_mysql_conf)
        sql = '''select unionid,parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user_%s where phone != "" and phone is not null''' %current_time
        crm_data = pd.read_sql(sql, conn_analyze)
        conn_analyze.close()


        df_merged = df_merged.merge(crm_data, how="left", on="phone")


        # 转类型
        df_merged["parentid"] = df_merged['parentid'].astype(str)
        df_merged["unionid"] = df_merged['unionid'].astype(str)
        df_merged['parentid'] = df_merged['parentid'].apply(lambda x: del_point(x))
        df_merged['unionid'] = df_merged['unionid'].apply(lambda x: del_point(x))
        if parent_id:
            df_merged = df_merged[df_merged["parentid"] == parent_id]

        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()

        last_data.fillna("", inplace=True)

        last_data = need_data.to_dict("records")

        last_data[0]["total_price"] = round(last_data[0]["total_price"],2)

        logger.info("last_data:%s" %last_data)
        return {"code": "0000", "status": "success", "msg": last_data, "count": df_merged_count}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()



'''个人转卖市场采购数据分析--个人'''
@pmbp.route("buy",methods=["POST"])
def person_buy():
    try:
        conn_read = direct_get_conn(lianghao_mysql_conf)
        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result


        phone = request.json["phone"]

        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = int(request.json["time_type"])
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        if time_type == 4:
            if not start_time or not end_time:
                return {"code":"10001","status":"failed","msg":message["10001"]}
            if start_time >= end_time:
                return {"code":"11020","status":"failed","msg":message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")
            datetime_end_time = datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")
            daysss = datetime_end_time-datetime_start_time
            if daysss.days+ daysss.seconds/(24.0*60.0*60.0) > 30:
                return {"code":"11018","status":"failed","msg":message["11018"]}



        if not phone:
            return {"code":"10001","status":"failed","msg":message["10001"]}

        cursor = conn_read.cursor()
        sql = '''select o.create_time,o.total_price,o.pay_type,s.pretty_type_name,o.count from lh_order o 
        left join lh_sell s on o.sell_id = s.id
        where o.phone = %s and o.del_flag = 0 and o.type in (1,4)  and o.`status` = 1
        order by create_time asc'''
        cursor.execute(sql,(phone))
        datas = cursor.fetchall()

        # logger.info(datas)

        first_data={"order_time":"","order_total_price":"","order_pay":"","order_type":"","order_count":""}
        second_data={"order_time":"","order_total_price":"","order_pay":"","order_type":"","order_count":""}
        last_data={"order_time":"","order_total_price":"","order_pay":"","order_type":"","order_count":""}

        personal_datas = {"first":first_data,"second":second_data,"last":last_data,"person":{}}
        try:
            first_data["order_time"] = datetime.datetime.strftime(datas[0][0], "%Y-%m-%d %H:%M:%S")
            first_data["order_total_price"] = datas[0][1]
            first_data["order_pay"] = datas[0][2]
            first_data["order_type"] = datas[0][3]
            first_data["order_count"] = datas[0][4]
        except:
            pass

        try:
            second_data["order_time"] = datetime.datetime.strftime(datas[1][0], "%Y-%m-%d %H:%M:%S")
            second_data["order_total_price"] = datas[1][1]
            second_data["order_pay"] = datas[1][2]
            second_data["order_type"] = datas[1][3]
            second_data["order_count"] = datas[1][4]
        except:
            pass

        try:
            if len(datas)>2:
                last_data["order_time"] = datetime.datetime.strftime(datas[-1][0], "%Y-%m-%d %H:%M:%S")
                last_data["order_total_price"] = datas[-1][1]
                last_data["order_pay"] = datas[-1][2]
                last_data["order_type"] = datas[-1][3]
                last_data["order_count"] = datas[-1][4]
        except:
            pass


        #通过手机号码直接查运营中心字段 并返回 nickname operate_name parent_phone parentid phone unionid
        crm_sql = '''select unionid,phone,parentid,parent_phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user_%s where phone = %s''' %(current_time,phone)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        user_data = pd.read_sql(crm_sql,conn_analyze)
        user_data = user_data.to_dict("records")
        if not user_data:
            # return {"code": "0000", "status": "success", "msg": "暂无该用户数据"}
            return {"code": "0000", "status": "success", "msg": [], "count": 0}


        personal_datas["person"] = user_data


        #获取所有的数据

        all_sql = '''select count(*) order_count,sum(count) total_count,sum(total_price) total_price,GROUP_CONCAT(pay_type) sum_pay_type from lh_order where del_flag = 0 and type in (1,4) and `status`=1 group by phone having phone = %s'''
        cursor.execute(all_sql,(phone))
        datas = cursor.fetchone()
        user_order_data = {"order_total_price":datas[2],"order_count":datas[0],"total_count":datas[1],"pay_type":datas[3]}
        order_data = {"user_order_data":user_order_data}

        # 今天
        last_data = {}

        if time_type == 1 or (time_type == 4 and daysss and daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0)<1):
            #今日

            if time_type == 1:
                circle_sql1 = '''select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''
            else:
                query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d")
                yesterday_query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d")
                circle_sql1 = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(query_time)
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(yesterday_query_time)

            # #直接拼接sql 不然会有很多重复的代码 很烦人
            circle_sql = ""

            condition_sql = " and phone = %s" %phone
            circle_sql = circle_sql1 +condition_sql + circle_conn + circle_sql2 + condition_sql

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today":circle_data[0][0],"today_buy_total_price":circle_data[0][1],"today_buy_order_count":circle_data[0][2],
                "yesterday":circle_data[1][0],"yes_buy_total_price":circle_data[1][1],"yes_buy_order_count":circle_data[1][2]
            }

            if time_type == 1:
                today_sql = '''select DATE_FORMAT(create_time, '%Y-%m-%d %H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
            else:
                today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d %%H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %query_time
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            today_sql = today_sql + condition_sql + group_order_sql

            logger.info("today_sql:%s" %today_sql)
            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            order_data["circle"] = circle
            order_data["today"] = today

        elif time_type == 2 or time_type == 3:
            query_range = []
            if time_type == 2:
                query_range = ["-0","-6","-7","-13"]
            elif time_type == 3:
                query_range = ["-0","-29","-30","-59"]
            circle_sql = '''
            select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc) a
            union all
            select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)   group by statistic_time order by statistic_time asc) b ''' %(phone,query_range[0],query_range[1],phone,query_range[2],query_range[3])

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],
                "today_buy_order_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1],
                "yes_buy_order_count": circle_data[1][2]
            }

            # 查询近七天的
            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc ''' %(phone,query_range[0],query_range[1])
            logger.info(today_sql)
            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)

            order_data["circle"] = circle
            order_data["today"] = today
        elif time_type == 4:
            sub_day = int(daysss.days+1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(start_time)
            logger.info(end_time)
            logger.info(before_start_time)
            logger.info(before_end_time)
            circle_sql = '''
                        select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) a
                        union all
                        select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) b ''' %(phone,end_time,start_time,phone,before_end_time,before_start_time)

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)

            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],
                "today_buy_order_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1],
                "yes_buy_order_count": circle_data[1][2]
            }


            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and phone = %s and create_time <= "%s" and create_time >= "%s" group by statistic_time order by statistic_time asc ''' %(phone,end_time,start_time)

            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)

            order_data["circle"] = circle
            order_data["today"] = today
        else:
            return {"code":"11009","status":"failed","msg":message["11009"]}

        datas = {"person":personal_datas,"order_data":order_data}
        return {"code":"0000","status":"success","msg":datas}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


'''个人转卖市场出售数据分析总'''
@pmbp.route("sell/all", methods=["POST"])
def personal_sell_all():
    try:

        conn_read = direct_get_conn(lianghao_mysql_conf)

        logger.info(request.json)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        page = request.json["page"]
        size = request.json["size"]

        # 可以是用户名称 手机号 unionid 模糊的
        keyword = request.json["keyword"]

        # 查询归属上级 精准的
        parent = request.json["parent"]

        bus_id = request.json["bus_id"]
        # 必须传年月日时分秒
        first_start_time = request.json["first_start_time"]
        first_end_time = request.json["first_end_time"]
        last_start_time = request.json["last_start_time"]
        last_end_time = request.json["last_end_time"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_id = ""
        bus_phone = []

        time_condition_sql = ""

        if first_start_time and first_end_time:
            if first_start_time >= first_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
        if last_start_time and last_end_time:
            if last_start_time >= last_end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}

        if first_start_time and first_end_time and last_start_time and last_end_time:

            # 11.2 11.5 10.31-11.1 no
            if last_end_time > first_start_time:
                pass
            else:
                return {"code": "11019", "status": "failed", "msg": message[["11019"]]}

        # 模糊查询
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
            else:
                # return {"code": "11016", "status": "failed", "msg": message["11016"]}
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        # 只查一个
        if parent:
            if len(parent) == 11:
                result = get_parent_by_phone(parent)
                if result[0] == 1:
                    parent_id = result[1]
                else:
                    return {"code": "11014", "status": "failed", "msg": message["code"]}
            else:
                parent_id = parent

        if bus_id:
            result = get_busphne_by_id(bus_id)
            if result[0] == 1:
                bus_phone = result[1].split(",")
            else:
                return {"code": "11015", "status": "failed", "msg": message["11015"]}

        # 对手机号码差交集

        if keyword_phone and bus_phone:
            query_phone = list(set(keyword_phone).intersection(set(bus_phone)))
            if not query_phone:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        elif keyword_phone:
            query_phone = keyword_phone
        elif bus_phone:
            query_phone = bus_phone
        else:
            query_phone = ""

        if page and size:
            code_page = (page - 1) * size
            code_size = page * size

        buy_sql = '''select sell_phone phone,total_price,create_time from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
        if time_condition_sql:
            buy_sql = buy_sql + time_condition_sql

        group_sql = ''' group by sell_phone'''
        if query_phone:
            condition_sql = ''' and sell_phone in (%s)''' % (",".join(query_phone))
            order_sql = buy_sql + condition_sql
        else:
            order_sql = buy_sql

        # 返回条数

        logger.info("order_sql:%s" % order_sql)
        order_data = pd.read_sql(order_sql, conn_read)
        order_data_group = order_data.groupby("phone")

        # 排序取出按时间第一条和最后一条的
        first_data = order_data.sort_values("create_time", ascending=True).groupby("phone").first().reset_index()
        first_data.rename(columns={"phone": "phone", "create_time": "first_time", "total_price": "first_total_price"},
                          inplace=True)
        first_data["first_time"] = first_data['first_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))

        last_data = order_data.sort_values("create_time", ascending=True).groupby("phone").last().reset_index()
        last_data.rename(columns={"phone": "phone", "create_time": "last_time", "total_price": "last_total_price"},inplace=True)
        last_data["last_time"] = last_data['last_time'].dt.strftime("%Y-%m-%d %H:%M:%S")

        sum_data = order_data.sort_values("create_time", ascending=True).groupby("phone").sum("total_price").reset_index()
        count_data = order_data.sort_values("create_time", ascending=True).groupby("phone").count().reset_index().drop("create_time", axis=1)
        count_data.rename(columns={"phone": "phone", "total_price": "count"}, inplace=True)

        df_list = []
        df_list.append(first_data)
        df_list.append(last_data)
        df_list.append(sum_data)
        df_list.append(count_data)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        if first_start_time and first_end_time:
            df_merged = df_merged[(df_merged["first_time"] >= first_start_time) & (df_merged["first_time"] <= first_end_time)]
            logger.info(df_merged.shape)
        if last_start_time and last_end_time:
            df_merged = df_merged[(df_merged["last_time"] >= last_start_time) & (df_merged["last_time"] <= last_end_time)]

        # 无数据返回空
        if df_merged.empty:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}

        result_count = len(df_merged)

        conn_analyze = direct_get_conn(analyze_mysql_conf)
        sql = '''select unionid,parentid,phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user_%s where phone != "" and phone is not null''' % current_time
        crm_data = pd.read_sql(sql, conn_analyze)
        conn_analyze.close()

        df_merged = df_merged.merge(crm_data,how="left",on="phone")

        # 转类型
        df_merged["parentid"] = df_merged['parentid'].astype(str)
        df_merged["unionid"] = df_merged['unionid'].astype(str)
        df_merged['parentid'] = df_merged['parentid'].apply(lambda x: del_point(x))
        df_merged['unionid'] = df_merged['unionid'].apply(lambda x: del_point(x))
        if parent_id:
            df_merged = df_merged[df_merged["parentid"] == parent_id]

        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()
        last_data.fillna("", inplace=True)
        last_data = need_data.to_dict("records")
        last_data[0]["total_price"] = round(last_data[0]["total_price"], 2)
        return {"code": "0000", "status": "success", "msg": last_data, "count": result_count}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()



'''个人转卖市场出售数据分析--个人'''
@pmbp.route("sell", methods=["POST"])
def person_sell():
    try:
        logger.info(request.json)
        conn_read = direct_get_conn(lianghao_mysql_conf)

        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        sell_phone = request.json["sell_phone"]

        # 1 今日 2 本周 3 本月  4 可选择区域
        time_type = int(request.json["time_type"])
        start_time = request.json["start_time"]
        end_time = request.json["end_time"]

        # sub_time = ""
        daysss = ""
        if time_type == 4:
            if not start_time or not end_time:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}
            if start_time >= end_time:
                return {"code": "11020", "status": "failed", "msg": message["11020"]}
            datetime_start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            datetime_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            daysss = datetime_end_time - datetime_start_time
            if daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0) > 30:
                return {"code": "11018", "status": "failed", "msg": message["11018"]}

            # 获取两个起始时间相减判断是否一天
            # sub_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

        if not sell_phone:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        cursor = conn_read.cursor()
        sql = '''select o.create_time,o.total_price,o.pay_type,s.pretty_type_name,o.count from lh_order o 
        left join lh_sell s on o.sell_id = s.id
        where o.sell_phone = %s and o.del_flag = 0 and o.type in (1,4)  and o.`status` = 1
        order by create_time asc'''
        cursor.execute(sql, (sell_phone))
        datas = cursor.fetchall()


        first_data = {"order_time": "", "order_total_price": "", "order_pay": "", "order_type": "", "order_count": ""}
        second_data = {"order_time": "", "order_total_price": "", "order_pay": "", "order_type": "", "order_count": ""}
        last_data = {"order_time": "", "order_total_price": "", "order_pay": "", "order_type": "", "order_count": ""}

        personal_datas = {"first": first_data, "second": second_data, "last": last_data, "person": {}}
        try:
            first_data["order_time"] = datetime.datetime.strftime(datas[0][0], "%Y-%m-%d %H:%M:%S")
            first_data["order_total_price"] = datas[0][1]
            first_data["order_pay"] = datas[0][2]
            first_data["order_type"] = datas[0][3]
            first_data["order_count"] = datas[0][4]
        except:
            pass

        try:
            second_data["order_time"] = datetime.datetime.strftime(datas[1][0], "%Y-%m-%d %H:%M:%S")
            second_data["order_total_price"] = datas[1][1]
            second_data["order_pay"] = datas[1][2]
            second_data["order_type"] = datas[1][3]
            second_data["order_count"] = datas[1][4]
        except:
            pass

        try:
            if len(datas) > 2:
                last_data["order_time"] = datetime.datetime.strftime(datas[-1][0], "%Y-%m-%d %H:%M:%S")
                last_data["order_total_price"] = datas[-1][1]
                last_data["order_pay"] = datas[-1][2]
                last_data["order_type"] = datas[-1][3]
                last_data["order_count"] = datas[-1][4]
        except:
            pass

        crm_sql = '''select unionid,phone,parentid,parent_phone,if(`name` is not null,`name`,if(nickname is not null,nickname,"")) nickname,operatename operate_name from crm_user_%s where phone = %s''' % (current_time, sell_phone)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        user_data = pd.read_sql(crm_sql, conn_analyze)
        user_data = user_data.to_dict("records")
        if not user_data:
            # return {"code": "0000", "status": "success", "msg": "暂无该用户数据"}
            return {"code": "0000", "status": "success", "msg": [], "count": 0}


        # 获取所有的数据

        all_sql = '''select count(*) order_count,sum(count) total_count,sum(total_price) total_price,GROUP_CONCAT(pay_type) sum_pay_type from lh_order where del_flag = 0 and type in (1,4) and `status`=1 group by sell_phone having sell_phone = %s'''
        cursor.execute(all_sql, (sell_phone))
        datas = cursor.fetchone()
        user_order_data = {"order_total_price": datas[2], "order_count": datas[0], "total_count": datas[1],
                           "pay_type": datas[3]}
        order_data = {"user_order_data": user_order_data}

        # 今天
        last_data = {}

        if time_type == 1 or (time_type == 4 and daysss and daysss.days + daysss.seconds / (24.0 * 60.0 * 60.0)<1):
            #今日
            logger.info("进来了")
            if time_type == 1:
                circle_sql1 = '''select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE()'''
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%Y-%m-%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = date_add(CURRENT_DATE(),INTERVAL -1 day)'''
            else:
                query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")).strftime("%Y-%m-%d")
                yesterday_query_time = (datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d")
                circle_sql1 = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(query_time)
                circle_conn = " union all"
                circle_sql2 = ''' select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,if(sum(total_price),sum(total_price),0) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %(yesterday_query_time)

            # #直接拼接sql 不然会有很多重复的代码 很烦人
            circle_sql = ""

            condition_sql = " and sell_phone = %s" %sell_phone
            circle_sql = circle_sql1 +condition_sql + circle_conn + circle_sql2 + condition_sql

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today":circle_data[0][0],"today_buy_total_price":circle_data[0][1],"today_buy_order_count":circle_data[0][2],
                "yesterday":circle_data[1][0],"yes_buy_total_price":circle_data[1][1],"yes_buy_order_count":circle_data[1][2]
            }

            if time_type == 1:
                today_sql = '''select DATE_FORMAT(create_time, '%Y-%m-%d %H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%Y%m%d') = CURRENT_DATE'''
            else:
                today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d %%H') AS statistic_time,count(*) buy_order_count,if(sum(count),sum(count),0) buy_total_count,if(sum(total_price),sum(total_price),0) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and DATE_FORMAT(create_time, '%%Y-%%m-%%d') = "%s"''' %query_time
            group_order_sql = ''' group by statistic_time order by statistic_time desc'''

            today_sql = today_sql + condition_sql + group_order_sql

            logger.info("today_sql:%s" %today_sql)
            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in reversed(today_data):
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[1])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[3])
                today.append(td_dict)
            logger.info(today)
            order_data["circle"] = circle
            order_data["today"] = today
        elif time_type == 2 or time_type == 3:
            query_range = []
            if time_type == 2:
                query_range = ["-0", "-6", "-7", "-13"]
            elif time_type == 3:
                query_range = ["-0", "-29", "-30", "-59"]
            circle_sql = '''
            select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc) a
            union all
            select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
            select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day)   group by statistic_time order by statistic_time asc) b ''' % (
            sell_phone, query_range[0], query_range[1], sell_phone, query_range[2], query_range[3])

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)
            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],
                "today_buy_order_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1],
                "yes_buy_order_count": circle_data[1][2]
            }

            # 查询近七天的
            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone = %s and DATE_FORMAT(create_time, '%%Y-%%m-%%d')<=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) and DATE_FORMAT(create_time, '%%Y-%%m-%%d')>=DATE_ADD(CURRENT_DATE(),INTERVAL %s day) group by statistic_time order by statistic_time asc ''' % (
            sell_phone, query_range[0], query_range[1])
            logger.info(today_sql)
            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[3])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[1])
                today.append(td_dict)
            logger.info(today)

            order_data["circle"] = circle
            order_data["today"] = today
        elif time_type == 4:
            sub_day = int(daysss.days + 1)
            before_start_time = (datetime_start_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            before_end_time = (datetime_end_time + datetime.timedelta(days=-sub_day)).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(start_time)
            logger.info(end_time)
            logger.info(before_start_time)
            logger.info(before_end_time)
            circle_sql = '''
                        select "current" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) a
                        union all
                        select "last" week,if(sum(buy_total_price),sum(buy_total_price),0) buy_total_price,if(sum(buy_order_count),sum(buy_order_count),0) buy_order_count from(
                        select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,sum(total_price) buy_total_price,count(*) buy_order_count from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone = %s and create_time<="%s" and create_time>="%s" group by statistic_time order by statistic_time asc) b ''' % (
            sell_phone, end_time, start_time, sell_phone, before_end_time, before_start_time)

            logger.info(circle_sql)
            cursor.execute(circle_sql)
            circle_data = cursor.fetchall()
            logger.info(circle_data)

            circle = {
                "today": circle_data[0][0], "today_buy_total_price": circle_data[0][1],
                "today_buy_order_count": circle_data[0][2],
                "yesterday": circle_data[1][0], "yes_buy_total_price": circle_data[1][1],
                "yes_buy_order_count": circle_data[1][2]
            }

            today_sql = '''select DATE_FORMAT(create_time, '%%Y-%%m-%%d') statistic_time,count(*) buy_order_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) and sell_phone = %s and create_time <= "%s" and create_time >= "%s" group by statistic_time order by statistic_time asc ''' % (
            sell_phone, end_time, start_time)

            cursor.execute(today_sql)
            today_data = cursor.fetchall()
            logger.info(today_data)
            today = []
            for td in today_data:
                td_dict = {}
                td_dict["statistic_time"] = td[0]
                td_dict["buy_order_count"] = int(td[3])
                td_dict["buy_total_count"] = float(td[2])
                td_dict["buy_total_price"] = float(td[1])
                today.append(td_dict)
            logger.info(today)

            order_data["circle"] = circle
            order_data["today"] = today
        else:
            return {"code": "11009", "status": "failed", "msg": message["11009"]}

        datas = {"person": personal_datas, "order_data": order_data}
        return {"code": "0000", "status": "success", "msg": datas}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


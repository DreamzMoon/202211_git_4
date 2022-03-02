# -*- coding: utf-8 -*-
# @Time : 2022/3/1  20:30
# @Author : shihong
# @File : .py
# --------------------------------------
'''个人名片网转让数据列表汇总'''
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime
from analyzeserver.user.sysuser import check_token
from analyzeserver.common import *
from functools import reduce

transummarybp = Blueprint('transummary', __name__, url_prefix='/lh/transummary')

@transummarybp.route('/user', methods=["POST"])
def user_summary():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 11:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            keyword = request.json['keyword'] # 关键词
            operate_id = request.json['operate_id'] # 归属运营中心
            parent = request.json['parent'] # 归属上级
            login_start_time = request.json['login_start_time'] # 最近登录起始时间
            login_end_time = request.json['login_end_time'] # 最近登录结束时间
            time_type = request.json['time_type'] # 时间类型
            start_time = request.json['start_time'] # 起始时间
            end_time = request.json['end_time'] # 结束时间
            page = request.json['page']
            size = request.json['size']
        except Exception as e:
            # 参数名错误
            logger.error(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        if (time_type != 4 and start_time and end_time) or time_type not in range(1, 5) or (
                time_type == 4 and not start_time and not end_time):
            return {"code": "10014", "status": "failed", "msg": message["10014"]}
        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_analyze or not conn_lh:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # 采购订单
        buy_sql = '''
            select phone, sell_phone, count,total_price, create_time, pay_type from lh_order where del_flag=0 and status=1 and type in (1,4) and phone is not null and phone != ''
        '''
        # 发布数据
        publish_sql = '''
            select sell_phone phone, count publish_count, total_price publish_total_price, create_time from lh_sell where del_flag=0 and sell_phone is not null and sell_phone != ''
        '''
        # 查询官方
        inside_recovery_phone_sql = '''
            select inside_recovery_phone from lh_analyze.data_board_settings where del_flag=0 and market_type=1
        '''
        # 靓号用户数据
        lh_user_sql = '''
            select phone, create_time, last_login_time from lh_user where phone is not null and phone !='' and del_flag=0
        '''
        # crm用户数据
        crm_user_sql = '''
            select parentid,phone,if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) nickname,operatename from crm_user where phone != "" and phone is not null and del_flag=0
        '''
        find_count = 0
        query_phone = []
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
                query_phone.extend(keyword_phone)
                find_count += 1
            else:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
        if parent: # 精确查找
            parent_sql = '''select phone from crm_user where del_flag=0'''
            if len(parent) == 11: # 手机号
                parent_sql += ''' and parent_phone=%s''' % parent
            else: # unionid
                parent_sql += ''' and parentid=%s''' % parent
            parent_df = pd.read_sql(parent_sql, conn_analyze)
            parent_phone = parent_df['phone'].tolist()
            if len(parent_phone) == 0:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
            if find_count == 0:
                query_phone.extend(parent_phone)
            else:
                query_phone = list(set(query_phone).intersection(set(parent_phone)))
            find_count += 1
        if operate_id:
            operate_sql = '''select phone from crm_user where operate_id = %s''' % operate_id
            phone_data = pd.read_sql(operate_sql,conn_analyze)
            operate_phone = phone_data['phone'].tolist()
            if len(operate_phone) == 0:
                return {"code": "11015", "status": "failed", "msg": message["11015"]}
            if find_count == 0:
                query_phone.extend(operate_phone)
            else:
                query_phone = list(set(query_phone).intersection(set(operate_phone)))
            find_count += 1
        if login_start_time and login_end_time:
            lh_user_condition_sql = lh_user_sql + ''' and last_login_time>="{start_time}" and last_login_time<="{end_time}"'''.format(start_time=login_start_time, end_time=login_end_time)
            lh_user_df = pd.read_sql(lh_user_condition_sql, conn_lh)
            lh_user_phone_list = lh_user_df['phone'].tolist()
            if len(lh_user_phone_list) == 0:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
            if find_count == 0:
                query_phone.extend(lh_user_phone_list)
            else:
                query_phone = list(set(query_phone).intersection(set(lh_user_phone_list)))
            find_count += 1

        # 判断是否有进行条件搜索
        if find_count > 0 and len(query_phone) == 0:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}
        elif len(query_phone) >0:
            lh_user_sql += ''' and phone in (%s)''' % ','.join(query_phone)
            buy_sql += ''' and phone in (%s)''' % ','.join(query_phone)
            publish_sql += ''' and sell_phone in (%s)''' % ','.join(query_phone)
        else: #  没有进行条件搜索的情况下，用靓号的手机号找crm_user
            pass
        # 采购数据
        buy_df = pd.read_sql(buy_sql, conn_lh)
        # 发布数据
        publish_df = pd.read_sql(publish_sql, conn_lh)
        # 读取靓号用户数据
        lh_user_df = pd.read_sql(lh_user_sql, conn_lh)

        df_list = []
        df_list.append(lh_user_df)
        # 最早采购
        first_buy_df = buy_df.sort_values("create_time", ascending=True).groupby("phone")[
            'create_time'].first().reset_index().rename(columns={"create_time": "first_buy_time"})
        df_list.append(first_buy_df)
        # 最近采购
        last_buy_df = buy_df.sort_values("create_time", ascending=True).groupby("phone")[
            'create_time'].last().reset_index().rename(columns={"create_time": "last_buy_time"})
        df_list.append(last_buy_df)
        # 最早上架
        first_publish_df = publish_df.sort_values("create_time", ascending=True).groupby("phone")[
            'create_time'].first().reset_index().rename(columns={"create_time": "first_publish_time"})
        df_list.append(first_publish_df)
        # 最近上架
        last_publish_df = publish_df.sort_values("create_time", ascending=True).groupby("phone")[
            'create_time'].last().reset_index().rename(columns={"create_time": "last_publish_time"})
        df_list.append(last_publish_df)
        #  时间选择
        to_day = datetime.date.today()  # 当前时间
        if time_type == 1:  # 今日
            buy_df = buy_df.loc[buy_df['create_time'].dt.date == to_day, :]
            publish_df = publish_df.loc[publish_df['create_time'].dt.date == to_day, :]
        elif time_type == 2:  # 周
            to_week = to_day + timedelta(days=-6)
            buy_df = buy_df.loc[(buy_df['create_time'].dt.date >= to_week) & (
                    buy_df['create_time'].dt.date <= to_day), :]
            publish_df = publish_df.loc[(publish_df['create_time'].dt.date >= to_week) & (
                    publish_df['create_time'].dt.date <= to_day), :]
        elif time_type == 3:  # 月
            to_month = to_day + timedelta(days=-29)
            buy_df = buy_df.loc[(buy_df['create_time'].dt.date >= to_month) & (
                    buy_df['create_time'].dt.date <= to_day), :]
            publish_df = publish_df.loc[(publish_df['create_time'].dt.date >= to_month) & (
                    publish_df['create_time'].dt.date <= to_day), :]
        else:  # 自定义时间
            buy_df = buy_df.loc[(buy_df['create_time'] >= start_time) & (
                    buy_df['create_time'] <= end_time), :]
            publish_df = publish_df.loc[(publish_df['create_time'] >= start_time) & (
                    publish_df['create_time'] <= end_time), :]

        # 采购总数量
        buy_count_df = buy_df.groupby('phone').agg({"count": "sum", "total_price": "sum"}).rename(
            columns={"count": "buy_count", "total_price": "buy_total_price"}).reset_index()
        df_list.append(buy_count_df)
        # 现金采购 (微信、支付宝)
        cache_buy_df = buy_df[buy_df['pay_type'].isin([3, 4])].groupby('phone')['total_price'].sum().reset_index().rename(columns={"total_price": "cache_total_price"})
        df_list.append(cache_buy_df)
        # 诚聊通余额
        surplus_buy_df = buy_df[buy_df['pay_type'].isin([1, 2])].groupby('phone')['total_price'].sum().reset_index().rename(columns={"total_price": "surplus_total_price"})
        df_list.append(surplus_buy_df)
        # 官方采购
        with conn_analyze.cursor() as cursor:
            cursor.execute(inside_recovery_phone_sql)
            inside_recovery_phone = cursor.fetchone()[0]
        if inside_recovery_phone: # 有官方号
            inside_recovery_phone = json.loads(inside_recovery_phone)
            # 官方采购
            official_buy_df = buy_df[buy_df['sell_phone'].isin(inside_recovery_phone)].groupby('phone').agg({"count": "sum", "total_price": "sum"}).reset_index()
        else: # 无官方号
            inside_recovery_phone = []
            official_buy_df = buy_df.loc[buy_df['sell_phone'].isin(inside_recovery_phone), ['phone', 'count', 'total_price']]
        official_buy_df.columns = ['phone', 'official_buy_count', 'official_total_price']
        df_list.append(official_buy_df)
        # 市场采购
        market_buy_df = buy_df[~buy_df['sell_phone'].isin(inside_recovery_phone)].groupby('phone').agg({"count": "sum", "total_price": "sum"}).reset_index()
        market_buy_df.columns = ['phone', 'market_buy_count', 'markey_total_price']
        df_list.append(market_buy_df)
        # 上架总数量
        publish_count_df = publish_df.groupby('phone').agg({"publish_count": "sum", "publish_total_price": "sum"})
        df_list.append(publish_count_df)

        fina_df = reduce(lambda left, right: pd.merge(left, right, how='outer', on=['phone']), df_list)
        fina_df['buy_count'].fillna(0, inplace=True)
        fina_df.sort_values('buy_count', ascending=False, inplace=True)

        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
            cut_df = fina_df[start_index:end_index]
        else:
            cut_df = fina_df.copy()
        cut_phone_list = cut_df['phone'].tolist()
        # 读取crm用户数据
        crm_user_sql += ''' and phone in (%s)''' % ','.join(cut_phone_list)
        crm_user_df = pd.read_sql(crm_user_sql, conn_analyze)
        # 合并最终数据
        cut_df = cut_df.merge(crm_user_df, how='left', on='phone')
        # 时间格式化
        for column in [columns for columns in cut_df.columns if 'time' in columns ]:
            cut_df[column] = cut_df[column].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if str(x) != "NaT" else '')
        # 数据圆整
        for column in [columns for columns in cut_df.columns if 'price' in columns ]:
            cut_df[column].fillna(0, inplace=True)
            cut_df[column] = cut_df[column].round(2)
        # 统计填充0
        for column in [columns for columns in cut_df.columns if 'count' in columns ]:
            cut_df[column].fillna(0, inplace=True)
        cut_df.fillna('', inplace=True)
        # 采购金 默认为空
        cut_df['purchase_money'] = 0
        return {"code": "0000", "status": "success", "msg": cut_df.to_dict("records"), "count": fina_df.shape[0]}
    except:
        conn_analyze.rollback()
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass
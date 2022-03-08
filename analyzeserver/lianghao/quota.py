# -*- coding: utf-8 -*-
# @Time : 2022/3/7  11:40
# @Author : shihong
# @File : .py
# --------------------------------------
'''转让市场数据增长分析'''
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
from functools import reduce
from analyzeserver.common import *
from dateutil.relativedelta import relativedelta

quotabp = Blueprint('quote', __name__, url_prefix='/lh/quota')

@quotabp.route('/user', methods=["POST"])
def user_quota():
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
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        table = 'quota_%s'
        table_time = datetime.datetime.now()
        addtime = datetime.datetime.now().strftime("%Y-%m-%d")

        # 今日最新数据
        quota_today_sql = '''
            select a.* from quota_today a
            join (select phone, max(addtime) time from lh_analyze.quota_today group by phone) b
            on a.phone = b.phone
            and a.addtime = b.time
        '''
        # 历史数据
        quota_sql = '''
            select * from %s where {time}
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
        if parent:  # 精确查找
            parent_sql = '''select phone from crm_user where del_flag=0 and phone is not null and phone !=""'''
            if len(parent) == 11:  # 手机号
                parent_sql += ''' and parent_phone=%s''' % parent
            else:  # unionid
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
            phone_data = pd.read_sql(operate_sql, conn_analyze)
            operate_phone = phone_data['phone'].tolist()
            if len(operate_phone) == 0:
                return {"code": "11015", "status": "failed", "msg": message["11015"]}
            if find_count == 0:
                query_phone.extend(operate_phone)
            else:
                query_phone = list(set(query_phone).intersection(set(operate_phone)))
            find_count += 1
        if login_start_time and login_end_time:
            quota_condition_sql = quota_today_sql + ''' where last_login_time>="{start_time}" and last_login_time<="{end_time}"'''.format(
                start_time=login_start_time, end_time=login_end_time)
            quota_df = pd.read_sql(quota_condition_sql, conn_analyze)
            quota_phone_list = quota_df['phone'].tolist()
            if len(quota_phone_list) == 0:
                return {"code": "0000", "status": "success", "msg": [], "count": 0}
            if find_count == 0:
                query_phone.extend(quota_phone_list)
            else:
                query_phone = list(set(query_phone).intersection(set(quota_phone_list)))
            find_count += 1

        # 判断是否有进行条件搜索
        if find_count > 0 and len(query_phone) == 0:
            return {"code": "0000", "status": "success", "msg": [], "count": 0}
        elif len(query_phone) > 0:
            logger.info(query_phone)
            quota_condition_sql = quota_today_sql + ''' where a.phone in (%s)''' % ','.join(query_phone)
        else:  # 没有进行条件搜索的情况下，用靓号的手机号找crm_user
            quota_condition_sql = quota_today_sql
        logger.info(quota_condition_sql)

        # 额度数据
        quota_today_df = pd.read_sql(quota_condition_sql, conn_analyze)
        logger.info(quota_today_df)
        quota_today_df.sort_values('market_buy_limit', ascending=False, inplace=True)
        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
            part_quota_df = quota_today_df[start_index: end_index]
        else:
            part_quota_df = quota_today_df.copy()

        # 读取数据库表
        show_tables = '''show tables'''
        tables_df = pd.read_sql(show_tables, conn_analyze)
        tables_df.columns = ['table_name']


        part_quota_phone_list = part_quota_df['phone'].tolist()
        base_sql = quota_sql + ''' and phone in (%s)''' % ','.join(part_quota_phone_list)

        to_day = datetime.date.today()  # 当前时间
        # 这里需要判断是否需要跨表，跨了几张表，跨的表是否存在
        table_name_list = []
        span_table = False
        span_table_count = 0
        if time_type == 1:  # 今日
            time_data_sql = base_sql.format(time='''addtime="%s"''' % to_day)
            # 环比
            to_day_ratio = to_day + timedelta(days=-1)
            if to_day_ratio.month != to_day.month:
                span_table = True
                span_table_count = to_day.month - to_day_ratio.month
                for i in range(span_table_count + 1):
                    logger.info(table_time + relativedelta(months=-i))
                    table_name = table % (table_time + relativedelta(months=-i)).strftime('%Y%m')
                    logger.info(table_name)
                    table_shape = tables_df[tables_df['table_name'] == table_name].shape[0]
                    if table_shape > 0:
                        table_name_list.append(table_name)
            time_data_ration_sql = base_sql.format(time='''addtime="%s"''' % to_day_ratio)
            logger.info(time_data_sql)
            logger.info(time_data_ration_sql)
            logger.info(table_name_list)
            return '11111'
        elif time_type == 2:  # 周
            to_week = to_day + timedelta(days=-6)
            time_data_sql = base_sql.format(time='''addtime>="%s" and addtime<="%s"''' % (to_week, to_day))
            # 环比
            to_week_end_ratio = to_week + timedelta(days=-1)
            to_week_start_ratio = to_week_end_ratio + timedelta(days=-6)
            time_data_ration_sql = base_sql.format(time='''addtime>="%s" and addtime<="%s"''' % (to_week_start_ratio, to_week_end_ratio))
        elif time_type == 3:  # 月
            to_month = to_day + timedelta(days=-29)
            time_data_sql = base_sql.format(time='''addtime>="%s" and addtime<="%s"''' % (to_month, to_day))
            # 环比
            to_month_end_ratio = to_month + timedelta(days=-1)
            to_month_start_ratio = to_month_end_ratio + timedelta(days=-29)
            time_data_ration_sql = base_sql.format(
                time='''addtime>="%s" and addtime<="%s"''' % (to_month_start_ratio, to_month_end_ratio))
        else:  # 自定义时间
            time_data_sql = base_sql.format(time='''addtime>="%s" and addtime<="%s"''' % (start_time, end_time))

            # 同比
            # 自定义时间间隔
            start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d")
            end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d")
            sub_day = end_time.day - start_time.day + 1
            # 同比时间
            custom_start_ratio = start_time + timedelta(days=-sub_day)
            custom_end_ratio = end_time + timedelta(days=-sub_day)
            time_data_ration_sql = base_sql.format(
                time='''addtime>="%s" and addtime<="%s"''' % (custom_start_ratio, custom_end_ratio))

        logger.info(time_data_sql)
        logger.info("-" * 50)
        logger.info(time_data_ration_sql)
        # time_data_df = pd.read_sql(time_data_sql, conn_analyze)
        # time_data_ration_df = pd.read_sql(time_data_ration_sql, conn_analyze)
        return '1111111111'
        # 如果匹配到数据大于0，再进行统计
        if time_data_df.shape[0] > 0:
            if time_type == 1 or (
                    time_type == 4 and request.json['start_time'].date() == request.json['end_time'].date()):
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

    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass

@quotabp.route('/plat', methods=["POST"])
def plat_quota():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 9:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            logger.info(check_token_result)
            if check_token_result["code"] != "0000":
                return check_token_result

            unionid_lists = [str(x).strip() for x in request.json["unionid_lists"]]
            phone_lists = [x.strip() for x in request.json["phone_lists"]]
            bus_lists = [str(x).strip() for x in request.json["bus_lists"]]

            start_time = request.json["start_time"]
            end_time = request.json["end_time"]
            tag_id = request.json["tag_id"]
            page = request.json['page']
            size = request.json['size']
        except Exception as e:
            # 参数名错误
            logger.error(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_analyze or not conn_lh:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        cursor_analyze = conn_analyze.cursor()

    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
            conn_analyze.close()
        except:
            pass


@quotabp.route("/operate",methods=["POST"])
def operate_quota():
    try:
        conn_read = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        logger.info(check_token_result)
        if check_token_result["code"] != "0000":
            return check_token_result

        logger.info("args:%s" %request.json)
        keyword = request.json.get("keyword")
        operate_id = request.json.get("operate_id")
        login_start_time = request.json.get("login_start_time")
        login_end_time = request.json.get("login_end_time")

        # 时间类型 1今天 2本周 3本月 4自定义
        time_type = request.json.get("time_type")
        order_start_time = request.json.get("order_start_time")
        order_end_time = request.json.get("order_end_time")

        page = request.json.get("page")
        size = request.json.get("size")

        #查询那些事官方号码
        official_upload_phone = ""
        official_sql = '''select * from data_board_settings where market_type = 1'''
        setting_data = pd.read_sql(official_sql,conn_analyze).to_dict("records")[0]

        logger.info(setting_data)
        if setting_data["inside_publish_phone"]:
            official_upload_phone = json.loads(setting_data["inside_publish_phone"])
        logger.info(official_upload_phone)

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()
        conn_analyze.close()
# -*- coding: utf-8 -*-
# @Time : 2022/2/9  14:12
# @Author : shihong
# @File : .py
# --------------------------------------
import sys
sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
from functools import reduce


transactionbp = Blueprint('transaction', __name__, url_prefix='/clg/transaction')

def status_data(df):
    data = {}
    # 交易订单数
    data['order_count'] = int(df['pay_order_count'].sum() + df['voucher_order_count'].sum())
    # 交易金额
    data['total_money'] = round(df['pay_money'].sum() + df['voucher_pay_money'].sum(), 2)
    # 已使用抵用金
    data['use_voucher_money'] = round(df['voucher_money'].sum(), 2)
    # 已取消订单金额--7、11
    cancel_df = df[(df['order_status'] == 7) | (df['order_status'] == 11)]
    data['cancel_order_count'] = int(cancel_df['pay_order_count'].sum() + cancel_df['voucher_order_count'].sum())
    data['cancel_order_money'] = round(cancel_df['pay_money'].sum() + cancel_df['voucher_pay_money'].sum(), 2)
    # 已退款--12
    refund_df = df[df['order_status'] == 12]
    data['refund_order_count'] = int(refund_df['pay_order_count'].sum() + refund_df['voucher_order_count'].sum())
    data['refund_order_money'] = round(refund_df['pay_money'].sum() + refund_df['voucher_pay_money'].sum(),2)
    # 有效 3,4,5,6,10,15
    success_list = [3, 4, 5, 6, 10, 15]
    success_df = df[df['order_status'].isin(success_list)]
    data['success_order_count'] = int(success_df['pay_order_count'].sum() + success_df['voucher_order_count'].sum())
    data['success_order_money'] = round(success_df['pay_money'].sum() + success_df['voucher_pay_money'].sum(), 2)
    # 交易运费
    data['freight_money'] = round(df['pay_freight_money'].sum() + df['voucher_freight_money'].sum(), 2)
    return data


@transactionbp.route("/plat/all", methods=["POST"])
def plat_all():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 7:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            unionid_lists = [x.strip() for x in request.json["unionid_lists"]]
            phone_lists = [x.strip() for x in request.json["phone_lists"]]
            bus_lists = [x.strip() for x in request.json["bus_lists"]]

            tag_id = request.json["tag_id"]

            start_time = request.json["start_time"]
            end_time = request.json["end_time"]
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_clg = direct_get_conn(clg_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze or not conn_clg:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        cursor_analyze = conn_analyze.cursor()
        cursor_clg = conn_clg.cursor()

        # 交易商品数量
        goods_num_sql = '''
            select sum(t2.buy_num) buy_num from trade_order_info t1
            left join trade_order_item t2
            on t1.order_sn=t2.order_sn
            where t1.del_flag=0 and t1.phone is not null and t2.del_flag=0
        '''
        # 持有抵用金
        hold_voucher_money_sql = '''select sum(available_voucher_money) hold_voucher_money from member_user where del_flag=0'''
        # 现金支付
        pay_money_sql = '''
            select order_status, count(*) pay_order_count, sum(pay_money) pay_money, sum(freight_money) pay_freight_money from trade_order_info
            where del_flag=0 and voucherMoneyType=1
        '''
        # 抵用金支付
        voucher_pay_sql = '''
            select order_status, count(*) voucher_order_count, sum(voucherMoney) voucher_money, sum(voucherPayMoney) voucher_pay_money
            from trade_order_info
            where del_flag=0 and voucherMoneyType=2
        '''
        # 状态分组sql
        status_group = ''' group by order_status'''

        # 条件筛选
        args_phone_lists = []
        if phone_lists:
            args_phone_lists = phone_lists.copy()
        elif unionid_lists:
            try:
                unionid_sql = '''select phone from crm_user where find_in_set (unionid, %s)'''
                ags_list = ",".join(unionid_lists)
                cursor_analyze.execute(unionid_sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for phone in phone_lists:
                    args_phone_lists.append(phone[0])
            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}
        elif bus_lists:
            str_bus_lists = ",".join(bus_lists)
            bus_sql = '''select not_contains from operate_relationship_crm where find_in_set (id, %s) and crm = 1 and del_flag = 0'''
            cursor_analyze.execute(bus_sql, str_bus_lists)
            phone_lists = cursor_analyze.fetchall()
            for phone in phone_lists:
                args_phone_lists.extend(eval(phone[0]))


        tag_phone_list = []
        tag_id_flag = False
        if tag_id:
            tag_id_flag = True
            phone_result = find_tag_user_phone(tag_id)
            if phone_result[0]:
                tag_phone_list = phone_result[1]
            else:
                return {"code": phone_result[1], "status": "failed", "message": message[phone_result[1]]}

        null_all_data = {
            "order_count": 0,
            "goods_num": 0,
            "total_money": 0,
            "voucher_money": 0,
            "hold_voucher_money": 0,
            "cancel_order_count": 0,
            "cancel_order_money": 0,
            "refund_order_count": 0,
            "refund_order_money": 0,
            "success_order_count": 0,
            "success_order_money": 0,
            "freight_money": 0,
        }
        null_today_data = null_all_data
        null_data = {
            "all_data": null_all_data,
            "today_data": null_today_data
        }
        # 1.如果有进行标签查找,不存在
        if len(tag_phone_list) == 0 and tag_id_flag:
            return {"code": "0000", "status": "success", "msg": null_data}
        # 2.如果有进行标签查找，存在
        if len(tag_phone_list) > 0:
            # 剔除过滤的手机号
            condition_phone_list = [phone for phone in tag_phone_list if phone not in args_phone_lists]
            # 如果剔除后没有手机号 返回空
            if len(condition_phone_list) == 0:
                return {"code": "0000", "status": "success", "msg": null_data}
            goods_num_sql += ''' and t1.phone in (%s)''' % ','.join(condition_phone_list)
            hold_voucher_money_sql += ''' and phone in (%s)''' % ','.join(condition_phone_list)
            pay_money_sql += ''' and phone in (%s)''' % ','.join(condition_phone_list)
            voucher_pay_sql += ''' and phone in (%s)''' % ','.join(condition_phone_list)
        else: # 未进行标签查找
            # 如果存在过滤
            if len(args_phone_lists) > 0:
                goods_num_sql += ''' and t1.phone not in (%s)''' % ','.join(args_phone_lists)
                hold_voucher_money_sql += ''' and phone not in (%s)''' % ','.join(args_phone_lists)
                pay_money_sql += ''' and phone not in (%s)''' % ','.join(args_phone_lists)
                voucher_pay_sql += ''' and phone not in (%s)''' % ','.join(args_phone_lists)
            else: # 不存在过滤
                pass
        # 今日sql
        today_goods_num_sql = goods_num_sql + ''' and date_format(t1.create_time, "%Y-%m-%d")=current_date'''
        # today_hold_voucher_money = hold_voucher_money + ''''''
        today_pay_money_sql = pay_money_sql + ''' and date_format(create_time, "%Y-%m-%d")=current_date'''
        today_voucher_pay_sql = voucher_pay_sql + ''' and date_format(create_time, "%Y-%m-%d")=current_date'''
        # 时间过滤
        if start_time and end_time:
            goods_num_sql += ''' and date_format(t1.create_time, "%Y-%m-%d") >= "{}" and date_format(t1.create_time, "%Y-%m-%d") <= "{}"'''.format(start_time, end_time)
            # hold_voucher_money += ''' and phone not in (%s)''' % ','.join(args_phone_lists)
            pay_money_sql += ''' and date_format(create_time, "%Y-%m-%d") >= "{}" and date_format(create_time, "%Y-%m-%d") <= "{}"'''.format(start_time, end_time)
            voucher_pay_sql += ''' and date_format(create_time, "%Y-%m-%d") >= "{}" and date_format(create_time, "%Y-%m-%d") <= "{}"'''.format(start_time, end_time)
        # 总sql与今日sql加上分组
        pay_money_sql += status_group
        voucher_pay_sql += status_group

        today_pay_money_sql += status_group
        today_voucher_pay_sql += status_group

        # 商品数
        cursor_clg.execute(goods_num_sql)
        goods_num = int(cursor_clg.fetchone()[0])

        # 持有中抵用金
        cursor_clg.execute(hold_voucher_money_sql)
        hold_voucher_money = round(cursor_clg.fetchone()[0], 2)

        # 现金支付与抵用金支付
        pay_money_df = pd.read_sql(pay_money_sql, conn_clg)
        voucher_pay_df = pd.read_sql(voucher_pay_sql, conn_clg)

        fina_df = pay_money_df.merge(voucher_pay_df, how='outer', on='order_status')
        all_data = status_data(fina_df)
        all_data['goods_num'] = goods_num
        all_data['hold_voucher_money'] = hold_voucher_money

        # 今日数据
        # 商品数
        cursor_clg.execute(today_goods_num_sql)
        today_goods_num = int(cursor_clg.fetchone()[0])

        # 现金支付与抵用金支付
        today_pay_money_df = pd.read_sql(today_pay_money_sql, conn_clg)
        today_voucher_pay_df = pd.read_sql(today_voucher_pay_sql, conn_clg)

        today_fina_df = today_pay_money_df.merge(today_voucher_pay_df, how='outer', on='order_status')
        today_data = status_data(today_fina_df)
        today_data['goods_num'] = today_goods_num
        today_data['hold_voucher_money'] = hold_voucher_money

        return_data = {
            "all_data": all_data,
            "today_data": today_data
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
        except:
            pass

@transactionbp.route("/plat/chart", methods=["POST"])
def plat_chart():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 8:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            unionid_lists = [x.strip() for x in request.json["unionid_lists"]]
            phone_lists = [x.strip() for x in request.json["phone_lists"]]
            bus_lists = [x.strip() for x in request.json["bus_lists"]]

            tag_id = request.json["tag_id"]

            time_type = request.json['time_type']
            start_time = request.json["start_time"]
            end_time = request.json["end_time"]
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
        # 数据库连接
        conn_clg = direct_get_conn(clg_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze or not conn_clg:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        cursor_analyze = conn_analyze.cursor()
        cursor_clg = conn_clg.cursor()

        # 交易商品数量
        goods_num_sql = '''
            select date_format(t1.create_time, "{}") create_time, t1.order_status, sum(t2.buy_num) buy_num from trade_order_info t1
            left join trade_order_item t2
            on t1.order_sn=t2.order_sn
            where t1.del_flag=0 and t1.phone is not null and t2.del_flag=0
        '''
        # 现金支付
        pay_money_sql = '''
            select date_format(create_time, "{}") create_time, order_status, count(*) pay_order_count, sum(pay_money) pay_money from trade_order_info
            where del_flag=0 and voucherMoneyType=1
        '''
        # 抵用金支付
        voucher_pay_sql = '''
            select date_format(create_time, "{}") create_time, order_status, count(*) voucher_order_count, sum(voucherMoney) voucher_money, sum(voucherPayMoney) voucher_pay_money
            from trade_order_info
            where del_flag=0 and voucherMoneyType=2
        '''
        status_group = ''' group by date_format(create_time, "{}"), order_status'''
        # 状态分组sql
        if time_type not in (1, 4):
            format_time = "%Y-%m-%d"
        else:
            # 判断是否为同一天
            if start_time.split(' ')[0] == end_time.split(' ')[0]:
                format_time = "%Y-%m-%d %H"
            else:
                format_time = "%Y-%m-%d"
        goods_num_sql = goods_num_sql.format(format_time)
        pay_money_sql = pay_money_sql.format(format_time)
        voucher_pay_sql = voucher_pay_sql.format(format_time)

        status_group = status_group.format(format_time)

        # 条件筛选
        args_phone_lists = []
        if phone_lists:
            args_phone_lists = phone_lists.copy()
        elif unionid_lists:
            try:
                unionid_sql = '''select phone from crm_user where find_in_set (unionid, %s)'''
                ags_list = ",".join(unionid_lists)
                cursor_analyze.execute(unionid_sql, ags_list)
                phone_lists = cursor_analyze.fetchall()
                for phone in phone_lists:
                    args_phone_lists.append(phone[0])
            except Exception as e:
                logger.exception(e)
                return {"code": "10006", "status": "failed", "msg": message["10006"]}
        elif bus_lists:
            str_bus_lists = ",".join(bus_lists)
            bus_sql = '''select not_contains from operate_relationship_crm where find_in_set (id, %s) and crm = 1 and del_flag = 0'''
            cursor_analyze.execute(bus_sql, str_bus_lists)
            phone_lists = cursor_analyze.fetchall()
            for phone in phone_lists:
                args_phone_lists.extend(eval(phone[0]))


        tag_phone_list = []
        tag_id_flag = False
        if tag_id:
            tag_id_flag = True
            phone_result = find_tag_user_phone(tag_id)
            if phone_result[0]:
                tag_phone_list = phone_result[1]
            else:
                return {"code": phone_result[1], "status": "failed", "message": message[phone_result[1]]}

        null_ration_data = {
            "now_order_count": 0,
            "now_total_money": 0,
            "ration_order_count": 0,
            "ration_total_money": 0
        }
        null_data = {
            "time_data": [],
            "ration_data": null_ration_data
        }
        # 1.如果有进行标签查找,不存在
        if len(tag_phone_list) == 0 and tag_id_flag:
            return {"code": "0000", "status": "success", "msg": null_data}
        # 2.如果有进行标签查找，存在
        if len(tag_phone_list) > 0:
            # 剔除过滤的手机号
            condition_phone_list = [phone for phone in tag_phone_list if phone not in args_phone_lists]
            # 如果剔除后没有手机号 返回空
            if len(condition_phone_list) == 0:
                return {"code": "0000", "status": "success", "msg": null_data}
            goods_num_sql += ''' and t1.phone in (%s)''' % ','.join(condition_phone_list)
            pay_money_sql += ''' and phone in (%s)''' % ','.join(condition_phone_list)
            voucher_pay_sql += ''' and phone in (%s)''' % ','.join(condition_phone_list)
        else: # 未进行标签查找
            # 如果存在过滤
            if len(args_phone_lists) > 0:
                goods_num_sql += ''' and t1.phone not in (%s)''' % ','.join(args_phone_lists)
                pay_money_sql += ''' and phone not in (%s)''' % ','.join(args_phone_lists)
                voucher_pay_sql += ''' and phone not in (%s)''' % ','.join(args_phone_lists)
            else: # 不存在过滤
                pass

        # 时间过滤
        time_info = {}
        to_day = datetime.date.today()  # 当前时间
        if time_type == 1:  # 今日
            today_sql = ''' and date_format({table}create_time, "%%Y-%%m-%%d")="%s"''' % to_day
            # 环比
            to_day_ratio = to_day + timedelta(days=-1)
            ration_sql = ''' and date_format({table}create_time, "%%Y-%%m-%%d")="%s"''' % to_day_ratio
        elif time_type == 2:  # 周
            to_week = to_day + timedelta(days=-6)
            today_sql = ''' and date_format({table}create_time, "%%Y-%%m-%%d")>="%s" and date_format({table}create_time, "%%Y-%%m-%%d")<="%s"''' % (to_week, to_day)

            # 环比
            to_week_end_ratio = to_week + timedelta(days=-1)
            to_week_start_ratio = to_week_end_ratio + timedelta(days=-6)
            ration_sql = ''' and date_format({table}create_time, "%%Y-%%m-%%d")>="%s" and date_format({table}create_time, "%%Y-%%m-%%d")<="%s"''' % (to_week_start_ratio, to_week_end_ratio)
        elif time_type == 3:  # 月
            to_month = to_day + timedelta(days=-29)
            today_sql = ''' and date_format({table}create_time, "%%Y-%%m-%%d")>="%s" and date_format({table}create_time, "%%Y-%%m-%%d")<="%s"''' % (to_month, to_day)

            # 环比
            to_month_end_ratio = to_month + timedelta(days=-1)
            to_month_start_ratio = to_month_end_ratio + timedelta(days=-29)
            ration_sql = ''' and date_format({table}create_time, "%%Y-%%m-%%d")>="%s" and date_format({table}create_time, "%%Y-%%m-%%d")<="%s"''' % (to_month_start_ratio, to_month_end_ratio)

        else:  # 自定义时间
            today_sql = ''' and {table}create_time>="%s" and {table}create_time<="%s"''' % (start_time, end_time)
            # 环比
            # 自定义时间间隔
            sub_day = request.json['end_time'].day - request.json['start_time'].day + 1
            logger.info(sub_day)
            # 环比时间
            custom_start_ratio = request.json['start_time'] + timedelta(days=-sub_day)
            custom_end_ratio = request.json['end_time'] + timedelta(days=-sub_day)

            ration_sql = ''' and {table}create_time>="%s" and {table}create_time<="%s"''' % (custom_start_ratio, custom_end_ratio)


        now_goods_num_sql = goods_num_sql + today_sql.format(table="t1.")
        now_pay_money_sql = pay_money_sql + today_sql.format(table="")
        now_voucher_pay_sql = voucher_pay_sql + today_sql.format(table="")

        # 环比数据
        # ration_goods_num_sql = goods_num_sql + ration_sql.format(table="t1.")
        ration_pay_money_sql = pay_money_sql + ration_sql.format(table="")
        ration_voucher_pay_sql = voucher_pay_sql + ration_sql.format(table="")

        # 总sql与今日sql加上分组
        now_goods_num_sql += status_group
        # ration_goods_num_sql += status_group

        now_pay_money_sql += status_group
        ration_pay_money_sql += status_group

        now_voucher_pay_sql += status_group
        ration_voucher_pay_sql += status_group

        # 读取时间数据
        now_goods_num = pd.read_sql(now_goods_num_sql, conn_clg)
        now_pay_money = pd.read_sql(now_pay_money_sql, conn_clg)
        now_voucher_pay = pd.read_sql(now_voucher_pay_sql, conn_clg)
        merge_df = now_pay_money.merge(now_goods_num, how='outer', on=['create_time', 'order_status'])
        merge_df = merge_df.merge(now_voucher_pay, how='outer', on=['create_time', 'order_status'])
        if merge_df.shape[0] == 0:
            # 如果查询数据为空
            ration_pay_money = pd.read_sql(ration_pay_money_sql, conn_clg)
            ration_voucher_pay = pd.read_sql(ration_voucher_pay_sql, conn_clg)
            null_data["ration_data"]['ration_total_money'] = round(
                ration_pay_money['pay_money'].sum() + ration_voucher_pay['voucher_pay_money'].sum(), 2)
            null_data["ration_data"]['ration_order_count'] = int(
                ration_pay_money['pay_order_count'].sum() + ration_voucher_pay['voucher_order_count'].sum())
            return {"code": "0000", "status": "success", "msg": null_data}
        merge_df.fillna(0, inplace=True)
        merge_df['order_count'] = merge_df['pay_order_count'] + merge_df['voucher_order_count']
        merge_df['total_money'] = merge_df['pay_money'] + merge_df['voucher_pay_money']
        merge_df.drop(['pay_order_count', 'voucher_order_count', 'pay_money', 'voucher_pay_money'], axis=1, inplace=True)

        fina_data = merge_df.drop_duplicates('create_time').loc[:, ['create_time']].sort_values('create_time', ascending='False').reset_index(drop=True)

        ### 已取消订单金额--7、11 已退款--12 有效 3,4,5,6,10,15
        merge_df_list = []
        # 总数据
        all_group_df = merge_df.groupby('create_time').sum().reset_index().sort_values('create_time', ascending='False')
        # 有效订单
        success_list = [3, 4, 5, 6, 10, 15]
        success_df = merge_df.loc[
            merge_df['order_status'].isin(success_list), ['create_time', 'order_count', 'total_money',
                                                          'voucher_money']].groupby(
            'create_time').sum().reset_index().sort_values('create_time', ascending='False')
        success_df.columns = ['create_time', 'success_order_count', 'success_pay_money', 'success_voucher_money']
        merge_df_list.append(success_df)
        refund_df = merge_df.loc[
            merge_df['order_status'] == 12, ['create_time', 'order_count', 'total_money', 'voucher_money']].groupby(
            'create_time').sum().reset_index().sort_values('create_time', ascending='False')
        refund_df.columns = ['create_time', 'refund_order_count', 'refund_pay_money', 'refund_voucher_money']
        merge_df_list.append(refund_df)
        cancel_df = merge_df.loc[merge_df['order_status'].isin([7, 11]), ['create_time', 'order_count', 'total_money',
                                                                          'voucher_money']].groupby(
            'create_time').sum().reset_index().sort_values('create_time', ascending='False')
        cancel_df.columns = ['create_time', 'cancel_order_count', 'cancel_pay_money', 'cancel_voucher_money']
        merge_df_list.append(cancel_df)

        fina_data['pay_money'] = all_group_df['total_money']  # 交易金额
        fina_data['pay_voucher_money'] = all_group_df['voucher_money']  # 交易抵用金
        fina_data['order_count'] = all_group_df['order_count']  # 订单数量
        fina_data['goods_num'] = all_group_df['buy_num']  # 商品数量
        merge_df_list.insert(0, fina_data)
        fina_data = reduce(lambda left, right: pd.merge(left, right, on='create_time', how='left'), merge_df_list)
        fina_data.fillna(0, inplace=True)
        # 价格元整
        for i in [column for column in fina_data.columns if 'money' in column]:
            fina_data[i] = fina_data[i].apply(lambda x: round(float(x), 2))

        # 环比数据
        ration_pay_money = pd.read_sql(ration_pay_money_sql, conn_clg)
        ration_voucher_pay = pd.read_sql(ration_voucher_pay_sql, conn_clg)
        ration_data = {}
        ration_data['now_total_money'] = round(fina_data['pay_money'].sum(), 2)
        ration_data['now_order_count'] = int(fina_data['order_count'].sum())
        ration_data['ration_total_money'] = round(
            ration_pay_money['pay_money'].sum() + ration_voucher_pay['voucher_pay_money'].sum(), 2)
        ration_data['ration_order_count'] = int(
            ration_pay_money['pay_order_count'].sum() + ration_voucher_pay['voucher_order_count'].sum())

        return_data = {
            "time_data": fina_data.to_dict("records"),
            "ration_data": ration_data
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
        except:
            pass
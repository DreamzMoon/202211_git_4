# -*- coding: utf-8 -*-
# @Time : 2022/2/11  16:15
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


clgtranuserbp = Blueprint('clgtranuser', __name__, url_prefix='/clgtranuser')

@clgtranuserbp.route("/data", methods=["POST"])
def clg_user_tran():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 6:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            keyword = request.json['keyword']
            start_time = request.json["start_time"]
            end_time = request.json["end_time"]
            page = request.json['page']
            size = request.json['size']
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_clg = direct_get_conn(clg_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze or not conn_clg:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # 交易商品数量
        goods_num_sql = '''
            select t1.phone,sum(t2.buy_num) goods_num from trade_order_info t1
            left join trade_order_item t2
            on t1.order_sn=t2.order_sn
            where t1.del_flag=0 and t1.phone is not null and t1.phone != ""
        '''

        # 现金支付
        pay_money_sql = '''
            select phone, order_status, count(*) pay_order_count, sum(pay_money) pay_money, sum(refundMoney) refund_pay_money from trade_order_info
            where del_flag=0 and voucherMoneyType=1 and phone is not null and phone != ""
        '''
        # 抵用金支付
        voucher_pay_sql = '''
            select phone, order_status, count(*) voucher_order_count, sum(voucherMoney) voucher_money, sum(voucherPayMoney) voucher_pay_money,
            sum(refundMoney) refund_voucher_pay_money, sum(refundVoucherMoney) refund_voucher_money from trade_order_info
            where del_flag=0 and voucherMoneyType=2 and phone is not null and phone != ""
        '''
        # 状态分组sql
        status_group = ''' group by phone, order_status'''

        keyword_phone_list = []
        if keyword != "":
            keyword_result = get_phone_by_keyword(keyword)
            if not keyword_result[0]:
                return '空'
            keyword_phone_list = keyword_result[1]
        if len(keyword_phone_list) != 0: # 进行关键词搜索
            find_phone_sql = ''' and {table}phone in (%s)''' % ','.join(keyword_phone_list)
        else: # 不进行关键词搜索
            find_phone_sql = ''
        goods_num_sql += find_phone_sql.format(table='t1.')
        pay_money_sql += find_phone_sql.format(table='')
        voucher_pay_sql += find_phone_sql.format(table='')

        if start_time and end_time:
            time_sql = ''' and {table}create_time >=%s and {table}create_time<=%s''' % (start_time, end_time)
        else:
            time_sql = ''
        goods_num_sql += time_sql.format(table='t1.')
        pay_money_sql += time_sql.format(table='')
        voucher_pay_sql += time_sql.format(table='')

        goods_num_sql += ''' group by t1.phone'''
        pay_money_sql += status_group
        voucher_pay_sql += status_group

        # 读取数据
        goods_num = pd.read_sql(goods_num_sql, conn_clg)
        pay_money_df = pd.read_sql(pay_money_sql, conn_clg)
        voucher_pay_df = pd.read_sql(voucher_pay_sql, conn_clg)

        merge_df = pay_money_df.merge(voucher_pay_df, how='outer', on=['phone', 'order_status'])
        merge_df.fillna(0, inplace=True)

        merge_df['order_count'] = merge_df['pay_order_count'] + merge_df['voucher_order_count']
        merge_df['total_money'] = merge_df['pay_money'] + merge_df['voucher_pay_money']
        merge_df['refund_money'] = merge_df['refund_pay_money'] + merge_df['refund_voucher_pay_money']
        merge_df.drop(['pay_order_count', 'voucher_order_count', 'pay_money', 'voucher_pay_money', 'refund_pay_money',
                       'refund_voucher_pay_money'], axis=1, inplace=True)

        df_list = []

        union_phone_df = merge_df.drop_duplicates('phone').loc[:, ['phone']].reset_index(drop=True)
        df_list.append(union_phone_df)

        # 交易订单
        all_df = merge_df.groupby('phone')['order_count', 'total_money', 'voucher_money'].sum().reset_index()
        all_df = all_df.merge(goods_num, how='left', on='phone')
        df_list.append(all_df)

        # 有效订单
        success_list = [3, 4, 5, 6, 10, 15]
        success_df = merge_df[merge_df['order_status'].isin(success_list)].groupby('phone')[
            'order_count', 'total_money', 'voucher_money'].sum().reset_index()
        success_df.columns = ['phone', 'success_order_count', 'success_order_money', 'success_order_voucher_money']
        df_list.append(success_df)

        # 已退款订单
        refund_df = merge_df[merge_df['order_status'] == 12].groupby('phone')[
            'order_count', 'refund_money', 'refund_voucher_money'].sum().reset_index()
        refund_df.rename(columns={"order_count": "refund_order_count"}, inplace=True)
        df_list.append(refund_df)

        # 已取消订单
        cancel_df = merge_df[merge_df['order_status'].isin([7, 11])].groupby('phone')[
            'order_count', 'total_money', 'voucher_money'].sum().reset_index()
        cancel_df.columns = ['phone', 'cancel_order_count', 'cancel_order_money', 'cancel_order_voucher_money']
        df_list.append(cancel_df)

        fina_df= reduce(lambda left, right: pd.merge(left, right, on='phone', how='left'), df_list)
        fina_df.fillna(0, inplace=True)
        summary_data = fina_df.iloc[:, 1:].sum().to_dict()
        for i in [column for column, value in summary_data.items() if 'money' in column]:
            summary_data[i] = round(summary_data[i], 2)

        if page and size:
            logger.info('fsdfasfsdfa')
            start_index = (page - 1) * size
            end_index = page * size
        else:
            start_index = ''
            end_index = ''

        # 用户信息
        user_info_sql = '''select unionid, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) name, phone from lh_analyze.crm_user'''
        if start_index and end_index:
            logger.info('sdfasdfafsfsadfafdsf')
            cut_data = fina_df[start_index:end_index]
        else:
            cut_data = fina_df.copy()
        user_info_sql += ''' where phone in (%s)''' % ','.join(cut_data['phone'].tolist())

        user_info_df = pd.read_sql(user_info_sql, conn_analyze)
        cut_data = cut_data.merge(user_info_df, how='left', on='phone')
        cut_data.fillna('', inplace=True)
        logger.info(cut_data.shape)

        return_data = {
            "summary_data": summary_data,
            "data": cut_data.to_dict("records")
        }
        return {"code": "0000", "status": "success", "msg": return_data, "count": cut_data.shape[0]}
    except:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
            conn_analyze.close()
        except:
            pass

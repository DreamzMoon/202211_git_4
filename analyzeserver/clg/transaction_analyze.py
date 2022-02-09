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

            tag_id = request.json.get("tag_id")

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

        # 交易商品数量
        goods_num_sql = '''select sum(buy_num) buy_sum from trade_order_item where del_flag=0'''
        # 持有抵用金
        hold_voucher_money = '''select sum(available_voucher_money) hold_voucher_money from member_user'''
        # 现金支付
        pay_money_sql = '''
            select order_status, count(*) order_count, sum(total_money) total_money, sum(pay_money) pay_money, sum(refundMoney) refundMoney from trade_order_info
            where del_flag=0 and voucherMoneyType=1{} group by order_status
        '''
        # 抵用金支付
        voucher_pay_sql = '''
            select order_status, count(*) order_count, sum(total_money) total_money, sum(voucherMoney) voucherMoney, sum(voucherPayMoney) voucherPayMoney, 
            sum(refundVoucherMoney) refundVoucherMoney, sum(refundMoney) refundMoney from trade_order_info
            where del_flag=0 and voucherMoneyType=2{} group by order_status
        '''

        # 条件筛选
        args_phone_lists = []
        if phone_lists:
            args_phone_lists = phone_lists.copy()
        elif unionid_lists:
            try:
                unionid_sql = '''select phone from crm_user where find_in_set (unionid, %s)'''
                ags_list = ",".join(unionid_lists)
                logger.info(ags_list)
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

        # 1.如果有进行标签查找,不存在
        if len(tag_phone_list) and tag_id_flag:
            return '空'
        # 2.如果有进行标签查找，存在
        if len(tag_phone_list) > 0:
            # 剔除过滤的手机号
            condition_phone_list = [phone for phone in tag_phone_list if phone not in args_phone_lists]
            # 如果剔除后没有手机号 返回空
            if len(condition_phone_list) == 0:
                return '空'
            # 总
            goods_num_sql += ''' and phone in (%s)''' % ','.join(condition_phone_list)
            hold_voucher_money += ''' where phone in (%s)''' % ','.join(condition_phone_list)
            pay_money_sql = pay_money_sql.format(' and phone in (%s)' % ','.join(condition_phone_list))
            voucher_pay_sql = voucher_pay_sql.format(' and phone in (%s)' % ','.join(condition_phone_list))

            # 今日
            today_goods_num_sql = goods_num_sql + ''' and date_format(create_time, "%Y-%m-%s")=current_date'''
            today_hold_voucher_money = hold_voucher_money
            today_pay_money_sql = pay_money_sql
            today_voucher_pay_sql = voucher_pay_sql


    except:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_clg.close()
        except:
            pass
# -*- coding: utf-8 -*-
# @Time : 2021/11/6  17:02
# @Author : shihong
# @File : .py
# --------------------------------------
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from analyzeserver.common import *
from config import *
import traceback
from util.help_fun import *
import pandas as pd
from analyzeserver.user.sysuser import check_token

typebp = Blueprint('type', __name__, url_prefix='/lh/personal')

# 7位支付类型
@typebp.route("/pay_type/list", methods=["GET"])
def pay_type_list():
    try:
        map_pay_type = [
            {"pay_id": "-1", "pay_name": "未知"},
            {"pay_id": "0", "pay_name": "信用点支付"},
            {"pay_id": "1", "pay_name": "诚聊余额支付"},
            {"pay_id": "2", "pay_name": "诚聊通余额支付"},
            {"pay_id": "3", "pay_name": "微信支付"},
            {"pay_id": "4", "pay_name": "支付宝支付"},
            {"pay_id": "5", "pay_name": "后台系统支付"},
            {"pay_id": "6", "pay_name": "银行卡支付"},
            {"pay_id": "7", "pay_name": "诚聊通佣金支付"},
            {"pay_id": "8", "pay_name": "诚聊通红包支付"}
        ]
        return {"code":"0000","status":"success","msg": map_pay_type}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 8位支付类型
@typebp.route("/eight/pay_type/list", methods=["GET"])
def eight_pay_type_list():
    try:
        map_pay_type = [
            {"pay_id": "-1", "pay_name": "信用点"},
            {"pay_id": "0", "pay_name": "采购金"},
            {"pay_id": "1", "pay_name": "收银台"},
            {"pay_id": "2", "pay_name": "诚聊通余额支付"},
            {"pay_id": "3", "pay_name": "微信支付"},
            {"pay_id": "4", "pay_name": "支付宝支付"},
            {"pay_id": "5", "pay_name": "后台系统支付"},
            {"pay_id": "6", "pay_name": "银行卡支付"},
            {"pay_id": "8", "pay_name": "禄可商务转入"}
        ]
        return {"code":"0000","status":"success","msg": map_pay_type}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 转让类型
@typebp.route("/transfer_type/list", methods=["GET"])
def transfer_type():
    try:
        map_transfer_type = [
            {"transfer_id": "0", "transfer_name": "自主"},
            {"transfer_id": "1", "transfer_name": "市场"},
            {"transfer_id": "3", "transfer_name": "未知"}
        ]
        return {"code": "0000", "status": "success", "msg": map_transfer_type}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 状态
@ typebp.route("status_type/list", methods=["GET"])
def status_type():
    try:
        map_status = [
            {"status_id": "0", "status_name": "上架"},
            {"status_id": "1", "status_name": "下架"},
            {"status_id": "2", "status_name": "已出售"},
            {"status_id": "3", "status_name": "已下单"}
        ]
        return {"code": "0000", "status": "success", "msg": map_status}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 服务商
@typebp.route("server/type",methods=["GET"])
def server_type():
    try:
        serpro_grade = [
            {"serpro_grade_id": "1", "serpro_grade_name": "初级服务商"},
            {"serpro_grade_id": "2", "serpro_grade_name": "中级服务商"},
            {"serpro_grade_id": "3", "serpro_grade_name": "高级服务商"},
            {"serpro_grade_id": "4", "serpro_grade_name": "机构服务商"}
        ]
        return {"code": "0000", "status": "success", "msg": serpro_grade}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 商务身份
@typebp.route("bus/identify",methods=["GET"])
def bus_identify():
    try:
        serpro_grade = [
            {"capacity_id": "1", "capacity_name": "运营中心/公司"},
            {"capacity_id": "2", "capacity_name": "网店主"},
            {"capacity_id": "3", "capacity_name": "带货者"},
            {"capacity_id": "20", "capacity_name": "无身份(普通用户)"}
        ]
        return {"code": "0000", "status": "success", "msg": serpro_grade}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 名片网归属类型
@typebp.route("/belong/type/list",methods=["GET"])
def belong_type():
    try:
        # 时间
        belong_time = [
            {"status_id": "0", "status_name": "持有时间"},
            {"status_id": "1", "status_name": "可转让时间"},
            {"status_id": "2", "status_name": "使用时间"},
        ]
        # 转让类型
        belong_transfer = [
            {"status_id": "0", "status_name": "不可转让"},
            {"status_id": "1", "status_name": "可转让"},
        ]
        # 使用状态
        belong_use = [
            {"status_id": "0", "status_name": "已使用"},
            {"status_id": "1", "status_name": "未使用"},
        ]
        # 购买来源
        belong_source = [
            {"status_id": "0", "status_name": "官方订单"},
            {"status_id": "1", "status_name": "交易市场订单"},
            {"status_id": "2", "status_name": "预订单"},
            {"status_id": "3", "status_name": "零售订单"},
            {"status_id": "4", "status_name": "二手市场订单"},
        ]
        # 靓号位数
        belong_length = [
            {"status_id": "4", "status_name": "4位"},
            {"status_id": "5", "status_name": "5位"},
            {"status_id": "6", "status_name": "6位"},
            {"status_id": "7", "status_name": "7位"},
        ]
        belong_type = {
            "belong_time": belong_time,
            "belong_transfer": belong_transfer,
            "belong_use": belong_use,
            "belong_source": belong_source,
            "belong_length": belong_length,
        }
        return {"code": "0000", "status": "success", "msg": belong_type}
    except:
        return {"code": "10000", "status": "success", "msg": message["10000"]}

# 名片网归属时间类型
@typebp.route("/belong_time/type/list",methods=["GET"])
def belong_time_type():
    try:
        belong_time = [
            {"status_id": "0", "status_name": "持有时间"},
            {"status_id": "1", "status_name": "可转让时间"},
            {"status_id": "2", "status_name": "使用时间"},
        ]
        return {"code": "0000", "status": "success", "msg": belong_time}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 名片网归属转让类型
@typebp.route("/belong_transfer/type/list",methods=["GET"])
def belong_transfer_type():
    try:
        belong_transfer = [
            {"status_id": "0", "status_name": "不可转让"},
            {"status_id": "1", "status_name": "可转让"},
        ]
        return {"code": "0000", "status": "success", "msg": belong_transfer}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 名片网归属使用状态
@typebp.route("/belong_use/type/list",methods=["GET"])
def belong_use_type():
    try:
        belong_use = [
            {"status_id": "0", "status_name": "已使用"},
            {"status_id": "1", "status_name": "未使用"},
        ]
        return {"code": "0000", "status": "success", "msg": belong_use}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 名片网归属购买来源
@typebp.route("/belong_source/type/list",methods=["GET"])
def belong_order_source_type():
    try:
        belong_source = [
            {"status_id": "0", "status_name": "官方订单"},
            {"status_id": "1", "status_name": "交易市场订单"},
            {"status_id": "2", "status_name": "预订单"},
            {"status_id": "3", "status_name": "零售订单"},
            {"status_id": "4", "status_name": "二手市场订单"},
        ]
        return {"code": "0000", "status": "success", "msg": belong_source}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 名片网归属靓号位数
@typebp.route("/belong_length/type/list",methods=["GET"])
def belong_length_type():
    try:
        belong_length = [
            {"status_id": "4", "status_name": "4位"},
            {"status_id": "5", "status_name": "5位"},
            {"status_id": "6", "status_name": "6位"},
            {"status_id": "7", "status_name": "7位"},
        ]
        return {"code": "0000", "status": "success", "msg": belong_length}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# 名片网归属靓号类型
@typebp.route("/belong_pretty/type/list",methods=["GET"])
def belong_pretty_type():
    try:
        token = request.headers["Token"]
        user_id = request.args.get("user_id")

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        pretty_type_sql = '''
            select id, name from lh_pretty_type
        '''
        pretty_type_data = pd.read_sql(pretty_type_sql, conn_lh)
        pretty_type_data = pretty_type_data.to_dict("records")
        return {"code": "0000", "status": "success", "msg": pretty_type_data}
    except:
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
        except:
            pass
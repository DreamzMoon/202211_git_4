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

typebp = Blueprint('type', __name__, url_prefix='/lh/personal')

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
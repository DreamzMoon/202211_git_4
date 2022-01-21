# -*- coding: utf-8 -*-

# @Time : 2022/1/21 16:59

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : le_type.py

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

letypebp = Blueprint('letype', __name__, url_prefix='/le/type')

'''靓号类型'''
@letypebp.route("list",methods=["GET"])
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
            select id, name from le_pretty_type
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


# 名片网归属类型
@letypebp.route("/belong/list",methods=["GET"])
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
            {"status_id": "0", "status_name": "可转让"},
            {"status_id": "1", "status_name": "不可转让"},
            {"status_id": "2", "status_name": "转让中"},
            {"status_id": "3", "status_name": "已转让"}
        ]
        # 使用状态
        belong_use = [
            {"status_id": "0", "status_name": "已使用"},
            {"status_id": "1", "status_name": "未使用"},
        ]
        # 购买来源
        # belong_source = [
        #     {"status_id": "0", "status_name": "官方订单"},
        #     {"status_id": "1", "status_name": "交易市场订单"},
        #     {"status_id": "3", "status_name": "零售订单"},
        #     {"status_id": "4", "status_name": "二手市场订单"},
        #     {"status_id": "7", "status_name": "转让零售订单"},
        #     {"status_id": "10", "status_name": "禄可商务转入订单"},
        #     {"status_id": "11", "status_name": "赠送禄可商务免费八位域名"},
        #     {"status_id": "12", "status_name": "诚聊通转赠商业名片"},
        # ]
        belong_source = [
            {"status_id": "0", "status_name": "官方订单"},
            {"status_id": "1", "status_name": "交易市场订单"},
            {"status_id": "3", "status_name": "零售订单"},
            {"status_id": "4", "status_name": "二手市场订单"},
        ]

        # 靓号位数
        belong_length = [
            {"status_id": "8", "status_name": "8位"},
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
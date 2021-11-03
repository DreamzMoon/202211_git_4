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
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime

pmbp = Blueprint('personal', __name__, url_prefix='/lh/personal')

@pmbp.route("total",methods=["POST"])
def personal_total():
    try:
        conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)
        with conn_read.cursor() as cursor:
            sql = '''select count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price,
            count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,
            sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee 
            from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) '''
            cursor.execute(sql)
            all_datas = cursor.fetchall()
            logger.info(all_datas)

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


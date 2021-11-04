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

# 个人转卖市场顶发布数据分析
@pmbp.route('/publish')
def personal_publish():
    pass


@pmbp.route('/orderflow')
def personal_order_flow():
    order_flow_sql = '''
    select order_sn, nick_name, phone buyer_phone, pay_type, count, total_price buy_price, sell_phone, total_price sell_price, total_price - sell_fee true_price, sell_fee, order_time
    from lh_pretty_client.lh_order
    where `status`  = 1
    and type in (1, 4)
    and sell_phone is not null
    '''
    conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
    order_flow_data = pd.read_sql(order_flow_sql, conn_lh)

    # 获取用户数据

    # 买方数据
    # buyer_df = crm_user_df.merge(fina_df.loc[:, ['phone', 'operatename']], how='left', on='phone')
    # buyer_df.columns = ['buyer_unionid', 'parentid', 'buyer_phone', 'operatename']
    # # 卖方数据
    # sell_df = buyer_df.loc[:, ['buyer_unionid', 'buyer_phone']]
    # sell_df.columns = ['sell_unionid', 'sell_phone']
    # # # 用户数据与订单数据合并
    # order_flow_data = order_flow_data.merge(buyer_df, how='left', on='buyer_phone')
    # order_flow_data = order_flow_data.merge(sell_df, how='left', on='sell_phone')




    pass
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


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
from analyzeserver.common import *
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime
from functools import reduce
from analyzeserver.common import *

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
    user_data_df = get_all_user_operationcenter()



    # 返回用户数据
    return_data = {
        "order_sn": "", # 订单编码
        "name": "", # 名称
        "buyer_phone": "", # 买方手机号
        "buyer_unionid": "", # 买方unionid
        "operatename": "", # 归属运营中心
        "parentid": "", # 归属上级
        "count": "", # 靓号数量
        "buy_price": "", # 购买金额
        "transfer_type" # 转让类型
        "pay_type": "", # 支付类型
        "sell_unionid": "", # 卖方unionid
        "sell_phone": "", # 卖方手机号
        "sell_price": "", # 出售金额
        "true_price": "", # 到账金额
        "sell_fee": "", # 手续费
        "order_time": "", # 交易时间
    }


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

        logger.info(request.json)
        time.sleep(2)
        page = request.json["page"]
        size = request.json["size"]

        # 可以是用户名称 手机号 unionid 模糊的
        keyword = request.json["keyword"]

        # 查询归属上级 精准的
        parent = request.json["parent"]
        bus = request.json["bus"]

        # 字符串拼接的手机号码
        query_phone = ""

        if parent:
            if len(parent) == 11:
                query_phone = parent
            else:
                result = get_phone_by_unionid(parent)
                if result[0] == 1:
                    query_phone = result[1]
                else:
                    return {"code":"11014","status":"failed","msg":message["code"]}

        if bus:
            result = get_lukebus_phone([bus])
            if result[0] == 1:
                query_phone = result[1]
            else:
                return {"code":"11015","status":"failed","msg":message["11015"]}

        if keyword:
            result = get_phone_by_keyword(keyword)
            if result[0] == 1:
                query_phone = result[1]
            else:
                return {"code":"11016","status":"failed","msg":message["11016"]}

        logger.info(query_phone)

        code_page = (page - 1) * 10
        code_size = page * size

        sql = '''select count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price,
        count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,
        sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee 
        from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) '''
        all_data = pd.read_sql(sql,conn_read)
        logger.info(all_data)
        logger.info("-----------------")
        all_data = all_data.to_dict("records")
        logger.info(all_data)

        order_sql = '''select phone,count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by phone'''
        order_data = pd.read_sql(order_sql,conn_read)
        logger.info(order_data.shape)

        sell_sql = '''select sell_phone phone,count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by sell_phone'''
        sell_order = pd.read_sql(sell_sql,conn_read)
        logger.info(sell_order.shape)

        public_sql = '''select sell_phone phone,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1 group by sell_phone '''
        public_order = pd.read_sql(public_sql,conn_read)
        logger.info(public_order.shape)

        df_list = []
        df_list.append(order_data)
        df_list.append(sell_order)
        df_list.append(public_order)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)


        if page and size:
            need_data = df_merged.loc[code_page:code_size]
        else:
            need_data = df_merged.copy()
        logger.info(need_data)

        result = user_belong_bus(need_data)

        if result[0] == 1:
            last_data = result[1]
        else:
            return {"code":"10006","status":"failed","msg":message["10006"]}
        return {"code":"0000","status":"success","msg":last_data,"count":len(df_merged)}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


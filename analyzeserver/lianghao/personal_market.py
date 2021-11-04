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

        page = request.json["page"]
        size = request.json["size"]

        if not page or not size:
            return {"code":10001,"status":"failed","msg":message["10001"]}

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


        need_data = df_merged.loc[code_page:code_size]
        logger.info(need_data)

        # phone_list = need_data.to_dict('list')['phone']

        # #然后查crm
        # conn_crm = direct_get_conn(crm_mysql_conf)
        #
        # crm_user_sql = '''select sex,id unionid,pid parentid,phone,nickname from luke_sincerechat.user where phone is not null or phone != ""'''
        # crm_user_data = pd.read_sql(crm_user_sql,conn_crm)
        # user_data = need_data.merge(crm_user_data,how="left",on="phone")
        #
        #
        # phone_list = user_data.to_dict('list')['phone']
        # logger.info(len(phone_list))
        #
        # #查运营中心
        # all_operate = []
        # for pl in phone_list:
        #     logger.info("phone:%s" %pl)
        #     pl_op_dict = {}
        #     operate_sql = '''
        #     select a.*,b.operatename,b.crm from
        #     (WITH RECURSIVE temp as (
        #             SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t WHERE phone = %s
        #             UNION ALL
        #             SELECT t.id,t.pid,t.phone,t.nickname,t.`name`,t.sex,t.`status` FROM luke_sincerechat.user t INNER JOIN temp ON t.id = temp.pid
        #     )
        #     SELECT * FROM temp
        #     )a left join luke_lukebus.operationcenter b
        #     on a.id = b.unionid
        #     ''' %pl
        #     operate_data = pd.read_sql(operate_sql,conn_crm)
        #     logger.info(operate_data)
        #     logger.info("----------------")
        #
        #
        #     if len(operate_data)>0:
        #         # pandas可以保留排序 取出运营中心不为空的 并且 crm支持等于1的 第一个
        #         current_operate_data = operate_data[(operate_data["operatename"] != "") & (operate_data["crm"] == 1)].iloc[0, :]
        #         pl_op_dict["operate_name"] = current_operate_data["operatename"]
        #         pl_op_dict["phone"] = pl
        #         all_operate.append(pl_op_dict)
        #     else:
        #         pl_op_dict["operate_name"] = ""
        #         pl_op_dict["phone"] = pl
        #         all_operate.append(pl_op_dict)
        # logger.info(all_operate)
        #
        # all_operate = pd.DataFrame(all_operate)
        # logger.info(all_operate)
        #
        # user_data = user_data.merge(all_operate,how="left",on="phone")
        # logger.info(user_data.loc[0])
        # # user_data.to_csv("e:/huhu.csv")
        # last_data = user_data.to_dict("records")
        # logger.info(last_data)
        # conn_crm.close()

        result = user_belong_bus(need_data)
        # logger.info(result)
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


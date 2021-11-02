# -*- coding: utf-8 -*-
# @Time : 2021/11/1  16:46
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

opbp = Blueprint('operations', __name__, url_prefix='/lh/operations')


@opbp.route('/center', methods=['POST'])
def operations_order_count():
    try:
        return 1
        num = int(request.json.get('num'))
        # 运营中心sql
        operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1'
        # 运营中心关系sql
        search_sql = '''
        select a.*,b.operatename,b.crm from 
        (WITH RECURSIVE temp as (
            SELECT t.id,t.pid,t.phone,t.nickname,t.name FROM luke_sincerechat.user t WHERE phone = %s
            UNION ALL
            SELECT t1.id,t1.pid,t1.phone, t1.nickname,t1.name FROM luke_sincerechat.user t1 INNER JOIN temp ON t1.pid = temp.id
        )
        SELECT * FROM temp
        )a left join luke_lukebus.operationcenter b
        on a.id = b.unionid
        '''
        # 靓号数据统计
        lh_count_sql = '''
        select t1.*, t2.publish_total_price, t2.publish_total_count, t2.publish_sell_count from
        ((select phone, count(*) buy_order, sum(`count`) buy_count, sum(total_price) buy_price, count(*) sell_order, count(`count`) sell_count, sum(total_price) sell_price, sum(total_price- sell_fee) true_price, sum(sell_fee) sell_fee from lh_order
        where del_flag = 0
        and type in (1,4)
        and `status` = 1
        group by phone) t1
        left join
        (select sell_phone, sum(`count`) publish_total_count, sum(total_price) publish_total_price, count(*) publish_sell_count
        from lh_sell
        where del_flag=0
        and `status` != 1
        group by sell_phone) t2
        on t1.phone=t2.sell_phone)
        '''
        # 靓号数据
        conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        user_order_df = pd.read_sql(lh_count_sql, conn_lh)

        # crm数据
        conn_crm = direct_get_conn(crm_mysql_conf)
        crm_cursor = conn_crm.cursor()

        # 运营中心数据
        crm_cursor.execute(operate_sql)
        operate_data = crm_cursor.fetchall()
        operate_df = pd.DataFrame(operate_data)

        # 运营中心手机号列表
        operate_telephone_list = operate_df['telephone'].to_list()

        fina_data_list = []
        for phone in operate_telephone_list[:2]:
            conn_crm = direct_get_conn(crm_mysql_conf)
            crm_cursor = conn_crm.cursor()
            crm_cursor.execute(search_sql, phone)
            all_data = crm_cursor.fetchall()
            # 总数据
            all_data = pd.DataFrame(all_data)
            all_data.dropna(subset=['phone'], axis=0, inplace=True)
            all_data_phone = all_data['phone'].tolist()
            # 运营中心名称
            operate_data = operate_df.loc[operate_df['telephone'] == phone, :]
            operatename = operate_data['operatename'].values[0]
            operate_leader_unionid = operate_data['unionid'].values[0]
            operate_leader_name = operate_data['name'].values[0]
            # 子运营中心
            center_phone_list = all_data.loc[all_data['operatename'].notna(), :]['phone'].tolist()
            child_center_phone_list = []
            # 第一级别
            first_child_center = []
            for i in center_phone_list[1:]:
                # 剔除下级的下级运营中心
                if i in child_center_phone_list:
                    continue
                first_child_center.append(i)
                crm_cursor.execute(search_sql, i)
                center_data = crm_cursor.fetchall()
                center_df = pd.DataFrame(center_data)
                center_df.dropna(subset=['phone'], axis=0, inplace=True)
                child_center_phone_list.extend(center_df['phone'].tolist())
            ret = list(set(all_data_phone) - set(child_center_phone_list))
            ret.extend(first_child_center)
            # 靓号数据
            child_df = user_order_df.loc[user_order_df['phone'].isin(ret), :]
            notice_data = {
                'operatename': operatename,  # 运营中心名
                'operate_leader_name': operate_leader_name,  # 运营中心负责人
                'operate_leader_phone': phone,  # 手机号
                'operate_leader_unionid': operate_leader_unionid,  # unionID
                'buy_order': child_df['buy_order'].sum(),  # 采购订单数量
                'buy_count': child_df['buy_count'].sum(),  # 采购靓号数量
                'buy_price': child_df['buy_price'].sum(),  # 采购金额
                'publish_total_count': child_df['publish_total_count'].sum(),  # 发布靓号
                'publish_sell_count': child_df['publish_sell_count'].sum(),  # 发布订单
                'publish_total_price': child_df['publish_total_price'].sum(),  # 发布金额
                'sell_order': child_df['sell_order'].sum(),  # 出售订单数
                'sell_price': child_df['sell_price'].sum(),  # 出售金额
                'sell_count': child_df['sell_count'].sum(),  # 出售靓号数
                'true_price': child_df['true_price'].sum(),  # 出售时实收金额
                'sell_fee': child_df['sell_fee'].sum(),  # 出售手续费
            }
            fina_data_list.append(notice_data)
            logger.info(notice_data)
        conn_crm.close()
        # return_data = json.dumps(fina_data_list[:num])
        # logger.info(return_data)
        logger.info('-' * 50)
        logger.info(fina_data_list[:num])
        return {"code": "0000", "status": "success", "data": fina_data_list[:num]}
    except:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}

# -*- coding: utf-8 -*-
# @Time : 2021/11/1  16:46
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
from functools import reduce

opbp = Blueprint('operations', __name__, url_prefix='/lh/operations')

@opbp.route('/center', methods=['POST'])
def operations_order_count():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 7:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            search_key = request.json['key']
            operateid = request.json['operateid']
            start_time = request.json['start_time']
            end_time = request.json['end_time']
            size = request.json['size']
            page = request.json['page']
        except:
            # 参数名错误
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        run_start = time.time()

        lh_count_sql_buy = '''
            select phone, count(*) buy_order, sum(`count`) buy_count, sum(total_price) buy_price
            from lh_pretty_client.lh_order
            where del_flag = 0
            and type in (1,4)
            and `status` = 1{}
            group by phone
            '''
        lh_count_sql_sell = '''
            select sell_phone phone, count(*) sell_order, sum(`count`) sell_count, sum(total_price) sell_price, sum(total_price- sell_fee) true_price, sum(sell_fee) sell_fee
            from lh_pretty_client.lh_order
            where del_flag = 0
            and type in (1,4)
            and `status` = 1{}
            group by sell_phone
        '''
        lh_count_sql_publish = '''
            select sell_phone phone, sum(`count`) publish_total_count, sum(total_price) publish_total_price, count(*) publish_sell_count
            from lh_pretty_client.lh_sell
            where del_flag=0
            and `status` != 1{}
            group by sell_phone
        '''
        if start_time and end_time:
            time_sql = ''' and date_format(create_time, "%Y-%m-%d") >= "{}" and date_format(create_time, "%Y-%m-%d") <= "{}"'''.format(start_time, end_time)
        else:
            time_sql = ''
        lh_count_sql_buy = lh_count_sql_buy.format(time_sql)
        lh_count_sql_sell = lh_count_sql_sell.format(time_sql)
        lh_count_sql_publish = lh_count_sql_publish.format(time_sql)


        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_an =direct_get_conn(analyze_mysql_conf)
        if not conn_lh or not conn_an:
            return {"code": "10008", "status": "failed", "msg": message["10008"]}

        user_order_df_list = []
        # 用户订单DataFrame
        user_order_buy_df = pd.read_sql(lh_count_sql_buy, conn_lh)
        user_order_df_list.append(user_order_buy_df)

        user_order_sell_df = pd.read_sql(lh_count_sql_sell, conn_lh)
        user_order_df_list.append(user_order_sell_df)

        user_order_publish_df = pd.read_sql(lh_count_sql_publish, conn_lh)
        user_order_df_list.append(user_order_publish_df)
        # 用户订单数据
        user_order_df = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), user_order_df_list)

        # 用户数据查询
        user_info_sql = 'select unionid, phone, operate_id operateid, operatename, leader operate_leader_name, bus_phone operate_leader_phone from lh_analyze.crm_user where operatename is not null'
        user_info_df = pd.read_sql(user_info_sql, conn_an)
        # 合并得到运营中心负责人unionid
        operate_unionid_df = user_info_df.loc[:, ['unionid', 'phone']].rename(columns={"unionid": "operate_leader_unionid", "phone": "operate_leader_phone"})
        user_info_df = user_info_df.merge(operate_unionid_df, how='left', on='operate_leader_phone')

        # # 运营中心信息
        operate_info_df = user_info_df.loc[:, ['operate_leader_name', 'operatename', 'operateid', 'operate_leader_phone', 'operate_leader_unionid']].drop_duplicates('operate_leader_phone')

        # 获取运营中心数据
        # result = get_operationcenter_data(user_order_df, search_key, operateid)
        # if not result[0]: # 不成功
        #     return {"code": result[1], "status": "failed", "msg": message[result[1]]}
        fina_df = user_order_df.merge(user_info_df, how='left', on='phone')
        operate_data_df = fina_df.groupby('operateid')['buy_order', 'buy_count', 'buy_price', 'publish_total_count', 'publish_sell_count', 'publish_total_price', 'sell_order', 'sell_price', 'sell_count', 'true_price', 'sell_fee'].sum().reset_index()
        operate_data_df = operate_data_df.merge(operate_info_df, how='outer', on='operateid')
        logger.info(operate_data_df.shape)
        # 查询运营中心已关闭的id,为防止crm库出现问题，加上异常捕获 [15, 58, 80]
        if not check_close_operate:
            try:
                conn_crm = direct_get_conn(crm_mysql_conf)
                close_id_sql = '''select id from luke_lukebus.operationcenter where crm=1 and capacity=1 and status=2'''
                close_id_df = pd.read_sql(close_id_sql, conn_crm)
                close_id_list = close_id_df['id'].tolist()
                conn_crm.close()
                operate_data_df = operate_data_df[~operate_data_df['operateid'].isin(close_id_list)]
            except:
                logger.info(traceback.format_exc())

        if search_key:
            operate_data_df['operate_leader_unionid'] = operate_data_df['operate_leader_unionid'].astype(str)
            operate_data_df = operate_data_df.loc[(operate_data_df['operate_leader_name'].str.contains(search_key))
                                                    | (operate_data_df['operate_leader_phone'].str.contains(search_key))
                                                    | (operate_data_df['operate_leader_unionid'].str.contains(search_key))]
        if operateid:
            operate_data_df = operate_data_df.loc[operate_data_df['operateid'] == operateid, :]
        title_data = {
            'buy_order': operate_data_df['buy_order'].sum(),  # 采购订单数量
            'buy_count': operate_data_df['buy_count'].sum(),  # 采购靓号数量
            'buy_price': round(float(operate_data_df['buy_price'].sum()), 2),  # 采购金额
            'publish_total_count': operate_data_df['publish_total_count'].sum(),  # 发布靓号
            'publish_sell_count': operate_data_df['publish_sell_count'].sum(),  # 发布订单
            'publish_total_price': round(float(operate_data_df['publish_total_price'].sum()), 2),  # 发布金额
            'sell_order': operate_data_df['sell_order'].sum(),  # 出售订单数
            'sell_price': round(float(operate_data_df['sell_price'].sum()), 2),  # 出售金额
            'sell_count': operate_data_df['sell_count'].sum(),  # 出售靓号数
            'true_price': round(float(operate_data_df['true_price'].sum()), 2),  # 出售时实收金额
            'sell_fee': round(float(operate_data_df['sell_fee'].sum()), 2),  # 出售手续费
        }
        # 填充，再根据采购金额降序排列
        operate_data_df.fillna(0, inplace=True)
        operate_data_df.sort_values("buy_price", ascending=False, inplace=True)
        if size and page:
            start_index = (page - 1) * size
            end_index = size * page
            data = operate_data_df[start_index:end_index]
            logger.info(data.shape)
        else:
            data = operate_data_df
        # 数据圆整
        data['buy_price'] = round(data['buy_price'].astype(float), 2)
        data['publish_total_price'] = round(data['publish_total_price'].astype(float), 2)
        data['sell_price'] = round(data['sell_price'].astype(float), 2)
        data['true_price'] = round(data['true_price'].astype(float), 2)
        data['sell_fee'] = round(data['sell_fee'].astype(float), 2)



        return_data = {
            'title_data': title_data,
            'data': data.to_dict('records')
        }
        run_end = time.time()
        logger.info(run_end - run_start)
        return {"code": "0000", "status": "success", "msg": return_data,'count': len(operate_data_df)}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message['10000']}
    finally:
        try:
            conn_lh.close()
            conn_an.close()
        except:
            pass



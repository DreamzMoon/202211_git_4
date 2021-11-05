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
@pmbp.route('/publish', methods=["POST"])
def personal_publish():
    pass


@pmbp.route('/orderflow', methods=["POST"])
def personal_order_flow():
    try:
        logger.info(request.json)
        # 参数个数错误
        if len(request.json) !=10:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}
        # 购买人信息
        buyer_info = request.json['buyer_info'].strip()
        # 表单选择operatename
        form_operatename = request.json['operatename']
        # 归属上级
        parentid = request.json['parentid'].strip()
        # 订单编码
        order_sn = request.json['order_sn'].strip()
        # 出售人信息
        sell_info = request.json['sell_info'].strip()
        # 交易时间
        order_time = request.json['order_time']
        # 支付类型
        pay_type = request.json['pay_type']
        # 转让类型
        transfer_type = request.json['transfer_type']

        # 每页显示条数
        num = request.json['num']
        # 页码
        page = request.json['page']
        # isdigit()可以判断是否为正整数
        if not num.isdigit() or int(num) < 1:
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        elif not page.isdigit() or int(page) < 1:
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        else:
            num = int(num)
            page = int(page)
    except:
        # 参数名错误
        return {"code": "10009", "status": "failed", "msg": message["10009"]}

    order_flow_sql = '''
        select order_sn, phone buyer_phone, pay_type, count, total_price buy_price, sell_phone, total_price sell_price, total_price - sell_fee true_price, sell_fee, order_time, sell_id
        from lh_pretty_client.lh_order
        where `status`  = 1
        and del_flag = 0
        and type in (1, 4)
        and sell_phone is not null
        '''
    conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
    if not conn_lh:
        return {"code": "10008", "status": "failed", "msg": message["10008"]}
    order_flow_data = pd.read_sql(order_flow_sql, conn_lh)

    order_flow_data = order_flow_data[order_flow_data['']]

    start_index = (page - 1) * num
    end_index = page * num
    if end_index > order_flow_data.shape[0]:
        end_index = len(order_flow_data.shape[0])
    logger.info('start_index: %s' % start_index)
    logger.info('end_index: %s' % end_index)
    part_user = order_flow_data.loc[start_index:end_index-1, :]
    order_buyer_list = part_user['buyer_phone'].tolist()
    logger.info(order_buyer_list)

    operate_sql = '''
        select a.*,b.operatename, b.crm from 
        (WITH RECURSIVE temp as (
                SELECT t.id,t.pid,t.phone,t.nickname FROM luke_sincerechat.user t WHERE phone = %s
                UNION ALL
                SELECT t.id,t.pid,t.phone,t.nickname FROM luke_sincerechat.user t INNER JOIN temp ON t.id = temp.pid
        )
        SELECT * FROM temp 
        )a left join luke_lukebus.operationcenter b
        on a.id = b.unionid
        '''

    conn_crm = direct_get_conn(crm_mysql_conf)
    crm_user_sql = '''select id unionid,pid parentid,phone,nickname from luke_sincerechat.user where phone is not null or phone != ""'''
    crm_user_data = pd.read_sql(crm_user_sql, conn_crm)

    crm_cursor = conn_crm.cursor()

    user_data_list = []
    for phone in set(order_buyer_list):
        logger.info('phone: %s' % phone)
        crm_cursor.execute(operate_sql, phone)
        operate_data = pd.DataFrame(crm_cursor.fetchall())
        operatename = operate_data.loc[(operate_data['operatename'].notna()) & (operate_data['crm'] == 1), 'operatename'].tolist()
        if operatename:
            operate_data.loc[0, 'operatename'] = operatename[0]
        user_data = operate_data.loc[:0, :]
        user_data_list.append(user_data)
    df_merged = pd.concat(user_data_list, ignore_index=True)
    df_merged.columns = ['buyer_unionid', 'parentid', 'buyer_phone', 'name', 'operatename', 'crm']
    logger.info('获取用户数据完成')
    # 买方信息
    part_user = part_user.merge(df_merged, how='left', on='buyer_phone')
    # 卖方信息
    sell_data = crm_user_data.loc[:, ['unionid', 'phone']]
    sell_data.columns = ['sell_unionid', 'sell_phone']
    part_user['order_time'] = part_user['order_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
    part_user = part_user.merge(sell_data, how='left', on='sell_phone')
    part_user['pay_type'] = part_user['pay_type'].astype(str)
    # 支付类型映射
    pay_type = {
        "-1": "未知",
        "0": "信用点支付",
        "1": "诚聊余额支付",
        "2": "诚聊通余额支付",
        "3": "微信支付",
        "4": "支付宝支付",
        "5": "后台系统支付",
        "6": "银行卡支付",
        "7": "诚聊通佣金支付",
        "8": "诚聊通红包支付"
    }
    part_user['pay_type'] = part_user['pay_type'].map(pay_type)
    logger.info(part_user)
    logger.info(part_user.shape)
    # 数据库默认为1
    part_user['transfer_type'] = 1
    for index, row in part_user.iterrows():
        transfer_type_sql = '''select price_status from lh_pretty_client.lh_sell where id="%s"''' % row['sell_id']
        transfer_type_data = pd.read_sql(transfer_type_sql, conn_lh)
        part_user.loc[index, 'transfer_type'] = transfer_type_data['price_status'].values[0]
    part_user.drop(['sell_id', 'crm'], axis=1, inplace=True)
    result = part_user.to_dict('records')
    logger.info(result)
    return_data = {
        'count': len(order_flow_data),
        'order_flow': result
    }
    conn_crm.close()
    return {"code": "0000", "status": "success", "msg": return_data}




@pmbp.route("total",methods=["POST"])
def personal_total():
    try:
        conn_read = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)

        logger.info(request.json)
        page = request.json["page"]
        size = request.json["size"]

        # 可以是用户名称 手机号 unionid 模糊的
        keyword = request.json["keyword"]

        # 查询归属上级 精准的
        parent = request.json["parent"]
        bus = request.json["bus"]

        # 字符串拼接的手机号码
        query_phone = ""
        keyword_phone = []
        parent_phone = []
        bus_phone = []

        # 模糊查询
        if keyword:
            result = get_phone_by_keyword(keyword)
            logger.info(result)
            if result[0] == 1:
                keyword_phone = result[1]
            else:
                return {"code":"11016","status":"failed","msg":message["11016"]}

        # 只查一个
        if parent:
            if len(parent) == 11:
                parent_phone.append(parent)
            else:
                result = get_phone_by_unionid(parent)
                if result[0] == 1:
                    parent_phone.append(result[1])
                else:
                    return {"code":"11014","status":"failed","msg":message["code"]}
        # 查禄可商务的
        if bus:
            result = get_lukebus_phone([bus])
            if result[0] == 1:
                bus_phone = result[1].split(",")
            else:
                return {"code":"11015","status":"failed","msg":message["11015"]}

        # 对手机号码差交集
        if keyword_phone and parent_phone and bus_phone:
            query_phone = list((set(keyword_phone).intersection(set(parent_phone))).intersection(set(bus_phone)))
        elif keyword_phone and parent_phone:
            query_phone = list(set(keyword_phone).intersection(set(parent_phone)))
        elif keyword_phone and bus_phone:
            query_phone = list(set(keyword_phone).intersection(set(bus_phone)))
        elif parent_phone and bus_phone:
            query_phone = list(set(parent_phone).intersection(set(bus_phone)))
        elif keyword_phone:
            query_phone = keyword_phone
        elif parent_phone:
            query_phone = parent_phone
        elif bus_phone:
            query_phone = bus_phone
        else:
            query_phone = ""

        code_page = (page - 1) * 10
        code_size = page * size


        # 如果没有手机号码可以一起查 如果有手机号码要分成三个sql
        # sql = '''select count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price,
        #         count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,
        #         sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee
        #         from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) '''
        # logger.info(sql)
        # last_all_data = pd.read_sql(sql,conn_read).to_dict("records")


        order_sql = '''select phone,count(*) buy_count,sum(count) buy_total_count,sum(total_price) buy_total_price from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
        group_sql = ''' group by phone'''
        if query_phone:
            condition_sql = ''' and phone in (%s)''' % (",".join(query_phone))
            order_sql = order_sql+condition_sql+group_sql
        else:
            order_sql = order_sql +group_sql
        logger.info("order_sql:%s" %order_sql)
        order_data = pd.read_sql(order_sql,conn_read)
        logger.info(order_data.shape)

        sell_sql = '''select sell_phone phone,count(*) sell_count,sum(count) sell_total_count,sum(total_price) sell_total_price,sum(total_price-sell_fee) sell_real_money,sum(sell_fee) sell_fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4)'''
        group_sql = ''' group by sell_phone'''
        if query_phone:
            condition_sql = ''' and sell_phone in (%s)''' % (",".join(query_phone))
            sell_sql = sell_sql + condition_sql + group_sql
        else:
            sell_sql = sell_sql + group_sql
        logger.info("sell_sql:%s" %sell_sql)
        sell_order = pd.read_sql(sell_sql,conn_read)
        logger.info(sell_order.shape)

        public_sql = '''select sell_phone phone,sum(total_price) publish_total_price,sum(count) publish_total_count,count(*) publish_sell_count from lh_sell where del_flag = 0 and status != 1'''
        group_sql = ''' group by sell_phone '''
        if query_phone:
            condition_sql = ''' and sell_phone in (%s)''' % (",".join(query_phone))
            public_sql = public_sql + condition_sql + group_sql
        else:
            public_sql = public_sql + group_sql
        logger.info("public_sql:%s" %public_sql)
        public_order = pd.read_sql(public_sql,conn_read)
        logger.info(public_order.shape)



        df_list = []
        df_list.append(order_data)
        df_list.append(sell_order)
        df_list.append(public_order)
        df_merged = reduce(lambda left, right: pd.merge(left, right, on=['phone'], how='outer'), df_list)

        all_data = {"buy_count": 0, "buy_total_count": 0, "buy_total_price": 0, "sell_count": 0, "sell_fee": 0,
                    "sell_real_money": 0, "sell_total_count": 0, "sell_total_price": 0}


        #把nan都填充0
        df_merged["buy_count"].fillna(0,inplace=True)
        df_merged["buy_total_count"].fillna(0,inplace=True)
        df_merged["buy_total_price"].fillna(0,inplace=True)
        df_merged["sell_count"].fillna(0,inplace=True)
        df_merged["sell_fee"].fillna(0,inplace=True)
        df_merged["sell_real_money"].fillna(0,inplace=True)
        df_merged["sell_total_count"].fillna(0,inplace=True)
        df_merged["sell_total_count"].fillna(0,inplace=True)

        all_df = df_merged.to_dict("records")

        for i in range(0,len(all_df)):

            all_data["buy_count"] = all_data["buy_count"] + all_df[i]["buy_count"]
            all_data["buy_total_count"] = all_data["buy_total_count"] + all_df[i]["buy_total_count"]
            all_data["buy_total_price"] = all_data["buy_total_price"] + all_df[i]["buy_total_price"]
            all_data["sell_count"] = all_data["sell_count"] + all_df[i]["sell_count"]
            all_data["sell_fee"] = all_data["sell_fee"] + all_df[i]["sell_fee"]
            all_data["sell_real_money"] = all_data["sell_real_money"] + all_df[i]["sell_real_money"]
            all_data["sell_total_count"] = all_data["sell_total_count"] + all_df[i]["sell_total_count"]
            all_data["sell_total_price"] = all_data["sell_total_price"] + all_df[i]["sell_total_price"]

        all_data["buy_total_price"] = round(all_data["buy_total_price"],2)
        all_data["sell_fee"] = round(all_data["sell_fee"],2)
        all_data["sell_real_money"] = round(all_data["sell_real_money"],2)
        all_data["sell_total_price"] = round(all_data["sell_total_price"],2)


        logger.info("code_page:%s" %code_page)
        logger.info("code_size:%s" %code_size)
        if page and size:
            need_data = df_merged[code_page:code_size]
        else:
            need_data = df_merged.copy()
        logger.info(need_data)

        result = user_belong_bus(need_data)

        if result[0] == 1:
            last_data = result[1]
            logger.info(last_data)
        else:
            return {"code":"10006","status":"failed","msg":message["10006"]}
        msg_data = {"data":last_data,"all_data":all_data}
        logger.info("msg_data:%s" %msg_data)
        return {"code":"0000","status":"success","msg":msg_data,"count":len(df_merged)}
    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_read.close()


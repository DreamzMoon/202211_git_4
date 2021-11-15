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


opbp = Blueprint('operations', __name__, url_prefix='/lh/operations')


# 不用
def operations_order_count_drop():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 4:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}
            search_key = request.json['key']
            form_operatename = request.json['operatename']
            num = request.json['num']
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

        # crm用户数据（手机号不为空）
        crm_user_sql = 'select `phone` from luke_sincerechat.user where phone is not null'
        # 运营中心关系sql
        supervisor_sql = '''
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
        # 数据库连接
        conn_crm = direct_get_conn(crm_mysql_conf)
        # conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_crm or not conn_lh:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        # crm_cursor
        crm_cursor = conn_crm.cursor()

        # 运营中心数据
        if search_key == "" and form_operatename == "":
            # 运营中心sql
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1'
        # 11位手机号
        elif search_key.isdigit() and len(search_key) == 11 and not form_operatename:
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and telephone=%s'
        elif search_key.isdigit() and len(search_key) == 11 and form_operatename:
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and telephone=%s and operatename=%s'
        # unionid
        elif search_key.isdigit() and len(search_key) < 11 and not form_operatename:
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and unionid=%s'
        elif search_key.isdigit() and len(search_key) < 11 and form_operatename:
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and unionid=%s and operatename=%s'
        # 名称
        elif search_key and not search_key.isdigit() and not form_operatename:
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and name=%s'
        elif search_key and not search_key.isdigit() and form_operatename:
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and name=%s and operatename=%s'
        elif not search_key and form_operatename:
            operate_sql = 'select unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and operatename=%s'
        else: # 手机号输入过长或者unionid输入错误
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        logger.info(operate_sql)
        if not search_key and not form_operatename: # 都为空
            crm_cursor.execute(operate_sql)
        elif search_key and not form_operatename: # 搜索不为空，运营中心为空
            crm_cursor.execute(operate_sql, search_key)
        elif search_key and form_operatename: # 都不为空
            crm_cursor.execute(operate_sql, (search_key, form_operatename))
        else: # 搜索为空，运营中心不为空
            crm_cursor.execute(operate_sql, form_operatename)
        operate_data = crm_cursor.fetchall()
        if not operate_data:
            return {"code": "10010", "status": "failed", "msg": message["10010"]}
        operate_df = pd.DataFrame(operate_data)

        # 用户数据
        crm_cursor.execute(crm_user_sql)
        crm_user_data = pd.DataFrame(crm_cursor.fetchall())  # 用户id为642184手机号为空值
        crm_user_data['type'] = 1

        # 靓号数据
        user_order_df = pd.read_sql(lh_count_sql, conn_lh)

        # 运营中心手机号列表
        operate_telephone_list = operate_df['telephone'].to_list()

        # 横标总数据
        title_df = user_order_df.merge(crm_user_data, how='left', on='phone')
        # 剔除不在crm的用户订单
        title_df = title_df.loc[title_df['type'] == 1, :]
        title_data = {
                'buy_order': int(title_df['buy_order'].sum()),  # 采购订单数量
                'buy_count': int(title_df['buy_count'].sum()),  # 采购靓号数量
                'buy_price': str(round(title_df['buy_price'].sum(), 2)),  # 采购金额
                'publish_total_count': int(title_df['publish_total_count'].sum()),  # 发布靓号
                'publish_sell_count': int(title_df['publish_sell_count'].sum()),  # 发布订单
                'publish_total_price': str(round(title_df['publish_total_price'].sum(), 2)),  # 发布金额
                'sell_order': int(title_df['sell_order'].sum()),  # 出售订单数
                'sell_price': str(round(title_df['sell_price'].sum(), 2)),  # 出售金额
                'sell_count': int(title_df['sell_count'].sum()),  # 出售靓号数
                'true_price': str(round(title_df['true_price'].sum(), 2)),  # 出售时实收金额
                'sell_fee': str(round(title_df['sell_fee'].sum(), 2)),  # 出售手续费
            }
        fina_center_data_list = []
        for phone in operate_telephone_list:
            crm_cursor.execute(supervisor_sql, phone)
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
                crm_cursor.execute(supervisor_sql, i)
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
                'operate_leader_unionid': str(int(operate_leader_unionid)),  # unionID
                'buy_order': int(child_df['buy_order'].sum()),  # 采购订单数量
                'buy_count': int(child_df['buy_count'].sum()),  # 采购靓号数量
                'buy_price': str(round(child_df['buy_price'].sum(), 2)),  # 采购金额
                'publish_total_count': int(child_df['publish_total_count'].sum()),  # 发布靓号
                'publish_sell_count': int(child_df['publish_sell_count'].sum()),  # 发布订单
                'publish_total_price': str(round(child_df['publish_total_price'].sum(), 2)),  # 发布金额
                'sell_order': int(child_df['sell_order'].sum()),  # 出售订单数
                'sell_price': str(round(child_df['sell_price'].sum(), 2)),  # 出售金额
                'sell_count': int(child_df['sell_count'].sum()),  # 出售靓号数
                'true_price': str(round(child_df['true_price'].sum(), 2)),  # 出售时实收金额
                'sell_fee': str(round(child_df['sell_fee'].sum(), 2)),  # 出售手续费
            }
            fina_center_data_list.append(notice_data)
            logger.info(notice_data)
        start_num = (page-1) * num
        end_num = page * num
        # 如果num超过数据条数
        if end_num > len(fina_center_data_list):
            end_num = len(fina_center_data_list)
        return_data = {
            'count': len(fina_center_data_list),
            'title_data': title_data,
            'search_data': fina_center_data_list[start_num:end_num]
            }
        logger.info(return_data)
        logger.info('-' * 50)
        # logger.info(result[1][start_num:end_num])
        return {"code": "0000", "status": "success", "data": return_data}
        # result = get_operationcenter_data(crm_cursor, operate_df, user_order_df)
        # if result[0]:
        #     conn_crm.close()
        #     start_num = (page-1) * num
        #     end_num = page * num
        #     # 如果num超过数据条数
        #     if end_num > len(result[1]):
        #         end_num = len(result[1])
        #     return_data = {
        #         'count': len(result[1]),
        #         'title_data': title_data,
        #         'search_data': result[1][start_num:end_num]
        #     }
        #     logger.info(return_data)
        #     logger.info('-' * 50)
        #     logger.info(result[1][start_num:end_num])
        #     return {"code": "0000", "status": "success", "data": return_data}
        # else:
        #     logger.info(result[1])
        #     return {"code": "10000", "status": "failed", "msg": message["10000"]}
    except Exception as e:
        logger.error(e)
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_crm.close()
            conn_lh.close()
        except:
            pass



@opbp.route('/center', methods=['POST'])
def operations_order_count():
    try:
        logger.info(request.json)
        # 参数个数错误
        if len(request.json) != 5:
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
        size = str(request.json['size'])
        page = str(request.json['page'])
        num = size
        if num and page:
            # isdigit()可以判断是否为正整数
            if not num.isdigit() or int(num) < 1:
                return {"code": "10009", "status": "failed", "msg": message["10009"]}
            elif not page.isdigit() or int(page) < 1:
                return {"code": "10009", "status": "failed", "msg": message["10009"]}
            else:
                num = int(num)
                page = int(page)
        else:
            pass
    except:
        # 参数名错误
        return {"code": "10009", "status": "failed", "msg": message["10009"]}

    start_time = time.time()
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

    conn_lh = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_mysql_conf)
    if not conn_lh:
        return {"code": "10008", "status": "failed", "msg": message["10008"]}

    # 用户订单DataFrame
    user_order_df = pd.read_sql(lh_count_sql, conn_lh)

    # 获取运营中心数据
    result = get_operationcenter_data(user_order_df, search_key, operateid)
    if not result[0]: # 不成功
        return {"code": result[1], "status": "failed", "msg": message[result[1]]}
    if num and page:
        start_num = (page - 1) * num
        end_num = page * num
        # 如果num超过数据条数
        if end_num > len(result[1]):
            end_num = len(result[1])
    else:
        start_num = 0
        end_num = len(result[1])
    return_data = {
        'count': len(result[1]),
        'title_data': result[2],
        'data': result[1][start_num:end_num]
    }
    end_time = time.time()
    logger.info(end_time - start_time)
    return {"code": "0000", "status": "success", "msg": return_data}



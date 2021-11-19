# -*- coding: utf-8 -*-
'''
禄可商务运营中心
'''
import sys, os, json
import traceback
from datetime import date
from functools import reduce

import pandas as pd

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from config import *
from util.help_fun import *

def operationcenter_info():
    try:
        logger.info('获取禄可商务运营中心数据')
        crm_mysql_conf["db"] = "luke_lukebus"
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = '''
                select
                    unionid, punionid parentid, capacity, `name`, telephone phone, operatename, is_factory, status, crm, from_unixtime(addtime, '%Y-%m-%d %H:%i:%s') addtime
                from
                    operationcenter
            '''
            cursor.execute(sql)
            center_data = cursor.fetchall()
        conn_crm.close()

        center_df = pd.DataFrame(center_data)

        # 插入数据
        logger.info('写入运营中心数据')
        conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, analyze_mysql_conf)
        logger.info(conn_rw)
        center_df.to_sql("bus_operationcenter", con=conn_rw, if_exists="append", index=False)
        logger.info('写入运营中心数据完成')
    except:
        logger.error('写入运营中心数据失败')
        logger.error(traceback.format_exc())


# operationcenter_info()


####################################################
# 运营中心关系首次更新数据
def operate_relationship(mode='first'):
    try:
        supervisor_sql = '''
            select a.*,b.operatename from
            (WITH RECURSIVE temp as (
                SELECT t.id,t.pid,t.phone,t.nickname,t.name FROM luke_sincerechat.user t WHERE phone = %s
                UNION ALL
                SELECT t1.id,t1.pid,t1.phone, t1.nickname,t1.name FROM luke_sincerechat.user t1 INNER JOIN temp ON t1.pid = temp.id
            )
            SELECT * FROM temp
            )a left join luke_lukebus.operationcenter b
            on a.id = b.unionid
            '''

        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return False, '10002'  # 数据库连接失败
        crm_cursor = conn_crm.cursor()

        operate_sql = 'select id, unionid, punionid parentid, name, telephone phone, operatename, crm, status from luke_lukebus.operationcenter where capacity=1'
        crm_cursor.execute(operate_sql)
        operate_data = crm_cursor.fetchall()
        operate_df = pd.DataFrame(operate_data)

        # 运营中心手机号列表
        operate_telephone_list = operate_df['phone'].to_list()

        fina_center_data_list = []
        for phone in operate_telephone_list:
            print(phone)
            crm_cursor.execute(supervisor_sql, phone)
            all_data = crm_cursor.fetchall()
            # 总数据
            all_data = pd.DataFrame(all_data)
            all_data.dropna(subset=['phone'], axis=0, inplace=True)
            all_data_phone = all_data['phone'].tolist()
            # 运营中心名称
            # operate_data = operate_df.loc[operate_df['telephone'] == phone, :]
            # 子运营中心
            center_phone_list = all_data.loc[all_data['operatename'].notna(), :]['phone'].tolist()
            child_center_phone_list = []
            # 第一级别运营中心
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
            not_contains = list(set(all_data_phone) - set(child_center_phone_list))
            contains = list(set(all_data_phone) - set(child_center_phone_list))
            contains.extend(first_child_center)
            # 包含运营中心id
            child_center_id = all_data.loc[all_data['operatename'].notna(), :]['id'].to_list()[1:]
            ret = {'phone': phone, 'not_contains': not_contains, "contains": contains, "child_center_id": child_center_id}
            fina_center_data_list.append(ret)
        fina_df = pd.DataFrame(fina_center_data_list)
        result_df = operate_df.merge(fina_df, how='left', on='phone')
        conn_crm.close()
        if mode == 'first':
            conn_rw = pd_conn(analyze_mysql_conf)
            logger.info(conn_rw)
            result_df['contains'] = result_df['contains'].apply(lambda x: json.dumps(x))
            result_df['not_contains'] = result_df['not_contains'].apply(lambda x: json.dumps(x))
            result_df['child_center_id'] = result_df['child_center_id'].apply(lambda x: json.dumps(x))
            result_df.to_sql("operate_relationship", con=conn_rw, if_exists="append", index=False)
            return True, '刷新成功'
        else:
            return True, result_df
    except:
        logger.error(traceback.format_exc())
        return False, '失败'

# result = operate_relationship()
# logger.info(result[1])


# 更新数据
def refresh_operate_relationship():
    new_result = operate_relationship(mode='refresh')
    if not new_result:
        return new_result[1]
    new_operate_df = new_result[1]
    old_operate_sql = '''
        select * from lh_analyze.operate_relationship_copy1
    '''
    conn_rw = pd_conn(analyze_mysql_conf)

    old_operate_df = pd.read_sql(old_operate_sql, conn_rw)
    for index, row in new_operate_df.iterrows():
        old_row_data = old_operate_df[old_operate_df['id'] == row['id']]
        if old_row_data.shape[0] == 0:
            new_data = pd.DataFrame(row).T
            new_data['contains'] = new_data['contains'].apply(lambda x: json.dumps(x))
            new_data['not_contains'] = new_data['not_contains'].apply(lambda x: json.dumps(x))
            new_data['child_center_id'] = new_data['child_center_id'].apply(lambda x: json.dumps(x))
            # 插入数据
            new_data.to_sql("operate_relationship", con=conn_rw, if_exists="append", index=False)

# refresh_operate_relationship()
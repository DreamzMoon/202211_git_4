# -*- coding: utf-8 -*-
# @Time : 2021/11/30  14:57
# @Author : shihong
# @File : .py
# --------------------------------------
import datetime
import sys, os, json
import traceback
from datetime import date
from functools import reduce

import pandas as pd

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from config import *
from util.help_fun import *
from datetime import timedelta


def operate_relationship_crm(mode='first'):
    try:
        # supervisor_sql = '''
        #     select a.*, if (crm =0, Null, b.operatename) operatename, b.id operateid from
        #     (WITH RECURSIVE temp as (
        #         SELECT t.id,t.pid,t.phone,t.nickname,t.name FROM luke_sincerechat.user t WHERE phone = %s
        #         UNION ALL
        #         SELECT t1.id,t1.pid,t1.phone, t1.nickname,t1.name FROM luke_sincerechat.user t1 INNER JOIN temp ON t1.pid = temp.id
        #     )
        #     SELECT * FROM temp
        #     )a left join luke_lukebus.operationcenter b
        #     on a.id = b.unionid
        #     '''

        supervisor_sql = '''
            select unionid id,parentid pid,phone,nickname,name,operatename,operate_id operateid from crm_user_{} where bus_phone = %s
                    '''.format(current_time)

        conn_analyze = direct_get_conn(analyze_mysql_conf)
        analyze_cusor = conn_analyze.cursor()
        # conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_analyze:
            return False, '10002'  # 数据库连接失败
        # crm_cursor = conn_crm.cursor()

        # operate_sql = 'select id, unionid, punionid parentid, name, telephone phone, operatename, crm, status from luke_lukebus.operationcenter where capacity=1 and crm=1'
        operate_sql = 'select id, unionid, punionid parentid, name, telephone phone, operatename, crm, status from lh_analyze.operationcenter where capacity=1 and crm=1'
        # analyze_cusor.execute(operate_sql)
        # operate_data = analyze_cusor.fetchall()
        operate_df = pd.read_sql(operate_sql, conn_analyze)

        # 运营中心手机号列表
        operate_telephone_list = operate_df['phone'].to_list()

        fina_center_data_list = []
        for phone in operate_telephone_list:
            logger.info(phone)
            analyze_cusor.execute(supervisor_sql, phone)
            all_data = analyze_cusor.fetchall()
            # 总数据
            all_data = pd.DataFrame(all_data)
            coll_lists = ["id", "pid", "phone", "nickname", "name", "operatename", "operateid"]
            all_data.columns = coll_lists

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
                # crm_cursor.execute(supervisor_sql, i)
                # center_data = crm_cursor.fetchall()
                analyze_cusor.execute(supervisor_sql, phone)
                center_data = analyze_cusor.fetchall()
                # 总数据
                center_df = pd.DataFrame(center_data)
                coll_lists = ["id", "pid", "phone", "nickname", "name", "operatename", "operateid"]
                center_df.columns = coll_lists
                center_df.dropna(subset=['phone'], axis=0, inplace=True)
                child_center_phone_list.extend(center_df['phone'].tolist())
            not_contains = list(set(all_data_phone) - set(child_center_phone_list))
            contains = list(set(all_data_phone) - set(child_center_phone_list))
            contains.extend(first_child_center)
            # 包含运营中心id
            child_center_df = all_data.loc[all_data['operatename'].notna(), :]
            child_center_df['operateid'] = child_center_df['operateid'].astype(int)
            child_center_id = child_center_df['operateid'].to_list()[1:]
            # 更新记录
            update_record_list = []
            update_record = {'addtime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'contains': len(contains), 'not_contains': len(not_contains), "child_center_id": len(child_center_id)}
            update_record_list.append(update_record)
            ret = {'phone': phone, 'not_contains': not_contains, "contains": contains, "child_center_id": child_center_id, "update_record": update_record_list}
            fina_center_data_list.append(ret)
        fina_df = pd.DataFrame(fina_center_data_list)
        result_df = operate_df.merge(fina_df, how='left', on='phone')

        if mode == 'first':
            conn_rw = pd_conn(analyze_mysql_conf)
            logger.info(conn_rw)
            result_df['contains'] = result_df['contains'].apply(lambda x: json.dumps(x))
            result_df['not_contains'] = result_df['not_contains'].apply(lambda x: json.dumps(x))
            result_df['child_center_id'] = result_df['child_center_id'].apply(lambda x: json.dumps(x))
            result_df['update_record'] = result_df['update_record'].apply(lambda x: json.dumps(x))
            result_df.to_sql("operate_relationship_crm", con=conn_rw, if_exists="append", index=False)
            return True, '添加数据成功'
        else:
            logger.info(result_df)
            return True, result_df
    except:
        logger.error(traceback.format_exc())
        return False, '失败'
    finally:
        # conn_crm.close()
        conn_analyze.close()
# result = operate_relationship_crm()
# logger.info(result[1])


# 更新数据
def refresh_operate_relationship_crm():
    try:
        logger.info('读取新数据')
        new_result = operate_relationship_crm(mode='refresh')
        if not new_result:
            return new_result[1]
        new_operate_df = new_result[1]

        old_operate_sql = '''
            select * from lh_analyze.operate_relationship_crm
        '''
        conn_rw = pd_conn(analyze_mysql_conf)
        conn_rw_refresh = direct_get_conn(analyze_mysql_conf)
        cursor = conn_rw_refresh.cursor()
        old_operate_df = pd.read_sql(old_operate_sql, conn_rw)
        # 判断是否为新的一天
        old_operate_time = old_operate_df['sync_time'].dt.strftime("%Y-%m-%d").values[0]
        flag = False
        if old_operate_time != (date.today()).strftime("%Y-%m-%d"):
            flag = True
        logger.info('进行数据更新')
        for index, row in new_operate_df.iterrows():
            logger.info(index)
            old_row_data = old_operate_df[old_operate_df['id'] == row['id']]
            # 如果为新增数据则插入
            if old_row_data.shape[0] == 0:
                new_data = pd.DataFrame(row).T
                new_data['contains'] = new_data['contains'].apply(lambda x: json.dumps(x))
                new_data['not_contains'] = new_data['not_contains'].apply(lambda x: json.dumps(x))
                new_data['child_center_id'] = new_data['child_center_id'].apply(lambda x: json.dumps(x))
                new_data['update_record'] = new_data['update_record'].apply(lambda x: json.dumps(x))
                # 插入数据
                new_data.to_sql("operate_relationship_crm", con=conn_rw, if_exists="append", index=False)
                continue
            # 已存在数据进行更新
            update_record = {'addtime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'contains': len(row['contains']), 'not_contains': len(row['not_contains']), "child_center_id": len(row['child_center_id'])}
            history_record = eval(old_row_data['update_record'].values[0])
            if flag:
                history_record = [history_record[-1]]
            history_record.append(update_record)
            update_sql = '''update operate_relationship_crm set unionid=%s, parentid=%s, name=%s, phone=%s, operatename=%s, crm=%s, status=%s, not_contains=%s, contains=%s, child_center_id=%s, update_record=%s where id="%s"'''
            update_data = (
            row['unionid'], row['parentid'], row['name'], row['phone'], row['operatename'], row['crm'], row['status'],
            json.dumps(row['not_contains']), json.dumps(row['contains']), json.dumps(row['child_center_id']), json.dumps(history_record), row['id'])
            cursor.execute(update_sql, update_data)
            conn_rw_refresh.commit()
        logger.info('更新完成')
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.info('更新失败')
    finally:
        try:
            conn_rw_refresh.close()
            conn_rw.close()
        except:
            pass

refresh_operate_relationship_crm()
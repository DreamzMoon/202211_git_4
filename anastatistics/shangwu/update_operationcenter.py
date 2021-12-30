# -*- coding: utf-8 -*-
# @Time : 2021/12/30  18:11
# @Author : shihong
# @File : .py
# --------------------------------------
# 更新运营中心数据
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
from datetime import timedelta
from functools import reduce
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token

def update_operatecenter():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze or not conn_crm:
            return "数据库连接失败"
        # 查找旧数据
        new_data_sql = '''select t1.*, t2.nickname from 
            (select * from luke_lukebus.operationcenter) t1
            left join
            (select id, nickname from luke_sincerechat.user) t2
            on t1.unionid = t2.id'''

        old_data_sql = '''
            select * from lh_analyze.operationcenter
        '''
        new_data = pd.read_sql(new_data_sql, conn_crm)
        old_data = pd.read_sql(old_data_sql, conn_analyze)
        new_data_id_list = new_data['id'].tolist()
        old_data_id_list = old_data['id'].tolist()
        sub_id_list = list(set(new_data_id_list) - set(old_data_id_list))
        # 如果没有数据不更新
        if not sub_id_list:
            return "暂无更新数据"
        logger.info('开始更新数据')
        new_data_df = new_data[new_data['id'].isin(sub_id_list)]
        new_data_df['create_time'] = new_data_df['addtime'].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)))
        new_data_df.drop('addtime', axis=1, inplace=True)
        conn_an = pd_conn(analyze_mysql_conf)
        new_data_df.to_sql('operationcenter', con=conn_an, if_exists='append', index=False)
        return '更新完成，更新%s个运营中心' % len(sub_id_list)
    except:
        logger.error(traceback.format_exc())
        return "更新失败"
    finally:
        try:
            conn_crm.close()
            conn_analyze.close()
        except:
            pass

if __name__ == '__main__':
    result = update_operatecenter()
    logger.info(result)
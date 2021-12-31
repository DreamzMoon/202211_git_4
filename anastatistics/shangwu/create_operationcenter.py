# -*- coding: utf-8 -*-
# @Time : 2021/12/31  14:54
# @Author : shihong
# @File : .py
# --------------------------------------
# 创建运营中心
import time
from config import *
from util.help_fun import *
conn_crm = direct_get_conn(crm_mysql_conf)

sql = '''select t1.*, t2.nickname from 
    (select * from luke_lukebus.operationcenter) t1
    left join
    (select id, nickname from luke_sincerechat.user) t2
    on t1.unionid = t2.id
'''
data = pd.read_sql(sql, conn_crm)
data['create_time'] = data['addtime'].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)))
data.drop('addtime', axis=1, inplace=True)
conn_an = pd_conn(analyze_mysql_conf)
data.to_sql('operationcenter', con=conn_an, if_exists='append', index=False)
conn_crm.close()
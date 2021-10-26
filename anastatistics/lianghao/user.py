# -*- coding: utf-8 -*-

# @Time : 2021/10/26 13:38

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : user.py

import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback
'''
vip等级先不拿
'''

conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
sql = '''select id lh_user_id,`status` lh_status,phone,create_time addtime,update_time updtime from lh_user where del_flag = 0'''
lh_datas = pd.read_sql(sql,conn_read)
conn_read.close()

logger.info(lh_datas.shape)
logger.info(lh_datas.loc[0])

crm_mysql_conf["db"] = "luke_sincerechat"
conn_crm = direct_get_conn(crm_mysql_conf)
crm_datas = ""
with conn_crm.cursor() as cursor:
    sql = '''select sex,id unionid,pid parentid,phone,nickname,`name` from user where phone is not null or phone != ""'''
    cursor.execute(sql)
    crm_datas = pd.DataFrame(cursor.fetchall())
    logger.info(len(crm_datas))
    logger.info(crm_datas.loc[0])

user_datas = lh_datas.merge(crm_datas,how="left",on="phone")
conn_crm.close()

logger.info(user_datas.shape)
logger.info(user_datas.loc[0])

#获取用户的实名信息
crm_mysql_conf["db"] = "luke_crm"
conn_crm = direct_get_conn(crm_mysql_conf)
auth_datas = ""
with conn_crm.cursor() as cursor:
    sql = '''select unionid,types auto_type,status vertify_status,address,birth,nationality from authentication '''
    cursor.execute(sql)
    auth_datas = pd.DataFrame(cursor.fetchall())

last_datas = user_datas.merge(auth_datas,how="left",on="unionid")

vip_datas = ""
serpro_datas = ""
with conn_crm.cursor() as cursor:
    sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from user_vip'''
    cursor.execute(sql)
    vip_datas = pd.DataFrame(cursor.fetchall())

    sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from user_serpro'''
    cursor.execute(sql)
    serpro_datas = pd.DataFrame(cursor.fetchall())

last_datas = last_datas.merge(vip_datas,how="left",on="unionid")
last_datas = last_datas.merge(serpro_datas,how="left",on="unionid")


conn_crm.close()

#准备入库
conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
last_datas.to_sql("lh_user", con=conn_rw, if_exists="append", index=False)
logger.info("写入成功")
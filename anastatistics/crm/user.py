# -*- coding: utf-8 -*-

# @Time : 2021/10/26 17:53

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
import pandas as pd

try:
    conn_crm = pd_conn(crm_mysql_conf)
    sql = '''select id unionid,pid parentid,phone,status,nickname,name,sex from luke_sincerechat.user'''
    crm_datas = pd.read_sql(sql,conn_crm)
    logger.info(crm_datas.shape)

    sql = '''select unionid,types auto_type,status vertify_status,address,birth,nationality from luke_crm.authentication'''
    auth_datas = pd.read_sql(sql,conn_crm)
    crm_datas = crm_datas.merge(auth_datas, how="left", on="unionid")

    conn = direct_get_conn(crm_mysql_conf)
    vip_datas = ""
    serpro_datas = ""
    # pandas read_sql 不支持sql时间戳格式化FROM_UNIXTIME 所以这里改用sql查询方式 然后进行pandas 的动态拼接
    with conn.cursor() as cursor:
        sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from luke_crm.user_vip'''
        cursor.execute(sql)
        vip_datas = pd.DataFrame(cursor.fetchall())

        sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from luke_crm.user_serpro'''
        cursor.execute(sql)
        serpro_datas = pd.DataFrame(cursor.fetchall())

    conn.close()

    crm_datas = crm_datas.merge(vip_datas, how="left", on="unionid")
    crm_datas = crm_datas.merge(serpro_datas, how="left", on="unionid")

    logger.info(crm_datas.shape)
    logger.info(crm_datas.describe)
    # logger.info(crm_datas.loc[0])

    # 准备入库
    conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
    crm_datas.to_sql("crm_user", con=conn_rw, if_exists="append", index=False)
    logger.info("写入成功")

except:
    logger.info(traceback.format_exc())
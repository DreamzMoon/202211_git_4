# -*- coding: utf-8 -*-

# @Time : 2021/10/25 9:52

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : history_purchase.py

'''
个人历史采购和采购数量按人统计按天分组
每天凌晨统计数据 00:10:00开始跑
'''
import time
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

try:

    conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
    sql = '''select phone,user_id lh_user_id,sum(total_price) total_money,sum(count) total_count,count(*) order_count from lh_order where del_flag = 0 and `status`=1 and phone != "" and DATE_FORMAT(create_time,"%Y%m%d")  != CURRENT_DATE group by phone order by total_money desc
    '''
    datas = pd.read_sql(sql, conn_read)
    logger.info(datas)
    logger.info("-------")
    conn_read.close()

    logger.info(datas.shape)
    logger.info(len(datas))
    logger.info(datas.loc[0])


    #准备进入数据拼接获取用户信息 获取crm拼接 数据要一条一条查不然有出入 数据匹配不对
    crm_mysql_conf["db"] = "luke_sincerechat"
    conn_crm = direct_get_conn(crm_mysql_conf)
    logger.info(conn_crm)
    if not conn_crm:
        logger.info("conn_crm failed")
        exit()

    # 查出crm的手机号码不等于空的
    crm_datas = ""
    with conn_crm.cursor() as cursor:
        sql = '''select id unionid,`name`,nickname,phone from user where phone is not null or phone != ""'''
        cursor.execute(sql)
        crm_datas = cursor.fetchall()
        crm_datas = pd.DataFrame(crm_datas)

    ok_datas = datas.merge(crm_datas,how="left",on="phone")
    # ok_datas.to_csv(r"e:/1.csv")

    logger.info("-------")
    logger.info(ok_datas.shape)
    logger.info(len(ok_datas))

    conn_crm.close()

    ok_datas.fillna("", inplace=True)
    ok_datas["statistic_time"] = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
    # 删除unionid为空
    last_datas = ok_datas.drop(ok_datas[ok_datas["unionid"] == ""].index)

    # 通过sqlclchemy创建的连接无需关闭
    logger.info("准备写入的数据")
    # logger.info(last_datas)
    logger.info(last_datas.loc[0])
    conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, analyze_mysql_conf)
    last_datas.to_sql("lh_history_pur", con=conn_rw, if_exists="append", index=False)
    logger.info("写入成功")
except:
    logger.exception(traceback.format_exception())

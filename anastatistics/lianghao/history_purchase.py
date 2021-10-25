# -*- coding: utf-8 -*-

# @Time : 2021/10/25 9:52

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : history_purchase.py

'''
个人历史采购和采购数量按人统计按天分组
每天凌晨统计数据 00:10:00开始跑
'''

import sys
sys.path.append(".")
sys.path.append("../")
sys.path.append("../../")
from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date


conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
sql = '''select phone,user_id lh_user_id,sum(total_price) total_money,sum(count) total_count,count(*) order_count from lh_order where del_flag = 0 and `status`=1 and phone != "" and DATE_FORMAT(create_time,"%Y%m%d")  != CURRENT_DATE group by phone order by total_money desc
'''
datas = pd.read_sql(sql, conn_read)
logger.info(datas)
logger.info("-------")
conn_read.close()

#准备进入数据拼接获取用户信息 获取crm拼接 数据要一条一条查不然有出入 数据匹配不对
crm_mysql_conf["db"] = "luke_sincerechat"
conn_crm = direct_get_conn(crm_mysql_conf)
logger.info(conn_crm)
if not conn_crm:
    exit()

with conn_crm.cursor() as cursor:
    for i in range(datas.shape[0]):
        logger.info("i:%s" %i)
        sql = '''select id unionid,`name`,nickname  from user where phone = %s'''
        phone = datas.loc[i,"phone"]
        cursor.execute(sql,(phone))
        data = cursor.fetchone()
        logger.info(data)
        if data:
            datas.loc[i,["unionid","name","nickname"]] = data.values()
            logger.info(datas)
        else:
            pass

#由于靓号数据有些在crm不存在 这样子查会少了
# phone_lists = tuple(datas["phone"].to_list())
# logger.info(phone_lists)
# logger.info("len_phone_lists:%s" %len(phone_lists))

# with conn_crm.cursor() as cursor:
#     sql = '''select id unionid,`name`,nickname  from user where phone in {}'''
#     sql = sql.format(phone_lists)
#     cursor.execute(sql)
#     crm_datas = cursor.fetchall()
#     # logger.info(crm_datas)
#     # logger.info(len(crm_datas))
#     crm_datas = pd.DataFrame(crm_datas)
#     logger.info("crm_length:%s" %len(crm_datas))
#     datas.loc[:,["unionid","name","nickname"]] = crm_datas




conn_crm.close()

datas.fillna("",inplace=True)
datas["statistic_time"] = [(date.today()+timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")]*len(datas)
#删除unionid为空
last_datas = datas.drop(datas[datas["unionid"]==""].index)


# 通过sqlclchemy创建的连接无需关闭
logger.info("准备写入的数据")
logger.info(datas)
conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
datas.to_sql("lh_history_pur",con=conn_rw,if_exists="append",index=False)
logger.info("写入成功")

# conn_rw = ssh_get_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
# last_datas = last_datas.values.tolist()
# logger.info(last_datas)
#
# # 批量插入
# with conn_rw.cursor() as cursor:
#     insert_sql = '''insert into lh_history_pur (phone,lh_user_id,total_money,totaL_count,order_count,unionid,name,nickname,statistic_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
#     cursor.executemany(insert_sql,last_datas)
#     conn_rw.commit()
#
# conn_rw.close()
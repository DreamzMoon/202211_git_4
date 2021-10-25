# -*- coding: utf-8 -*-

# @Time : 2021/10/25 9:52

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : history_purchase.py

'''
个人历史采购和采购数量按人统计按天分组
'''

import sys
sys.path.append(".")
sys.path.append("../")
sys.path.append("../../")
from config import *
from util.help_fun import *
import json


conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
sql = '''select phone,sum(total_price) total_money,sum(count) total_count from lh_order where del_flag = 0 and `status`=1 and phone !="" and phone is not null group by phone order by total_money desc limit 3'''
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
        sql = '''select id unionid,`name`,nickname  from user where phone = %s'''
        logger.info("phone:%s" %datas.loc[i,"phone"])
        phone = datas.loc[i,"phone"]
        cursor.execute(sql,(phone))
        data = cursor.fetchone()
        logger.info(data)
        if data:
            datas.loc[i,["unionid","name","nickname"]] = data.values()
            logger.info(datas)
        else:
            pass

conn_crm.close()


#重新指定列
# last_data = {}
# last_data["unionid"] = datas["unionid"]
# last_data["name"] = datas["name"]
# last_data["nickname"] = datas["nickname"]
# last_data["phone"] = datas["phone"]
# last_data["total_money"] = datas["total_money"]
# last_data["total_count"] = datas["total_count"]
# last_data=pd.DataFrame(last_data)
# logger.info(last_data)

logger.info("准备写入")
# 通过sqlclchemy创建的连接无需
conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
datas.to_sql("lh_history_pur",con=conn_rw,if_exists="replace",index=False)
# conn_rw.close()
logger.info("写入成功")

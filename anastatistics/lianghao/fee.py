# -*- coding: utf-8 -*-

# @Time : 2021/10/22 9:46

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : fee.py
'''
费用统计
'''


import sys
sys.path.append(".")
sys.path.append("../")
sys.path.append("../../")
from config import *
from util.help_fun import *




conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
# 执行查询卖方手续费 买方手续费的统计
data = pd.read_sql("select DATE_FORMAT(create_time, '%Y%m%d') AS date_time, count(*) count,sum(count) total_count,sum(total_price) total_price,sum(sell_fee) sell_fee,sum(fee) fee from lh_order where `status` = 1 and  del_flag = 0 and type in (1,4) group by date_time order by date_time desc", conn_read)
# logger.info(data)
conn_read.close()

logger.info("准备写入")
# 通过sqlclchemy创建的连接无需
conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
testdata = pd.read_sql("select * from user limit 2",conn_rw)
logger.info(testdata)
logger.info(conn_rw)
testdata.to_sql("lh_fee_test",con=conn_rw,if_exists="replace",index=False)
# conn_rw.close()
logger.info("写入成功")
























# 直连的无问题
# from sqlalchemy import create_engine
# import pandas as pd
#
#
# # engine = create_engine("mysql://root:root@localhost:3306/card")
# engine = create_engine("mysql+pymysql://root:root@localhost:3306/card",encoding='utf-8')
# con = engine.connect()
# logger.info(con)
# df = pd.DataFrame(['A','B'],columns=['new_tablecol'])
# df.to_sql(name='new_table',con=con,if_exists='append')
# con.close()
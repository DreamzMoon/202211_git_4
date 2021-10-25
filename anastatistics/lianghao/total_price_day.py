# -*- coding: utf-8 -*-
'''
总费用统计
'''
import sys
sys.path.append(".")
sys.path.append("../")
sys.path.append("../../")
from config import *
from util.help_fun import *


conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
with conn_read.cursor() as cursor:
    sql = 'insert into lh_analyze.lh_total_price_test(statistic_time, order_count, total_count, sell_fee, buyer_fee, total_price) values (select date_format(create_time, "%y-%m-%d") statistic_time, count(*) order_count, sum(count) total_count, sum(sell_fee) sell_fee, sum(fee) buyer_fee, sum(total_price) total_price from lh_pretty_client.lh_order where `status` = 1 and del_flag = 0 group by statistic_time having statistic_time <> curdate() order by statistic_time desc)'

    cursor.execute(sql)
    logger.info('插入成功')
conn_read.close()
# 执行查询总费用的统计
# data = pd.read_sql("select date_format(create_time, '%y-%m-%d') statistic_time, sum(count) total_count, sum(total_price) total_price, sum(fee) buyer_fee, sum(sell_fee) sell_fee from lh_order where `status` = 1 and del_flag = 0 group by statistic_time  having statistic_time <> curdate() order by statistic_time desc limit 10", conn_read)
# logger.info(data)
# conn_read.close()
# #
#
# logger.info("总费用准备写入")
# # 通过sqlclchemy创建的连接无需关闭
# conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
# logger.info(conn_rw)
# data.to_sql("lh_total_price_test",con=conn_rw,if_exists="append",index=False)
# logger.info("写入成功")


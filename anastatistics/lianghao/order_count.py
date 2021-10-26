# -*- coding: utf-8 -*-
'''
总费用统计、官方订单统计、转让订单统计
'''
import sys, os
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

import traceback
from config import *
from util.help_fun import *

# 建表插数据
def init_table(table_name):
    if table_name == 'lh_total_price':
        select_sql = 'select date_format(create_time, "%Y-%m-%d") statistic_time, count(*) order_count, sum(count) total_count, sum(sell_fee) sell_fee, sum(fee) buyer_fee, sum(total_price) total_price from lh_order where `status` = 1 and del_flag = 0 group by statistic_time having statistic_time < curdate() order by statistic_time desc'
    elif table_name == 'lh_official_total_price':
        select_sql = 'select date_format(create_time, "%Y-%m-%d") statistic_time, count(*) order_count, sum(count) total_count, sum(sell_fee) sell_fee, sum(fee) buyer_fee, sum(total_price) total_price from lh_order where `status` = 1 and del_flag = 0 and type = 0 group by statistic_time having statistic_time < curdate() order by statistic_time desc'
    elif table_name == 'lh_transfer_total_price':
        select_sql = 'select date_format(create_time, "%Y-%m-%d") statistic_time, count(*) order_count, sum(count) total_count, sum(sell_fee) sell_fee, sum(fee) buyer_fee, sum(total_price) total_price from lh_order where `status` = 1 and del_flag = 0 and type in (1, 4) group by statistic_time having statistic_time < curdate() order by statistic_time desc'
    else:
        return logger.error('请输入正确的表名')
    try:
        conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        with conn_read.cursor() as cursor:
            cursor.execute(select_sql)
            select_data = cursor.fetchall()
            logger.info('%s查询成功' % table_name)
        conn_read.close()

        conn_rw = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
        with conn_rw.cursor() as cursor:
            insert_sql = "insert into {}(`statistic_time`, `order_count`, `total_count`, `sell_fee`, `buyer_fee`, `total_price`) values (%s, %s, %s, %s, %s, %s)".format(table_name)
            cursor.executemany(insert_sql, select_data)
            logger.info('插入成功')
        conn_rw.commit()
        conn_rw.close()
    except:
        logger.error(traceback.format_exc())
        logger.error('表数据初始化异常')



def count_order_data():
    try:
        # 执行查询总费用的统计
        conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        total_data = pd.read_sql("select date_format(create_time, '%Y-%m-%d') statistic_time, count(*) order_count, sum(count) total_count, sum(sell_fee) sell_fee, sum(fee) buyer_fee, sum(total_price) total_price from lh_order where `status` = 1 and del_flag = 0 and date_format(create_time, '%Y-%m-%d') = date_sub(curdate(), interval 1 day)", conn_read)
        logger.info("总费用查询完成")
        official_total_data = pd.read_sql("select date_format(create_time, '%Y-%m-%d') statistic_time, count(*) order_count, sum(count) total_count, sum(sell_fee) sell_fee, sum(fee) buyer_fee, sum(total_price) total_price from lh_order where `status` = 1 and del_flag = 0 and type = 0 and date_format(create_time, '%Y-%m-%d') = date_sub(curdate(), interval 1 day)", conn_read)
        logger.info("官方总费用查询成功")
        transfer_total_data = pd.read_sql("select date_format(create_time, '%Y-%m-%d') statistic_time, count(*) order_count, sum(count) total_count, sum(sell_fee) sell_fee, sum(fee) buyer_fee, sum(total_price) total_price from lh_order where `status` = 1 and del_flag = 0 and type in (1, 4) and date_format(create_time, '%Y-%m-%d') = date_sub(curdate(), interval 1 day)", conn_read)
        logger.info("转让费用查询成功")
        conn_read.close()
        logger.info("准备写入")

        # 通过sqlclchemy创建的连接无需关闭
        conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
        logger.info(conn_rw)
        total_data.to_sql("lh_total_price", con=conn_rw,if_exists="append",index=False)
        logger.info("总费用写入成功")
        official_total_data.to_sql("lh_official_total_price", con=conn_rw,if_exists="append",index=False)
        logger.info("官方总费用写入成功")
        transfer_total_data.to_sql("lh_transfer_total_price", con=conn_rw, if_exists="append", index=False)
        logger.info("转让费用写入成功")
    except:
        logger.error(traceback.format_exc())
        logger.error('费用数据统计失败')


count_order_data()


# table_list = ['lh_total_price', 'lh_official_total_price', 'lh_transfer_total_price']
# for table in table_list:
#     init_table(table)

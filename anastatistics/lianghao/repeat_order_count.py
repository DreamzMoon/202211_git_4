# -*- coding: utf-8 -*-
'''
重复订单数统计
'''
import sys, os
import traceback
from datetime import date
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from config import *
from util.help_fun import *

def clear():
    # 清除数据
    conn_lh_rw = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
    with conn_lh_rw.cursor() as cursor:
        clear_sql = "delete from lh_repeat_order_count"
        cursor.execute(clear_sql)
        logger.info('数据清除成功')
    conn_lh_rw.commit()
    conn_lh_rw.close()


def repeat_order_data():
    try:
        # 重复订单查询
        conn_lh_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        repeat_order_sql = '''
            select
                user_id, phone, count(*) repeat_count, sum(total_price) total_price, sum(count) total_count from lh_order
            where (now() - create_time) > 10 and `status` <> 1
            group by user_id
        '''
        logger.info('重复订单查询')
        repeat_data = pd.read_sql(repeat_order_sql, conn_lh_read)
        conn_lh_read.close()
        logger.info('重复订单查询结束')
        logger.info(repeat_data.shape)

        #用户信息查询
        crm_mysql_conf["db"] = "luke_sincerechat"
        conn_crm = direct_get_conn(crm_mysql_conf)
        logger.info(conn_crm)
        if not conn_crm:
            logger.info("conn_crm failed")
            exit()
        with conn_crm.cursor() as cursor:
            sql = '''select id as unionid, phone, `name`, nickname from user where phone is not null and phone != '' '''
            cursor.execute(sql)
            crm_data = cursor.fetchall()
        conn_crm.close()

        logger.info("准备写入")
        crm_data = pd.DataFrame(crm_data)
        repeat_data["statistic_time"] = (date.today()).strftime("%Y-%m-%d")
        repeat_data = repeat_data.merge(crm_data, how='left', on='phone')
        repeat_data = repeat_data.loc[:, ['statistic_time', 'user_id', 'unionid', 'name', 'nickname', 'phone', 'repeat_count', 'total_price', 'total_count']]
        logger.info(repeat_data.shape)
        # 通过sqlclchemy创建的连接无需关闭
        conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
        logger.info(conn_rw)
        repeat_data.to_sql("lh_repeat_order_count", con=conn_rw, if_exists="append", index=False)
        logger.info("重复订单数统计写入成功")
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('重复订单数统计失败')

repeat_order_data()
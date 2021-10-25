# -*- coding: utf-8 -*-
'''
重复订单数统计
'''
import sys, os
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from config import *
from util.help_fun import *

def repeat_order_data():
    # 清除数据
    # conn_lh_rw = ssh_get_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
    # with conn_lh_rw.cursor() as cursor:
    #     clear_sql = "delete from lh_repeat_order_count"
    #     cursor.execut(clear_sql)
    #     logger.info('数据清除成功')
    # conn_lh_rw.commit()
    # conn_lh_rw.close()


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

    repeat_data['nickname'] = repeat_data['nickname']
    # 用户信息查询
    crm_mysql_conf["db"] = "luke_sincerechat"
    conn_crm = direct_get_conn(crm_mysql_conf)
    logger.info(conn_crm)
    if not conn_crm:
        logger.info("conn_crm failed")
        exit()
    with conn_crm.cursor() as cursor:
        for i in range(repeat_data.shape[0]):
            logger.info("i:%s" % i)
            sql = '''select id unionid,`name`,nickname  from user where phone = %s'''
            phone = repeat_data.loc[i, "phone"]
            cursor.execute(sql, (phone))
            data = cursor.fetchone()
            logger.info(data)
            if data:
                repeat_data.loc[i, ["unionid", "name", "nickname"]] = data.values()
            else:
                pass
    conn_crm.close()
    logger.info(repeat_data)

    #     logger.info("准备写入")
    #
    #     # 通过sqlclchemy创建的连接无需关闭
    #     conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
    #     logger.info(conn_rw)
    #     transfer_total_data.to_sql("lh_transfer_total_price", con=conn_rw, if_exists="append", index=False)
    #     logger.info("转让费用写入成功")
    # except:
    #     return logger.error("数据更新异常")


repeat_order_data()
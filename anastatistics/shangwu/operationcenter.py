# -*- coding: utf-8 -*-
'''
禄可商务运营中心
'''
import sys, os
import traceback
from datetime import date
from functools import reduce
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from config import *
from util.help_fun import *

def operationcenter_info():
    try:
        logger.info('获取禄可商务运营中心数据')
        crm_mysql_conf["db"] = "luke_lukebus"
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = '''
                select
                    unionid, punionid parentid, capacity, `name`, telephone phone, operatename, is_factory, status, crm, from_unixtime(addtime, '%Y-%m-%d %H:%i:%s') addtime
                from
                    operationcenter
            '''
            cursor.execute(sql)
            center_data = cursor.fetchall()
        conn_crm.close()

        center_df = pd.DataFrame(center_data)

        # 插入数据
        logger.info('写入运营中心数据')
        conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, analyze_mysql_conf)
        logger.info(conn_rw)
        center_df.to_sql("bus_operationcenter", con=conn_rw, if_exists="append", index=False)
        logger.info('写入运营中心数据完成')
    except:
        logger.error('写入运营中心数据失败')
        logger.error(traceback.format_exc())


operationcenter_info()
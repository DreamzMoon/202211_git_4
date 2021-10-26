# -*- coding: utf-8 -*-
'''
禄可商务用户表
'''

import sys, os
import traceback
from datetime import date
from functools import reduce
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from config import *
from util.help_fun import *

def luke_business():
    try:
        # luke_bus user
        logger.info('获取禄可商务用户数据')
        crm_mysql_conf["db"] = "luke_lukebus"
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = \
                '''
                select
                    unionid, punionid parentid, phone, u_nickname nickname, capacity, `exclusive`, `status` bus_status, from_unixtime(addtime,'%Y-%m-%d %H:%i:%s') addtime, from_unixtime(udptime,'%Y-%m-%d %H:%i:%s') udptime, from_unixtime(invtime,'%Y-%m-%d %H:%i:%s') invtime
                from
                    `user`
                where
                    phone is not null
                and
                    phone != ''
                '''
            cursor.execute(sql)
            bus_data = cursor.fetchall()
        conn_crm.close()
        logger.info('获取禄可商务用户数据完成')

        # luke_crm authentication
        logger.info('获取crm用户认证数据')
        crm_mysql_conf["db"] = "luke_crm"
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = \
                '''
                select
                    unionid, identity, types auto_type, `status` vertify_status, sex, `name`, address, birth, nationality, from_unixtime(extime,'%Y-%m-%d %H:%i:%s') extime
                from
                    luke_crm.authentication
                '''
            cursor.execute(sql)
            authentication_data = cursor.fetchall()
        conn_crm.close()
        logger.info('获取crm用户认证数据完成')

        # luke_crm user_serpro
        logger.info('获取crm用户服务商数据')
        crm_mysql_conf["db"] = "luke_crm"
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = \
                '''
                select
                    unionid, grade provider_garde, `status` provider_status, from_unixtime(starttime,'%Y-%m-%d %H:%i:%s') provider_starttime
                from
                    luke_crm.user_serpro
                '''
            cursor.execute(sql)
            serpro_data = cursor.fetchall()
        conn_crm.close()
        logger.info('获取crm用户服务商数据完成')

        # luke_crm user_vip
        logger.info('获取crm用户vip数据')
        crm_mysql_conf["db"] = "luke_crm"
        conn_crm = direct_get_conn(crm_mysql_conf)
        with conn_crm.cursor() as cursor:
            sql = \
                '''
                select
                    unionid, grade vip_grade, from_unixtime(starttime,'%Y-%m-%d %H:%i:%s')  vip_starttime, from_unixtime(endtime,'%Y-%m-%d %H:%i:%s')  vip_endtime
                from
                    luke_crm.user_vip
                '''
            cursor.execute(sql)
            vip_data = cursor.fetchall()
        conn_crm.close()
        logger.info('获取crm用户vip数据完成')

        # 数据合并
        df_list = []
        bus_df = pd.DataFrame(bus_data)
        df_list.append(bus_df)
        authentication_df = pd.DataFrame(authentication_data)
        df_list.append(authentication_df)
        serpro_df = pd.DataFrame(serpro_data)
        df_list.append(serpro_df)
        vip_df = pd.DataFrame(vip_data)
        df_list.append(vip_df)
        df_merged = reduce(lambda left,right: pd.merge(left, right, on=['unionid'], how='left'), df_list)

        # 数据写入
        logger.info('写入用户数据')
        conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf, lianghao_rw_mysql_conf)
        logger.info(conn_rw)
        df_merged.to_sql("bus_user", con=conn_rw, if_exists="append", index=False)
        logger.info('写入用户数据完成')
    except:
        logger.error('禄可商户用户数据更新失败')
        logger.error(traceback.format_exc())


luke_business()
# -*- coding: utf-8 -*-

# @Time : 2021/9/27 15:43

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : help_fun.py

import sys
sys.path.append("../")
from config import *
import pymysql
pymysql.install_as_MySQLdb()
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy as sa
import redis


def ssh_get_conn(ssh_conf,mysql_conf):
    '''
    利用ssh通道连接数据库
    :param ssh_conf: 服务器配置
    :param mysql_conf: 数据库配置
    :return:  数据库的连接器
    '''
    try:
        server = SSHTunnelForwarder(
            (ssh_conf["host"], ssh_conf["port"]),
            ssh_password=ssh_conf["ssh_password"],
            ssh_username=ssh_conf["ssh_username"],
            remote_bind_address=(mysql_conf["host"], mysql_conf["port"])
        )
        server.start()
        client = pymysql.connect(host='127.0.0.1',  # 此处必须是是127.0.0.1
                                    port=server.local_bind_port,
                                    user=mysql_conf["user"],
                                    passwd=mysql_conf["password"],
                                    db=mysql_conf["db"],
                                    charset="utf8mb4")
        return client
    except:
        return None





def ssh_get_sqlalchemy_conn(ssh_conf,mysql_conf):
    '''
    利用ssh通道连接数据库 pandas连接可以 用这个 这个连接器可以实现to_sql
    :param ssh_conf: 服务器配置
    :param mysql_conf: 数据库配置
    :return:  数据库的连接器
    '''
    try:
        server = SSHTunnelForwarder(
            (ssh_conf["host"], ssh_conf["port"]),
            ssh_password=ssh_conf["ssh_password"],
            ssh_username=ssh_conf["ssh_username"],
            remote_bind_address=(mysql_conf["host"], mysql_conf["port"])
        )
        server.start()

        sql_conf = {}
        sql_conf["user"] = mysql_conf["user"]
        sql_conf["port"] = server.local_bind_port
        sql_conf["host"] = "127.0.0.1"
        sql_conf["password"] = mysql_conf["password"]
        sql_conf["db"] = mysql_conf["db"]

        client = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(db)s?charset=utf8mb4' % sql_conf,encoding='utf-8')

        return client
    except:
        return None

def direct_get_conn(sql_conf):
    '''
    :param sql_conf: 数据库配置
    :return: 数据库连接器
    '''
    try:
        conn = pymysql.connect(**sql_conf)
        return conn
    except:
        return None

def get_connection():
    try:
        conn = pymysql.connect(**mysql_conf)
        return conn
    except:
        return None

def pd_conn(sql_conf):
    '''
    :param sql_conf: 数据库连接
    :return:数据库引擎
    '''
    try:
        # 这样子连接可以对号入座 如果写死的话 密码里面有特殊字符会被转义 比如 con = create_engine('mysql+pymysql://root:luke@20193306@47.98.131.135:43306/expect_dev?charset=utf-8')
        conn_engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(db)s?charset=utf8mb4' % sql_conf,encoding='utf-8')
        return conn_engine
    except:
        return None

def sqlalchemy_conn(sql_conf):
    '''
    该数据库是基于orm的进行查询
    :param sql_conf: 数据库连接
    :return:数据库引擎
    '''
    try:
        # 这样子连接可以对号入座 如果写死的话 密码里面有特殊字符会被转义 比如 con = create_engine('mysql+pymysql://root:luke@20193306@47.98.131.135:43306/expect_dev?charset=utf-8')
        conn_engine = create_engine('mysql://%(user)s:%(password)s@%(host)s:%(port)d/%(db)s?charset=utf8mb4' % sql_conf,encoding='utf-8')
        return conn_engine
    except:
        return None


def ssh_redis():
    try:
        server = SSHTunnelForwarder(
            (lianghao_ssh_conf["host"], lianghao_ssh_conf["port"]),
            ssh_password=lianghao_ssh_conf["ssh_password"],
            ssh_username=lianghao_ssh_conf["ssh_username"],
            remote_bind_address=(redis_host, redis_port)
        )
        server.start()
        if redis_password:
            rclient = redis.Redis(host="127.0.0.1",
                                  port=server.local_bind_port,
                                  password=redis_password,
                                  db=redis_db,
                                  decode_responses=True)
        else:
            rclient = redis.Redis(host="127.0.0.1",
                                  port=server.local_bind_port,
                                  db=redis_db,
                                  decode_responses=True)
        return rclient
    except:
        return None


def get_redis():
    try:
        logger.info("redis_host:%s redis_port:%s redis_pass:%s db:%s" %(redis_host,redis_port,redis_port,redis_db))
        if redis_password:
            rclient = redis.Redis(host=redis_host,
                                  port=redis_port,
                                  password=redis_password,
                                  db=redis_db,
                                  decode_responses=True)
        else:
            rclient = redis.Redis(host=redis_host,
                                  port=redis_port,
                                  db=redis_db,
                                  decode_responses=True)
        return rclient
    except:
        return None

if __name__ == "__main__":
    logger.info("start")

    rclient = ssh_redis()
    logger.info(rclient)
    logger.info(rclient.get("hello"))

    # 跳板机连接
    # conn = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)
    # with conn.cursor() as cursor:
    #     # sql = "select * from lh_user where phone = 13559436425"
    #     sql = 'show tables'
    #     cursor.execute(sql)
    #     data = cursor.fetchall()
    #     logger.info(data)
    # conn.close()


    # 引擎是不需要关闭的
    # conn_engine = pd_conn(qw3_mysql_conf)
    # data = pd.read_sql("select * from t_user limit 20",conn_engine)
    # logger.info(data)


    #通道pandas 如果是跳板机 可以先生成conn连接器
    # conn = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
    # data = pd.read_sql("select * from lh_user order by create_time desc limit 20", conn)
    # logger.info(data)

    # sqlalchemy pandas
    # conn = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_mysql_conf)
    # data = pd.read_sql("select * from lh_user order by create_time desc limit 20", conn)
    # logger.info(data)

    #直连
    # logger.info(crm_mysql_conf)
    # conn = direct_get_conn(crm_mysql_conf)
    # # conn = get_connection()
    # logger.info(conn)
    # cursor = conn.cursor()
    # sql = '''show tables'''
    # cursor.execute(sql)
    # data = cursor.fetchall()
    # logger.info(data)

    #crm
    # conn_engine = pd_conn(crm_mysql_conf)
    # sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from luke_crm.user_vip'''
    # data = pd.read_sql(sql,conn_engine)
    # logger.info(data)




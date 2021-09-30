# -*- coding: utf-8 -*-

# @Time : 2021/9/27 15:43

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : help_fun.py

import sys
sys.path.append("../")
from config import *
import pymysql
from sshtunnel import SSHTunnelForwarder


def get_conn(ssh_conf,mysql_conf):
    '''
    利用ssh通道连接数据库
    :param ssh_conf: 服务器配置
    :param mysql_conf: 数据库配置
    :return:
    '''
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
                                charset="utf8")
    return client

if __name__ == "__main__":
    conn = get_conn(lianghao_ssh_conf,lianghao_mysql_conf)
    with conn.cursor() as cursor:
        sql = "select * from lh_user where phone = 13559436425"
        cursor.execute(sql)
        data = cursor.fetchone()
        logger.info(data)
    conn.close()
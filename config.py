# -*- coding: utf-8 -*-

# @Time : 2021/9/27 15:37

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : config.py

#设置日志等级
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from env import *

if ENV == "test":
    pass
elif ENV == "pro":
    pass
else:
    pass


lianghao_ssh_conf = {
    "host":"47.117.125.39",
    "ssh_username":"office",
    "ssh_password":"luke2020",
    "port":22
}

#公共模块
lianghao_mysql_conf = {
  "host": 'pc-uf6p512w5q51z3k72.rwlb.rds.aliyuncs.com',
  "port": 3306,
  "user": 'lh_read',
  "password": 'fBaVM4MMS8Myx9g6',
  "db": 'lh_pretty_client',
  "charset": "utf8",
}

#默认配置
ssh_conf = {}
mysql_conf = {}

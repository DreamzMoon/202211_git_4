# -*- coding: utf-8 -*-

# @Time : 2021/10/25 19:29

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : recycle.py

import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback


conn_rw = ssh_get_conn(lianghao_ssh_conf,lianghao_mysql_conf)

# -*- coding: utf-8 -*-
# @Time : 2022/1/10  10:57
# @Author : shihong
# @File : .py
# --------------------------------------
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
import pandas as pd
from analyzeserver.common import *
import numpy as np
import time
import oss2

base_url = 'https://luke-sincerechat.oss-cn-beijing.aliyuncs.com'
auth = oss2.Auth(AccessKeyID, AccessKeySecret)
bucket = oss2.Bucket(auth, endpoint, 'luke-analyze')

def adress_split(addr):
    pass


# 图片上传函数
def sub_img(data):
    identify_front_url = base_url + '/' + data['identify_front']
    input = requests.get('')
    # 填写Object完整路径。Object完整路径中不能包含Bucket名称。
    bucket.put_object('exampleobject.txt', input)


# 获取用户图片信息
user_info_sql_1 = '''
    select unionid, head_pic user_icon from luke_crm.user_info
'''
user_info_sql_2 = '''
    select unionid, identity, pic_just identify_front, pic_back identify_back from luke_crm.authentication
'''
user_info_sql_3 = '''
    select unionid, pic_head face_pic from luke_crm.authentication_livingthing
'''

conn_crm = direct_get_conn(crm_mysql_conf)
conn_analyze = direct_get_conn(analyze_mysql_conf)

user_info_1 = pd.read_sql(user_info_sql_1, conn_crm)
user_info_2 = pd.read_sql(user_info_sql_2, conn_crm)
user_info_3 = pd.read_sql(user_info_sql_3, conn_crm)

user_info = user_info_1.merge(user_info_2, how='outer', on='unionid')
user_info = user_info.merge(user_info_3, how='outer', on='unionid')

for index, rows in user_info.iterrows():
    sub_img(rows)

# -*- coding: utf-8 -*-
# @Time : 2022/1/10  10:57
# @Author : shihong
# @File : .py
# --------------------------------------
'''crm用户地址、图片信息首次入库'''
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
from functools import reduce
import numpy as np
import time
import oss2

base_url = 'https://luke-sincerechat.oss-cn-beijing.aliyuncs.com'
# auth = oss2.Auth(AccessKeyID, AccessKeySecret)
# bucket = oss2.Bucket(auth, endpoint, 'luke-analyze')

# 图片上传函数
# def sub_img(index, row_data):
#     img_url_dict = {}
#     # 身份证正面
#     if str(row_data['identifyfront']) != 'nan':
#         identify_front_url = base_url + '/' + row_data['identifyfront']
#         img_url_dict['identifyfront'] = identify_front_url
#     # 身份证反面
#     if str(row_data['identifyback']) != 'nan':
#         identify_back_url = base_url + '/' + row_data['identifyback']
#         img_url_dict['identifyback'] = identify_back_url
#     # 头像
#     if str(row_data['usericon']) != 'nan':
#         usericon_url = base_url + '/' + row_data['usericon']
#         img_url_dict['usericon'] = usericon_url
#     # 活体认证
#     if str(row_data['facepic']) != 'nan':
#         face_pic_url = base_url + '/' + row_data['facepic']
#         img_url_dict['facepic'] = face_pic_url
#     if len(img_url_dict) == 0:
#         return
#     unionid = row_data['unionid']
#     t = int(time.time() * 1000)
#     for k, v in img_url_dict.items():
#         img_data = requests.get(v)
#         file_name = k + str(t)
#         # 填写Object完整路径。Object完整路径中不能包含Bucket名称。
#         bucket.put_object('userinfo/%s/%s.jpg' % (unionid, file_name), img_data)
#         return_url = "https://luke-analyze.oss-cn-beijing.aliyuncs.com/userinfo/%s/%s.jpg" % (unionid, file_name)
#         user_info.loc[index, k] = v
#         logger.info(return_url)


# 当前住所地址

def sub_img(index, row_data):
    img_url_dict = {}
    # 身份证正面
    if str(row_data['identifyfront']) != 'nan':
        identify_front_url = base_url + '/' + row_data['identifyfront']
        img_url_dict['identifyfront'] = identify_front_url
    # 身份证反面
    if str(row_data['identifyback']) != 'nan':
        identify_back_url = base_url + '/' + row_data['identifyback']
        img_url_dict['identifyback'] = identify_back_url
    # 头像
    if str(row_data['usericon']) != 'nan':
        usericon_url = base_url + '/' + row_data['usericon']
        img_url_dict['usericon'] = usericon_url
    # 活体认证
    if str(row_data['facepic']) != 'nan':
        face_pic_url = base_url + '/' + row_data['facepic']
        img_url_dict['facepic'] = face_pic_url
    if len(img_url_dict) == 0:
        return
    # unionid = row_data['unionid']
    # t = int(time.time() * 1000)
    for k, v in img_url_dict.items():
        # img_data = requests.get(v)
        # file_name = k + str(t)
        # # 填写Object完整路径。Object完整路径中不能包含Bucket名称。
        # bucket.put_object('userinfo/%s/%s.jpg' % (unionid, file_name), img_data)
        # return_url = "https://luke-analyze.oss-cn-beijing.aliyuncs.com/userinfo/%s/%s.jpg" % (unionid, file_name)
        user_info.loc[index, k] = v
        # logger.info(return_url)

def now_address_split(df):
    try:
        now_add_sql = '''
            WITH RECURSIVE temp as (
                SELECT t.id,t.pid,t.name FROM luke_crm.address t WHERE id = %s
                UNION ALL
                SELECT t1.id,t1.pid,t1.name FROM luke_crm.address t1 INNER JOIN temp ON t1.id = temp.pid
            )
            SELECT * FROM temp
        '''
        crm_conn = direct_get_conn(crm_mysql_conf)
        cursor = crm_conn.cursor()
        df['province'] = None
        df['city'] = None
        df['region'] = None
        df['town'] = None
        df['address_detail'] = None
        for index, row_data in df.iterrows():
            logger.info(index)
            cursor.execute(now_add_sql, row_data['area_id'])
            datas = cursor.fetchall()
            datas = datas[::-1]
            if datas[0]['name'] != '中国':
                area_detail = ''
                for data in datas:
                    area_detail += data['name']
                df.loc[index, 'address_detail'] = area_detail
            else:
                address_columns = ['province', 'city', 'region', 'town', 'address_detail']
                for _, data in enumerate(datas[1:]):
                    df.loc[index, address_columns[_]] = data['name']
        return df
    except:
        logger.info(traceback.format_exc())
    finally:
        try:
            crm_conn.close()
        except:
            pass

start_time = time.time()
try:
    # 获取用户图片信息
    # 用户头像
    user_info_sql_1 = '''
        select unionid, head_pic usericon from luke_crm.user_info where head_pic is not null and head_pic!= ''
    '''
    # 用户身份证、身份证正反面
    user_info_sql_2 = '''
        select unionid, identity, pic_just identifyfront, pic_back identifyback, issue from luke_crm.authentication
    '''
    # 人脸照片
    user_info_sql_3 = '''
        select unionid, pic_head facepic from luke_crm.authentication_livingthing
    '''
    # 用户所在地
    user_area_sql = '''
        select id unionid, area area_id from luke_sincerechat.user
    '''

    conn_crm = direct_get_conn(crm_mysql_conf)
    conn_analyze = direct_get_conn(analyze_mysql_conf)

    df_list = []
    user_area = pd.read_sql(user_area_sql, conn_crm)
    user_info_1 = pd.read_sql(user_info_sql_1, conn_crm)
    user_info_2 = pd.read_sql(user_info_sql_2, conn_crm)
    user_info_3 = pd.read_sql(user_info_sql_3, conn_crm)
    df_list.append(user_area)

    # 获取用户头像完整地址
    logger.info('合并头像地址')
    for index, rows in user_info_1.iterrows():
        logger.info(index)
        usericon_url = base_url + '/' + rows['usericon']
        user_info_1.loc[index, 'usericon'] = usericon_url
    df_list.append(user_info_1)
    # 获取用户身份证
    # 合并身份证地址
    for index, rows in user_info_2.iterrows():
        logger.info(index)
        identify_front_url = base_url + '/' + rows['identifyfront']
        user_info_2.loc[index, 'identifyfront'] = identify_front_url
        # 身份证反面
        identify_back_url = base_url + '/' + rows['identifyback']
        user_info_2.loc[index, 'identifyback'] = identify_back_url
    df_list.append(user_info_2)

    # 人脸照片
    logger.info('合并人脸地址')
    for index, rows in user_info_3.iterrows():
        logger.info(index)
        face_pic_url = base_url + '/' + rows['facepic']
        user_info_3.loc[index, 'facepic'] = face_pic_url
    df_list.append(user_info_3)

    # 当前地址
    area_df = user_area[(user_area['area_id'].notna()) & (user_area['area_id'] != '') & (user_area['area_id'] != 0)]
    logger.info('分割当前地址')
    area_df = now_address_split(area_df)
    area_df.drop('area_id', axis=1, inplace=True)
    df_list.append(area_df)

    user_info = reduce(lambda left,right: pd.merge(left, right, on=['unionid'], how='left'), df_list)

    user_info.drop('area_id', axis=1, inplace=True)
    user_info.rename(columns={"identifyfront": "identify_front", "identifyback": "identify_back", "facepic": "face_pic"}, inplace=True)

    '''编码匹配'''
    df_add_list = []
    crm_user_province_df = user_info.loc[user_info['province'].notna(), ['unionid', 'province']]
    crm_user_city_df = user_info.loc[user_info['city'].notna(), ['unionid', 'city']]
    crm_user_region_df = user_info.loc[user_info['region'].notna(), ['unionid', 'region']]
    crm_user_town_df = user_info.loc[user_info['town'].notna(), ['unionid', 'town']]
    user_info.drop(['province', 'city', 'region', 'town'], axis=1, inplace=True)
    df_add_list.append(user_info)

    # 匹配省
    # 省份编码
    province_sql = '''
        select code, name from lh_analyze.province
    '''
    province_df = pd.read_sql(province_sql, conn_analyze)
    province_df['province'] = None
    # 匹配省编码
    for index, rows in crm_user_province_df.iterrows():
        province_df.loc[province_df['name'].str.contains(rows['province']), 'province'] = rows['province']
    crm_user_province_df = crm_user_province_df.merge(province_df, how='left', on='province')
    crm_user_province_df.drop(['province', 'name'], axis=1, inplace=True)
    crm_user_province_df.rename(columns={"code": "province_code"}, inplace=True)
    df_add_list.append(crm_user_province_df)

    # 匹配市
    city_sql = '''
        select code, name from lh_analyze.city
    '''
    city_df = pd.read_sql(city_sql, conn_analyze)
    # 直辖市名修改
    city_df.loc[city_df['code'] == '110100000000', 'name'] = '北京市'
    city_df.loc[city_df['code'] == '120100000000', 'name'] = '天津市'
    city_df.loc[city_df['code'] == '310100000000', 'name'] = '上海市'
    city_df.loc[city_df['code'] == '500100000000', 'name'] = '重庆市'
    city_df.loc[city_df['code'] == '429000000000', 'name'] = '潜江市'
    city_df['city'] = None
    for index, rows in crm_user_city_df.iterrows():
        city_df.loc[city_df['name'].str.contains('^%s.*' % rows['city']), 'city'] = rows['city']
    crm_user_city_df = crm_user_city_df.merge(city_df, how='left', on='city')
    crm_user_city_df.drop(['city', 'name'], axis=1, inplace=True)
    crm_user_city_df.rename(columns={"code": "city_code"}, inplace=True)
    df_add_list.append(crm_user_city_df)

    # 匹配区
    region_sql = '''
        select city_code, code, name from lh_analyze.region
    '''
    region_df = pd.read_sql(region_sql, conn_analyze)
    crm_user_region_df = crm_user_region_df.merge(crm_user_city_df.loc[:, ['unionid', 'city_code']], how='left', on='unionid')
    region_df['region'] = None
    for index, rows in crm_user_region_df.iterrows():
        region_df.loc[region_df['name'].str.contains('^%s.*' % rows['region']), 'region'] = rows['region']
    crm_user_region_df = crm_user_region_df.merge(region_df, how='left', on=['city_code', 'region'])
    crm_user_region_df.drop(['region', 'city_code', 'name'], axis=1, inplace=True)
    crm_user_region_df.rename(columns={"code": "region_code"}, inplace=True)
    df_add_list.append(crm_user_region_df)

    # 匹配镇
    # 镇编码
    town_sql = '''
        select region_code, code, name from lh_analyze.town
    '''
    town_df = pd.read_sql(town_sql, conn_analyze)
    crm_user_town_df = crm_user_town_df.merge(crm_user_region_df.loc[:, ['unionid', 'region_code']], how='left', on='unionid')
    town_df['town'] = None
    for index, rows in crm_user_town_df.iterrows():
        town_df.loc[town_df['name'] == rows['town'], 'town'] = rows['town']
    crm_user_town_df = crm_user_town_df.merge(town_df, how='left', on=['region_code', 'town'])
    crm_user_town_df.drop(['town', 'region_code', 'name'], axis=1, inplace=True)
    crm_user_town_df.rename(columns={"code": "town_code"}, inplace=True)
    df_add_list.append(crm_user_town_df)

    user_info = reduce(lambda left, right: pd.merge(left, right, on=['unionid'], how='left'), df_add_list)

    conn_add = pd_conn(analyze_mysql_conf)
    logger.info('数据准备入库')
    user_info.to_sql('crm_user_info', conn_add, index=False, if_exists="append")
    logger.info('入库完成')
except:
    logger.info(traceback.format_exc())
finally:
    try:
        conn_analyze.close()
        conn_crm.close()
    except:
        pass
end_time = time.time()
logger.info(end_time - start_time)

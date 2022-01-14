# -*- coding: utf-8 -*-
# @Time : 2021/12/28  14:13
# @Author : shihong
# @File : .py
# --------------------------------------
# 同步crm数据库新增用户与crm_user_info数据
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

# 纯pandas的入库

# 转时间函数 转不成功赋值 最后在清洗处理
def str_to_date(x):
    try:
        if x:
           return pd.to_datetime(x).strftime('%Y-%m-%d')
        else:
            pass
    except:
       return "error_birth"

def int_time_to_str(x):
    try:
        if x:
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x)))
            return time_str
        else:
            return ""
    except:
        return ""

# 当前地址分割
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

def get_user_operationcenter_direct(crm_user_data=""):
    '''
    crm_user_data 必须是dataframe
    :param crm_cursor: crm数据库游标。需再调用方法后手动关闭数据库连接
    :return: crm手机不为空的用户对应运营中心
    '''
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        if not conn_crm:
            return False, '数据库连接失败'
        crm_cursor = conn_crm.cursor()
        operate_sql = 'select id operate_direct_id,unionid, name direct_leader, telephone, operatename operatenamedirect from luke_lukebus.operationcenter where status=1'
        crm_cursor.execute(operate_sql)
        operate_data = crm_cursor.fetchall()
        operate_df = pd.DataFrame(operate_data)

        # crm用户数据
        crm_user_df = ""
        if len(crm_user_data) > 0:
            crm_user_df = crm_user_data
        else:
            crm_user_sql = 'select id unionid, pid parentd, phone from luke_sincerechat.user where phone is not null'
            crm_cursor.execute(crm_user_sql)
            crm_user_df = pd.DataFrame(crm_cursor.fetchall())

        # 运营中心手机列表
        operate_telephone_list = operate_df['telephone'].to_list()

        # 关系查找ql
        supervisor_sql = '''
                select a.*,b.operatename operatenamedirect,b.crm from 
                (WITH RECURSIVE temp as (
                    SELECT t.id,t.pid,t.phone,t.nickname,t.name FROM luke_sincerechat.user t WHERE phone = %s
                    UNION ALL
                    SELECT t1.id,t1.pid,t1.phone, t1.nickname,t1.name FROM luke_sincerechat.user t1 INNER JOIN temp ON t1.pid = temp.id
                )
                SELECT * FROM temp
                )a left join luke_lukebus.operationcenter b
                on a.id = b.unionid
                '''
        child_df_list = []
        for phone in operate_telephone_list:
            # 1、获取运营中心所有下级数据
            crm_cursor.execute(supervisor_sql, phone)
            all_data = crm_cursor.fetchall()
            # 总数据
            all_data = pd.DataFrame(all_data)
            all_data.dropna(subset=['phone'], axis=0, inplace=True)
            all_data_phone = all_data['phone'].tolist()
            # 运营中心名称
            operatename = operate_df.loc[operate_df['telephone'] == phone, 'operatenamedirect'].values[0]
            operate_direct_id = operate_df.loc[operate_df['telephone'] == phone, 'operate_direct_id'].values[0]
            direct_bus_phone = operate_df.loc[operate_df['telephone'] == phone, 'telephone'].values[0]
            direct_leader = operate_df.loc[operate_df['telephone'] == phone, 'direct_leader'].values[0]
            direct_leader_unionid = operate_df.loc[operate_df['telephone'] == phone, 'unionid'].values[0]

            # 子运营中心-->包含本身
            center_phone_list = all_data.loc[all_data['operatenamedirect'].notna(), :]['phone'].tolist()
            child_center_phone_list = []  # 子运营中心所有下级
            # 2、得到运营中心下所有归属下级
            first_child_center = []  # 第一级运营中心
            for i in center_phone_list[1:]:
                # 剔除下级的下级运营中心
                if i in child_center_phone_list:
                    continue
                # 排除运营中心重复统计
                #         if i not in center_phone_list:
                #         first_child_center.append(i)
                crm_cursor.execute(supervisor_sql, i)
                center_data = crm_cursor.fetchall()
                center_df = pd.DataFrame(center_data)
                center_df.dropna(subset=['phone'], axis=0, inplace=True)
                child_center_phone_list.extend(center_df['phone'].tolist())
            ret = list(set(all_data_phone) - set(child_center_phone_list))
            #     ret.extend(first_child_center)
            # 3、取得每个运营中心下级df合并
            child_df = crm_user_df.loc[crm_user_df['phone'].isin(ret), :]
            child_df['operatenamedirect'] = operatename
            child_df["operate_direct_id"] = operate_direct_id
            child_df["direct_bus_phone"] = direct_bus_phone
            child_df["direct_leader"] = direct_leader
            child_df["direct_leader_unionid"] = direct_leader_unionid
            child_df_list.append(child_df)
        # 用户数据拼接
        exist_center_df = pd.concat(child_df_list)
        fina_df = crm_user_df.merge(exist_center_df.loc[:, ['phone', 'operatenamedirect','operate_direct_id','direct_bus_phone','direct_leader','direct_leader_unionid']], how='left', on='phone')
        conn_crm.close()
        logger.info('返回用户数据成功')
        return True, fina_df
    except Exception as e:
        logger.exception(traceback.format_exc())
        return False, "10000"

# crm_user表更新
def crm_new_user_fun():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        analyze_cursor = conn_analyze.cursor()
        # 查找crm库unionid
        crm_unionid_sql = '''select id unionid from luke_sincerechat.user'''
        # crm_user表unionid
        crm_user_unionid_sql = '''select unionid from lh_analyze.crm_user'''
        crm_uninonid_df = pd.read_sql(crm_unionid_sql, conn_crm)
        crm_user_unionid_df = pd.read_sql(crm_user_unionid_sql, conn_analyze)
        # unionid差集
        sub_unionid_list = list(set(crm_uninonid_df['unionid'].tolist()) - set(crm_user_unionid_df['unionid'].tolist()))
        # 如果新增
        if sub_unionid_list:
            logger.info('新增数量：%s' %len(sub_unionid_list))
            sql = '''
            select a.id unionid,a.pid parentid,a.phone,b.phone parent_phone,b.nickname parent_nickname,b.name parent_name,a.status,a.nickname,a.name,a.sex,FROM_UNIXTIME(a.addtime,"%Y-%m-%d %H:%i:%s") addtime,
            c.phone bus_parent_phone,c.u_nickname bus_parent_nickname,c.u_name bus_parent_name,c.unionid bus_parentid
            from luke_sincerechat.user a
            left join luke_sincerechat.user b on a.pid = b.id
            left join luke_lukebus.user c on a.pid = c.unionid
            where a.id in ({})
            '''.format(','.join(str(unionid) for unionid in sub_unionid_list))
            logger.info(sql)
            crm_datas = pd.read_sql(sql,conn_crm)
            logger.info(crm_datas.shape)

            #查询禄可商务推荐的上级和手机号码
            sql = '''select unionid,status vertify_status,address,birth,nationality from luke_crm.authentication'''
            auth_datas = pd.read_sql(sql,conn_crm)
            crm_datas = crm_datas.merge(auth_datas, how="left", on="unionid")

            #活体认证
            sql = '''select unionid,status huoti_status from authentication_livingthing'''
            huoti_data = pd.read_sql(sql,conn_crm)
            crm_datas = crm_datas.merge(huoti_data,how="left",on="unionid")

            # conn = direct_get_conn(crm_mysql_conf)
            vip_datas = ""
            serpro_datas = ""
            # pandas read_sql 不支持sql时间戳格式化FROM_UNIXTIME 所以这里改用sql查询方式 然后进行pandas 的动态拼接
            with conn_crm.cursor() as cursor:
                # sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from luke_crm.user_vip'''
                sql = '''select unionid,grade vip_grade,starttime vip_starttime,endtime vip_endtime from luke_crm.user_vip'''
                cursor.execute(sql)
                vip_datas = pd.DataFrame(cursor.fetchall())

                sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from luke_crm.user_serpro'''
                cursor.execute(sql)
                serpro_datas = pd.DataFrame(cursor.fetchall())


            vip_datas["vip_starttime"] = vip_datas['vip_starttime'].apply(lambda x: int_time_to_str(x))
            vip_datas["vip_endtime"] = vip_datas['vip_endtime'].apply(lambda x: int_time_to_str(x))
            crm_datas = crm_datas.merge(vip_datas, how="left", on="unionid")
            crm_datas = crm_datas.merge(serpro_datas, how="left", on="unionid")

            logger.info(crm_datas.shape)
            logger.info(crm_datas.describe)

            #查询运营中心的身份
            sql = '''select phone,capacity,exclusive from luke_lukebus.user'''
            operate_data = pd.read_sql(sql,conn_crm)
            crm_datas = crm_datas.merge(operate_data,how="left",on="phone")

            #查对应的运营中心
            user_data_result = get_all_user_operationcenter(crm_datas)

            last_data = user_data_result[1]

            user_data_result = get_user_operationcenter_direct(last_data)
            logger.info(user_data_result)
            last_data = user_data_result[1]
            #把数字的要转成str 不然不能替换
            last_data["vertify_status"] = last_data["vertify_status"].astype("object")
            last_data["vip_grade"] = last_data["vip_grade"].astype("object")
            last_data["serpro_grade"] = last_data["serpro_grade"].astype("object")
            last_data["serpro_status"] = last_data["serpro_status"].astype("object")
            last_data["exclusive"] = last_data["exclusive"].astype("object")
            last_data["capacity"] = last_data["capacity"].astype("object")

            last_data = last_data.where(last_data.notnull(), None)
            last_data["birth"] = last_data["birth"].replace("",None)
            last_data["birth"] = last_data['birth'].apply(lambda x: str_to_date(x))
            last_data.drop(last_data[last_data["birth"]=="error_birth"].index,inplace=True,axis=0)

            last_data["statistic_time"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.info('可入库的新增用户数：%s' % last_data.shape[0])

            # 第一次入库走这个
            logger.info("数据准备入库")
            logger.info(last_data['unionid'].tolist())
            conn = sqlalchemy_conn(analyze_mysql_conf)
            last_data.to_sql("crm_user", con=conn, if_exists="append", index=False)
            logger.info("写入成功")
        else:
            logger.info('无新增的用户数据')
    except:
        logger.info(traceback.format_exc())
    finally:
        conn_analyze.close()
        conn_crm.close()

# crm_user_info表更新
def crm_new_user_info_fun():
    try:
        base_url = 'https://luke-sincerechat.oss-cn-beijing.aliyuncs.com'
        logger.info('更新用户信息数据')
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        # 查找crm库unionid
        crm_unionid_sql = '''select id unionid from luke_sincerechat.user'''
        # crm_user表unionid
        crm_user_unionid_sql = '''select unionid from lh_analyze.crm_user_info'''
        crm_uninonid_df = pd.read_sql(crm_unionid_sql, conn_crm)
        crm_user_info_unionid_df = pd.read_sql(crm_user_unionid_sql, conn_analyze)
        # unionid差集
        sub_unionid_list = list(set(crm_uninonid_df['unionid'].tolist()) - set(crm_user_info_unionid_df['unionid'].tolist()))
        logger.info('新增用户数：%s' % len(sub_unionid_list))
        if len(sub_unionid_list) == 0:
            return "暂无新增用户"
        # 获取用户图片信息
        # 用户头像
        user_info_sql_1 = '''
            select unionid, head_pic usericon from luke_crm.user_info where head_pic is not null and head_pic!= '' and unionid in (%s)
        ''' % ','.join(str(unionid) for unionid in sub_unionid_list)
        # 用户身份证、身份证正反面
        user_info_sql_2 = '''
            select unionid, identity, pic_just identifyfront, pic_back identifyback, issue, address from luke_crm.authentication where unionid in (%s)
        ''' % ','.join(str(unionid) for unionid in sub_unionid_list)
        # 人脸照片
        user_info_sql_3 = '''
            select unionid, pic_head facepic from luke_crm.authentication_livingthing where unionid in (%s)
        ''' % ','.join(str(unionid) for unionid in sub_unionid_list)
        # 用户所在地
        user_area_sql = '''
            select id unionid, area area_id from luke_sincerechat.user where id in (%s)
        ''' % ','.join(str(unionid) for unionid in sub_unionid_list)


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

        user_info = reduce(lambda left, right: pd.merge(left, right, on=['unionid'], how='left'), df_list)

        user_info.drop('area_id', axis=1, inplace=True)
        user_info.rename(
            columns={"identifyfront": "identify_front", "identifyback": "identify_back", "facepic": "face_pic"},
            inplace=True)

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
            city_df.loc[city_df['name'].str.contains(rows['city']), 'city'] = rows['city']
        crm_user_city_df = crm_user_city_df.merge(city_df, how='left', on='city')
        crm_user_city_df.drop(['city', 'name'], axis=1, inplace=True)
        crm_user_city_df.rename(columns={"code": "city_code"}, inplace=True)
        df_add_list.append(crm_user_city_df)

        # 匹配区
        region_sql = '''
            select city_code, code, name from lh_analyze.region
        '''
        region_df = pd.read_sql(region_sql, conn_analyze)
        crm_user_region_df = crm_user_region_df.merge(crm_user_city_df.loc[:, ['unionid', 'city_code']], how='left',
                                                      on='unionid')
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
        crm_user_town_df = crm_user_town_df.merge(crm_user_region_df.loc[:, ['unionid', 'region_code']], how='left',
                                                  on='unionid')
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
        logger.info('同步unionid: ')
        logger.info(user_info['unionid'].tolist())
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
if __name__ == '__main__':

    start_time = int(time.time())
    crm_new_user_fun()
    crm_new_user_info_fun()
    end_time = int(time.time())
    logger.info(end_time-start_time)
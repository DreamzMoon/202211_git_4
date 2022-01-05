# -*- coding: utf-8 -*-
# @Time : 2021/12/28  14:13
# @Author : shihong
# @File : .py
# --------------------------------------
# 同步crm数据库新增用户
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


start_time = int(time.time())
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

end_time = int(time.time())
logger.info(end_time-start_time)
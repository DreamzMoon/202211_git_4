# -*- coding: utf-8 -*-
# @Time : 2021/12/3  16:29
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

def get_operate_relationship(user_df, mode):
    conn = direct_get_conn(crm_mysql_conf)
    if not conn:
        return False, '数据连接失败'
    cursor = conn.cursor()

    if mode == 'crm':
        # 运营中心
        operate_sql = 'select id, unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1 and crm=1'
        cursor.execute(operate_sql)
        operate_df = pd.DataFrame(cursor.fetchall())
        supervisor_sql = '''
            select a.*, if (crm =0,Null,b.operatename) operatename, b.crm from 
            (WITH RECURSIVE temp as (
                SELECT t.id,t.pid,t.phone,t.nickname,t.name FROM luke_sincerechat.user t WHERE phone = %s
                UNION ALL
                SELECT t1.id,t1.pid,t1.phone, t1.nickname,t1.name FROM luke_sincerechat.user t1 INNER JOIN temp ON t1.pid = temp.id
            )
            SELECT * FROM temp
            )a left join luke_lukebus.operationcenter b
            on a.id = b.unionid
        '''
        change_columns = ['phone', 'leader', 'operatename', 'operate_id', 'bus_phone']
    else:
        # 运营中心
        operate_sql = 'select id, unionid, name, telephone, operatename from luke_lukebus.operationcenter where capacity=1'
        cursor.execute(operate_sql)
        operate_df = pd.DataFrame(cursor.fetchall())
        supervisor_sql = '''
            select a.*,b.operatename operatename,b.crm from 
            (WITH RECURSIVE temp as (
                SELECT t.id,t.pid,t.phone,t.nickname,t.name FROM luke_sincerechat.user t WHERE phone = %s
                UNION ALL
                SELECT t1.id,t1.pid,t1.phone, t1.nickname,t1.name FROM luke_sincerechat.user t1 INNER JOIN temp ON t1.pid = temp.id
            )
            SELECT * FROM temp
            )a left join luke_lukebus.operationcenter b
            on a.id = b.unionid
        '''
        change_columns = ['phone', 'direct_bus_leader', 'operatenamedirect', 'operate_direct_id', 'direct_bus_phone']
    operate_telephone_list = operate_df['telephone'].to_list()
    child_df_list = []
    for phone in operate_telephone_list:
        # 1、获取运营中心所有下级数据
        cursor.execute(supervisor_sql, phone)
        all_data = cursor.fetchall()
        # 总数据
        all_data = pd.DataFrame(all_data)
        all_data.dropna(subset=['phone'], axis=0, inplace=True)
        all_data_phone = all_data['phone'].tolist()
        # 运营中心名称
        operatename = operate_df.loc[operate_df['telephone'] == phone, 'operatename'].values[0]
        # 运营中心id
        operateid = operate_df.loc[operate_df['telephone'] == phone, 'id'].values[0]
        # 运营中心手机号
        phone = operate_df.loc[operate_df['telephone'] == phone, 'telephone'].values[0]
        # 运营中心负责人
        name = operate_df.loc[operate_df['telephone'] == phone, 'name'].values[0]
        # 子运营中心-->包含本身
        center_phone_list = all_data.loc[all_data['operatename'].notna(), :]['phone'].tolist()
        child_center_phone_list = []  # 子运营中心所有下级
        # 2、得到运营中心下所有归属下级
        first_child_center = []  # 第一级运营中心
        for i in center_phone_list[1:]:
            # 剔除下级的下级运营中心
            if i in child_center_phone_list:
                continue
            cursor.execute(supervisor_sql, i)
            center_data = cursor.fetchall()
            center_df = pd.DataFrame(center_data)
            center_df.dropna(subset=['phone'], axis=0, inplace=True)
            child_center_phone_list.extend(center_df['phone'].tolist())
        ret = list(set(all_data_phone) - set(child_center_phone_list))
        #     ret.extend(first_child_center)
        # 3、取得每个运营中心下级df合并
        child_df = user_df.loc[user_df['phone'].isin(ret), :]
        child_df['operatename'] = operatename
        child_df["operatid"] = operateid
        child_df["operate_phone"] = phone
        child_df['name'] = name
        child_df_list.append(child_df)
    # 用户数据拼接
    exist_center_df = pd.concat(child_df_list)
    exist_center_df = exist_center_df.loc[:, ['phone', 'name', 'operatename', 'operatid', 'operate_phone']]
    exist_center_df.columns = change_columns
    fina_df = user_df.merge(exist_center_df, how='left', on='phone')
    conn.close()
    return True, fina_df

def get_crm_bus_data(user_df):
    conn = direct_get_conn(crm_mysql_conf)
    if not conn:
        return False, '数据连接失败'
    cursor = conn.cursor()

    # 认证
    sql = '''select unionid,status vertify_status,address,birth,nationality from luke_crm.authentication'''
    auth_datas = pd.read_sql(sql, conn)
    crm_datas = user_df.merge(auth_datas, how="left", on="unionid")

    # 活体认证
    sql = '''select unionid,status huoti_status from authentication_livingthing'''
    huoti_data = pd.read_sql(sql, conn)
    crm_datas = crm_datas.merge(huoti_data, how="left", on="unionid")

    vip_datas = ""
    serpro_datas = ""
    # pandas read_sql 不支持sql时间戳格式化FROM_UNIXTIME 所以这里改用sql查询方式 然后进行pandas 的动态拼接
        # vip信息
    sql = '''select unionid,grade vip_grade,starttime vip_starttime,endtime vip_endtime from luke_crm.user_vip'''
    cursor.execute(sql)
    vip_datas = pd.DataFrame(cursor.fetchall())

        # 服务商
    sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from luke_crm.user_serpro'''
    cursor.execute(sql)
    serpro_datas = pd.DataFrame(cursor.fetchall())

    vip_datas["vip_starttime"] = vip_datas['vip_starttime'].apply(lambda x: int_time_to_str(x))
    vip_datas["vip_endtime"] = vip_datas['vip_endtime'].apply(lambda x: int_time_to_str(x))
    crm_datas = crm_datas.merge(vip_datas, how="left", on="unionid")
    crm_datas = crm_datas.merge(serpro_datas, how="left", on="unionid")

    # 禄可商务
    sql = '''select phone, capacity, exclusive from luke_lukebus.user'''
    operate_data = pd.read_sql(sql, conn)
    last_data = crm_datas.merge(operate_data, how="left", on="phone")

    # 把数字的要转成str 不然不能替换
    last_data["vertify_status"] = last_data["vertify_status"].astype("object")
    last_data["vip_grade"] = last_data["vip_grade"].astype("object")
    last_data["serpro_grade"] = last_data["serpro_grade"].astype("object")
    last_data["serpro_status"] = last_data["serpro_status"].astype("object")
    last_data["exclusive"] = last_data["exclusive"].astype("object")
    last_data["capacity"] = last_data["capacity"].astype("object")

    last_data = last_data.where(last_data.notnull(), None)
    last_data["birth"] = last_data["birth"].replace("", None)
    last_data["birth"] = last_data['birth'].apply(lambda x: str_to_date(x))
    last_data.drop(last_data[last_data["birth"] == "error_birth"].index, inplace=True, axis=0)

    last_data["statistic_time"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return True, last_data

def create_table():
    conn_analyze = direct_get_conn(analyze_mysql_conf)
    analyze_cursor = conn_analyze.cursor()
    create_sql = '''
    CREATE TABLE `lh_user_%s` (
        `unionid` bigint(20) DEFAULT NULL COMMENT '用户id',
        `parentid` bigint(20) DEFAULT NULL COMMENT '推荐id',
        `lh_id` varchar(32) DEFAULT NULL COMMENT '靓号用户id',
        `phone` varchar(20) DEFAULT NULL COMMENT '手机号码',
        `parent_phone` varchar(20) DEFAULT NULL COMMENT '上级手机号码',
        `parent_nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '昵称',
        `parent_name` varchar(50) DEFAULT NULL COMMENT '姓名',
        `bus_parent_phone` varchar(20) DEFAULT NULL COMMENT '禄可商务的上级手机号码',
        `bus_parent_nickname` varchar(100) DEFAULT NULL COMMENT '禄可商务的上级昵称',
        `bus_parent_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '禄可商务的上级名字',
        `bus_parentid` bigint(20) DEFAULT NULL COMMENT '禄可商务的推荐id',
        `status` tinyint(1) DEFAULT '1' COMMENT '1正常 2禁用',
        `enable_status` tinyint(1) DEFAULT '1' COMMENT '启用状态 0否 1是',
        `nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '昵称',
        `name` varchar(50) DEFAULT NULL COMMENT '姓名',
        `sex` tinyint(1) DEFAULT '0' COMMENT '0：位置   1：男  2：女',
        `birth` date DEFAULT NULL COMMENT '生日',
        `address` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT '地址',
        `nationality` varchar(50) DEFAULT NULL COMMENT '族',
        `vertify_status` tinyint(1) DEFAULT NULL COMMENT '0待认证 1待审核 2认证中 3失败 4成功',
        `huoti_status` tinyint(1) DEFAULT NULL COMMENT '活体认证状态：0待认证 1待审核 2认证中 3失败 4成功',
        `vip_grade` tinyint(1) DEFAULT NULL COMMENT '等级 1普通会员 2vip会员 3至尊VIP',
        `vip_starttime` datetime DEFAULT NULL COMMENT 'vip开始时间',
        `vip_endtime` datetime DEFAULT NULL COMMENT 'vip结束时间',
        `serpro_grade` tinyint(1) DEFAULT NULL COMMENT '服务商等级：1初级 2中级 3高级 4机构服务商',
        `serpro_status` tinyint(1) DEFAULT NULL COMMENT '服务视状态：1正常  0已注销',
        `serpro_starttime` datetime DEFAULT NULL COMMENT '服务商开通时间',
        `user_type` tinyint(1) DEFAULT '0' COMMENT '0:用户 1：开发 2：运营 3：测试',
        `capacity` tinyint(2) DEFAULT NULL COMMENT '1运营中心/公司 2网店主 3带货者 20无身份(普通用户)',
        `exclusive` tinyint(1) DEFAULT NULL COMMENT '网店主类型：1专营店 2自营店',
        `addtime` datetime DEFAULT NULL COMMENT '用户的注册时间 可能在各业务系统',
        `operate_id` bigint(20) DEFAULT NULL COMMENT 'crm运营中心的id',
        `operatename` varchar(100) DEFAULT NULL COMMENT 'crm用户对应的运营中心',
        `leader` varchar(100) DEFAULT NULL COMMENT 'crm运营中心负责人',
        `bus_phone` varchar(20) DEFAULT NULL COMMENT 'crm禄可商务的手机号码',
        `operate_direct_id` bigint(20) DEFAULT NULL COMMENT '运营中心的id',
        `operatenamedirect` varchar(100) DEFAULT NULL COMMENT '用户对应的运营中心',
        `direct_bus_leader` varchar(100) DEFAULT NULL COMMENT 'crm运营中心负责人',
        `direct_bus_phone` varchar(20) DEFAULT NULL COMMENT '禄可商务的手机号码',
        `is_buy_from_official` tinyint(1) DEFAULT '1' COMMENT '是否能从官方购买',
        `is_buy_from_market` tinyint(1) DEFAULT '1' COMMENT '是否能从市场购买',
        `is_sell_from_market` tinyint(1) DEFAULT '1' COMMENT '是否能从市场出售',
        `is_buy_from_retail_market` tinyint(1) DEFAULT '1' COMMENT '是否能从零售市场购买',
        `is_sell_from_retail_market` tinyint(1) DEFAULT '1' COMMENT '是否能从零售市场出售',
        `vip_domain_count` int(11) DEFAULT '0' COMMENT 'vip域名购买数量',
        `is_special` tinyint(1) DEFAULT '0' COMMENT '是否特殊用户 0否1是 不受发布次数限制',
        `statistic_time` datetime DEFAULT NULL COMMENT '统计时间',
        `del_flag` int(1) DEFAULT '0' COMMENT '1：删除'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    ''' % tomorrow_time
    analyze_cursor.execute(create_sql)
    conn_analyze.commit()
    logger.info("建表成功")

start_time = int(time.time())
try:
    # 读取靓号用户数据
    conn_read_lh = direct_get_conn(lianghao_mysql_conf)

    if not conn_read_lh:
        print('连接错误')

    lh_user_sql = '''
        select phone, id lh_id, `status` enable_status, is_buy_from_official, is_buy_from_market, is_sell_from_market, is_buy_from_retail_market, is_sell_from_retail_market, vip_count vip_domain_count, is_special
        from lh_pretty_client.lh_user
        where (phone is not null or phone != "")
        and del_flag=0
    '''
    lh_user_df = pd.read_sql(lh_user_sql, conn_read_lh)
    lh_user_df.drop_duplicates('phone', inplace=True)
    lh_user_df = lh_user_df[lh_user_df['phone'] != '']
    conn_read_lh.close()

    # 获取crm用户关系（上下级）
    conn_crm = direct_get_conn(crm_mysql_conf)
    if not conn_crm:
        print('数据库连接失败')
    crm_cursor = conn_crm.cursor()

    crm_user_sql = sql = '''
        select a.id unionid,a.pid parentid,a.phone,b.phone parent_phone,b.nickname parent_nickname,b.name parent_name,a.status,a.nickname,a.name,a.sex,FROM_UNIXTIME(a.addtime,"%Y-%m-%d %H:%i:%s") addtime,
        c.phone bus_parent_phone,c.u_nickname bus_parent_nickname,c.u_name bus_parent_name,c.unionid bus_parentid
        from luke_sincerechat.user a
        left join luke_sincerechat.user b on a.pid = b.id
        left join luke_lukebus.user c on a.pid = c.unionid
        where a.phone is not null or a.phone != ''
    '''
    crm_cursor.execute(crm_user_sql)
    crm_user_df = pd.DataFrame(crm_cursor.fetchall())
    conn_crm.close()

    # 靓号用户与crm用户关系合并
    user_merge = lh_user_df.merge(crm_user_df, how='left', on='phone')

    # 禄可商务
    bus_result = get_operate_relationship(user_merge, 'bus')
    if bus_result[0]:
    # crm与禄可商务数据合并
        fina_result = get_operate_relationship(bus_result[1], 'crm')
        if fina_result[0]:
            last_data = get_crm_bus_data(fina_result[1])
            # 创建表格
            create_table()
            # 数据入库
            conn = pd_conn(analyze_mysql_conf)
            last_data[1].to_sql('lh_user_%s' % tomorrow_time, con=conn, if_exists="append", index=False)
            logger.info('导入成功')
        else:
            logger.info('导入失败')
    else:
        logger.info('导入失败')
except:
    logger.error(traceback.format_exc())
    logger.info('导入失败')


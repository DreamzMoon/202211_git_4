# -*- coding: utf-8 -*-

# @Time : 2021/11/19 14:45

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : crm_user.py
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
        operate_sql = 'select id operate_direct_id,unionid, name direct_leader, telephone, operatename operatenamedirect from luke_lukebus.operationcenter'
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
    logger.info(conn_analyze)
    # sql = '''select a.id unionid,a.pid parentid,a.phone,b.phone parent_phone,b.nickname parent_nickname,b.name parent_name,a.status,a.nickname,a.name,a.sex,FROM_UNIXTIME(a.addtime,"%Y-%m-%d %H:%i:%s") addtime from luke_sincerechat.user a
    # left join luke_sincerechat.user b on a.pid = b.id'''
    sql = '''
    select a.id unionid,a.pid parentid,a.phone,b.phone parent_phone,b.nickname parent_nickname,b.name parent_name,a.status,a.nickname,a.name,a.sex,FROM_UNIXTIME(a.addtime,"%Y-%m-%d %H:%i:%s") addtime,
    c.phone bus_parent_phone,c.u_nickname bus_parent_nickname,c.u_name bus_parent_name,c.unionid bus_parentid
    from luke_sincerechat.user a
    left join luke_sincerechat.user b on a.pid = b.id
    left join luke_lukebus.user c on a.pid = c.unionid
    '''
    logger.info(sql)
    crm_datas = pd.read_sql(sql,conn_crm)
    logger.info(crm_datas.shape)

    #查询禄可商务推荐的上级和手机号码
    sql = ''''''

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

    # last_data = last_data.to_dict("records")
    # 准备入库


    # logger.info(tomorrow_time)
    # #建表 以明天的时间建表
    # create_sql = '''
    #    CREATE TABLE `crm_user_%s` (
    #   `unionid` bigint(20) DEFAULT NULL COMMENT '用户id',
    #   `parentid` bigint(20) DEFAULT NULL COMMENT '推荐id',
    #   `phone` varchar(20) DEFAULT NULL COMMENT '手机号码',
    #   `parent_phone` varchar(20) DEFAULT NULL COMMENT '上级手机号码',
    #   `parent_nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '昵称',
    #   `parent_name` varchar(50) DEFAULT NULL COMMENT '姓名',
    #   `bus_parent_phone` varchar(20) DEFAULT NULL COMMENT '禄可商务的上级手机号码',
    #   `bus_parent_nickname` varchar(100) DEFAULT NULL COMMENT '禄可商务的上级昵称',
    #   `bus_parent_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '禄可商务的上级名字',
    #   `bus_parentid` bigint(20) DEFAULT NULL COMMENT '禄可商务的推荐id',
    #   `status` tinyint(1) DEFAULT '1' COMMENT '1正常 2禁用',
    #   `nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '昵称',
    #   `name` varchar(50) DEFAULT NULL COMMENT '姓名',
    #   `sex` tinyint(1) DEFAULT '0' COMMENT '0：位置   1：男  2：女',
    #   `birth` date DEFAULT NULL COMMENT '生日',
    #   `address` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci COMMENT '地址',
    #   `nationality` varchar(50) DEFAULT NULL COMMENT '族',
    #   `vertify_status` tinyint(1) DEFAULT NULL COMMENT '0待认证 1待审核 2认证中 3失败 4成功',
    #   `huoti_status` tinyint(1) DEFAULT NULL COMMENT '活体认证状态：0待认证 1待审核 2认证中 3失败 4成功',
    #   `vip_grade` tinyint(1) DEFAULT NULL COMMENT '等级 1普通会员 2vip会员 3至尊VIP',
    #   `vip_starttime` datetime DEFAULT NULL COMMENT 'vip开始时间',
    #   `vip_endtime` datetime DEFAULT NULL COMMENT 'vip结束时间',
    #   `serpro_grade` tinyint(1) DEFAULT NULL COMMENT '服务商等级：1初级 2中级 3高级 4机构服务商',
    #   `serpro_status` tinyint(1) DEFAULT NULL COMMENT '服务视状态：1正常  0已注销',
    #   `serpro_starttime` datetime DEFAULT NULL COMMENT '服务商开通时间',
    #   `user_type` tinyint(1) DEFAULT '0' COMMENT '0:用户 1：开发 2：运营 3：测试',
    #   `capacity` tinyint(2) DEFAULT NULL COMMENT '1运营中心/公司 2网店主 3带货者 20无身份(普通用户)',
    #   `exclusive` tinyint(1) DEFAULT NULL COMMENT '网店主类型：1专营店 2自营店',
    #   `addtime` datetime DEFAULT NULL COMMENT '用户的注册时间 可能在各业务系统',
    #   `operate_id` bigint(20) DEFAULT NULL COMMENT 'crm运营中心的id',
    #   `operatename` varchar(100) DEFAULT NULL COMMENT 'crm用户对应的运营中心',
    #   `bus_phone` varchar(20) DEFAULT NULL COMMENT 'crm禄可商务的手机号码',
    #   `operate_direct_id` bigint(20) DEFAULT NULL COMMENT '运营中心的id',
    #   `operatenamedirect` varchar(100) DEFAULT NULL COMMENT '用户对应的运营中心',
    #   `direct_bus_phone` varchar(20) DEFAULT NULL COMMENT '禄可商务的手机号码',
    #   `direct_leader` varchar(20) DEFAULT NULL COMMENT '禄可商务对应的负责人',
    #   `leader` varchar(20) DEFAULT NULL COMMENT 'crm禄可商务对应的负责人',
    #   `direct_leader_unionid` bigint(20) DEFAULT NULL COMMENT '禄可商务运营中心负责人的id',
    #   `leader_unionid` bigint(20) DEFAULT NULL COMMENT 'crm运营中心负责人的id',
    #   `statistic_time` datetime DEFAULT NULL COMMENT '统计时间',
    #   `del_flag` int(1) DEFAULT '0' COMMENT '1：删除'
    # ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='crm用户表';
    #     '''  %tomorrow_time
    #
    # analyze_cursor.execute(create_sql)
    # conn_analyze.commit()
    # logger.info("建表成功")
    # logger.info(tomorrow_time)

    # 第一次入库走这个
    logger.info("数据准备入库")
    conn = sqlalchemy_conn(analyze_mysql_conf)
    last_data.to_sql("crm_user", con=conn, if_exists="append", index=False)
    logger.info("写入成功")


    # 删除前天的表 保留昨天的 万一有用
    # try:
    #     delete_sql = '''drop TABLE crm_user_%s''' %yesterday_time
    #     analyze_cursor.execute(delete_sql)
    #     conn_analyze.commit()
    #     logger.info("删除成功")
    # except:
    #     logger.info("删表失败")

    #后续要走批量更新
    #手机号码为空的过滤
    # logger.info(last_data.shape)
    # ok_data = last_data[last_data["phone"].notna()]
    # logger.info(ok_data.shape)
    # ok_data = ok_data.to_dict("records")
    # ok_data_len = len(ok_data)

    # for i,data in enumerate(ok_data):
    #     logger.info("当前到:%s 总数:%s" %(i,ok_data_len))
    #     logger.info(data)
    #     logger.info("------------------------------------")
    #     select_sql = '''select * from crm_user_pd where phone = %s'''
    #     analyze_cursor.execute(select_sql,(data["phone"]))
    #     dd = analyze_cursor.fetchone()
    #     if dd:
    #         update_sql = '''update crm_user_pd set unionid=%s,parentid=%s,status=%s,nickname=%s,name=%s,sex=%s,birth=%s,address=%s,
    #         nationality=%s,vertify_status=%s,vip_grade=%s,vip_starttime=%s,vip_endtime=%s,serpro_grade=%s,serpro_status=%s,
    #         serpro_starttime=%s,capacity=%s,exclusive=%s,addtime=%s,operatename=%s,statistic_time=%s where phone = %s
    #         '''
    #         # logger.info("update_sql:%s" %update_sql)
    #         params = [data["unionid"],data["parentid"],data["status"],data["nickname"],data["name"],data["sex"],data["birth"],data["address"],
    #                   data["nationality"],data["vertify_status"],data["vip_grade"],data["vip_starttime"],data["vip_endtime"],data["serpro_grade"],
    #                   data["serpro_status"],data["serpro_starttime"],data["capacity"],data["exclusive"],data["addtime"],data["operatename"],datetime.datetime.now(),data["phone"]]
    #
    #         analyze_cursor.execute(update_sql,params)
    #         # analyze_cursor.execute(update_sql)
    #         conn_analyze.commit()
    #         logger.info("更新成功")
    #     else:
    #         insert_sql = '''insert into crm_user_pd
    #                 values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s，%s)'''
    #         params = [data["unionid"], data["parentid"], data["phone"], data["status"], data["nickname"], data["name"],
    #                   data["sex"], data["birth"], data["address"], data["nationality"], data["vertify_status"],
    #                   data["vip_grade"], data["vip_starttime"], data["vip_endtime"], data["serpro_grade"],
    #                   data["serpro_status"], data["serpro_starttime"],
    #                   0, data["capacity"], data["exclusive"], data["addtime"], data["operatename"],
    #                   data["statistic_time"],0]
    #         analyze_cursor.execute(insert_sql, params)
    #         conn_analyze.commit()
    #         logger.info("插入成功")

except:
    logger.info(traceback.format_exc())
finally:
    conn_analyze.close()
    conn_crm.close()

end_time = int(time.time())
logger.info(end_time-start_time)

# 耗时 更新和插入

# 以下是速度很慢
#pandas读取 sql增删改查
# try:
#     conn_crm = direct_get_conn(crm_mysql_conf)
#     conn_analyze = direct_get_conn(analyze_mysql_conf)
#     analyze_cursor = conn_analyze.cursor()
#     logger.info(conn_analyze)
#     sql = '''select id unionid,pid parentid,phone,status,nickname,name,sex,FROM_UNIXTIME(addtime,"%Y-%m-%d %H:%i:%s") addtime from luke_sincerechat.user'''
#     logger.info(sql)
#     crm_datas = pd.read_sql(sql,conn_crm)
#     logger.info(crm_datas.shape)
#
#     sql = '''select unionid,status vertify_status,address,birth,nationality from luke_crm.authentication'''
#     auth_datas = pd.read_sql(sql,conn_crm)
#     crm_datas = crm_datas.merge(auth_datas, how="left", on="unionid")
#
#     # conn = direct_get_conn(crm_mysql_conf)
#     vip_datas = ""
#     serpro_datas = ""
#     # pandas read_sql 不支持sql时间戳格式化FROM_UNIXTIME 所以这里改用sql查询方式 然后进行pandas 的动态拼接
#     with conn_crm.cursor() as cursor:
#         sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from luke_crm.user_vip'''
#         cursor.execute(sql)
#         vip_datas = pd.DataFrame(cursor.fetchall())
#
#         sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from luke_crm.user_serpro'''
#         cursor.execute(sql)
#         serpro_datas = pd.DataFrame(cursor.fetchall())
#
#
#
#     crm_datas = crm_datas.merge(vip_datas, how="left", on="unionid")
#     crm_datas = crm_datas.merge(serpro_datas, how="left", on="unionid")
#
#     logger.info(crm_datas.shape)
#     logger.info(crm_datas.describe)
#
#     #查询运营中心的身份
#     sql = '''select phone,capacity,exclusive from luke_lukebus.user'''
#     operate_data = pd.read_sql(sql,conn_crm)
#     crm_datas = crm_datas.merge(operate_data,how="left",on="phone")
#
#     #查对应的运营中心
#     user_data_result = get_all_user_operationcenter(crm_datas)
#
#     last_data = user_data_result[1]
#
#     #把数字的要转成str 不然不能替换
#     last_data["vertify_status"] = last_data["vertify_status"].astype(str)
#     last_data["vip_grade"] = last_data["vip_grade"].astype(str)
#     last_data["serpro_grade"] = last_data["serpro_grade"].astype(str)
#     last_data["serpro_status"] = last_data["serpro_status"].astype(str)
#     last_data["exclusive"] = last_data["exclusive"].astype(str)
#     last_data["capacity"] = last_data["capacity"].astype(str)
#
#
#     logger.info(last_data.info())
#     logger.info(last_data.iloc[0])
#     last_data = last_data.where(last_data.notnull(), None)
#     last_data["birth"] = last_data["birth"].replace("",None)
#     logger.info(last_data.iloc[0])
#
#     last_data["statistic_time"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     last_data = last_data.to_dict("records")
#     # 准备入库
#     for data in last_data:
#         data["vertify_status"] = None if data["vertify_status"] == "nan" else data["vertify_status"]
#         data["vip_grade"] = None if data["vip_grade"] == "nan" else data["vip_grade"]
#         data["serpro_grade"] = None if data["serpro_grade"] == "nan" else data["serpro_grade"]
#         data["serpro_status"] = None if data["serpro_status"] == "nan" else data["serpro_status"]
#         data["exclusive"] = None if data["exclusive"] == "nan" else data["exclusive"]
#         data["capacity"] = None if data["capacity"] == "nan" else data["capacity"]
#         data["birth"] = None if data["birth"] == "" else data["birth"]
#         logger.info("处理后data:%s" %data)
#
#         # 生日类型的数据处理
#         if data["birth"]:
#             birth_data = list(data["birth"])
#             birth_data.insert(4,"-")
#             birth_data.insert(-2,"-")
#             birth_data = "".join(birth_data)
#             logger.info(birth_data)
#             try:
#                 datetime.datetime.strptime(birth_data, "%Y-%m-%d")
#             except:
#                 logger.info("生日转换报错了 不要了")
#                 continue
#
#         insert_sql = '''insert into crm_user
#         values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
#         params = [data["unionid"],data["parentid"],data["phone"],data["status"],data["nickname"],data["name"],
#                   data["sex"],data["birth"],data["address"],data["nationality"],data["vertify_status"],
#                   data["vip_grade"],data["vip_starttime"],data["vip_endtime"],data["serpro_grade"],data["serpro_status"],data["serpro_starttime"],
#                   0,data["capacity"],data["exclusive"],data["addtime"],data["operatename"],data["statistic_time"]]
#         analyze_cursor.execute(insert_sql,params)
#         conn_analyze.commit()
#         logger.info("入库成功")
#         logger.info("-------------------------------------")
#
#
#
#
#
#     # 第一次
#     # last_data = user_data_result[1]
#
#
#     # last_data["birth"].fillna("",inplace=True)
#
#     # logger.info(last_data.iloc[0])
#     # conn = sqlalchemy_conn(analyze_mysql_conf)
#     # last_data.to_sql("crm_user", con=conn, if_exists="append", index=False)
#     # logger.info("写入成功")
#
# except:
#     logger.info(traceback.format_exc())
# finally:
#     conn_analyze.close()
#     conn_crm.close()



# pandas读取 sql批量插入
# 转时间函数 转不成功赋值 最后在清洗处理
# def str_to_date(x):
#     try:
#         if x:
#            return pd.to_datetime(x).strftime('%Y-%m-%d')
#         else:
#             pass
#     except:
#        return "error_birth"
#
# start_time = int(time.time())
# try:
#     conn_crm = direct_get_conn(crm_mysql_conf)
#     conn_analyze = direct_get_conn(analyze_mysql_conf)
#     analyze_cursor = conn_analyze.cursor()
#     logger.info(conn_analyze)
#     sql = '''select id unionid,pid parentid,phone,status,nickname,name,sex,FROM_UNIXTIME(addtime,"%Y-%m-%d %H:%i:%s") addtime from luke_sincerechat.user'''
#     logger.info(sql)
#     crm_datas = pd.read_sql(sql,conn_crm)
#     logger.info(crm_datas.shape)
#
#     sql = '''select unionid,status vertify_status,address,birth,nationality from luke_crm.authentication'''
#     auth_datas = pd.read_sql(sql,conn_crm)
#     crm_datas = crm_datas.merge(auth_datas, how="left", on="unionid")
#
#     # conn = direct_get_conn(crm_mysql_conf)
#     vip_datas = ""
#     serpro_datas = ""
#     # pandas read_sql 不支持sql时间戳格式化FROM_UNIXTIME 所以这里改用sql查询方式 然后进行pandas 的动态拼接
#     with conn_crm.cursor() as cursor:
#         sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from luke_crm.user_vip'''
#         cursor.execute(sql)
#         vip_datas = pd.DataFrame(cursor.fetchall())
#
#         sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from luke_crm.user_serpro'''
#         cursor.execute(sql)
#         serpro_datas = pd.DataFrame(cursor.fetchall())
#
#
#
#     crm_datas = crm_datas.merge(vip_datas, how="left", on="unionid")
#     crm_datas = crm_datas.merge(serpro_datas, how="left", on="unionid")
#
#     logger.info(crm_datas.shape)
#     logger.info(crm_datas.describe)
#
#     #查询运营中心的身份
#     sql = '''select phone,capacity,exclusive from luke_lukebus.user'''
#     operate_data = pd.read_sql(sql,conn_crm)
#     crm_datas = crm_datas.merge(operate_data,how="left",on="phone")
#
#     #查对应的运营中心
#     user_data_result = get_all_user_operationcenter(crm_datas)
#
#     last_data = user_data_result[1]
#
#     #把数字的要转成str 不然不能替换
#     last_data["vertify_status"] = last_data["vertify_status"].astype("object")
#     last_data["vip_grade"] = last_data["vip_grade"].astype("object")
#     last_data["serpro_grade"] = last_data["serpro_grade"].astype("object")
#     last_data["serpro_status"] = last_data["serpro_status"].astype("object")
#     last_data["exclusive"] = last_data["exclusive"].astype("object")
#     last_data["capacity"] = last_data["capacity"].astype("object")
#
#
#
#     last_data = last_data.where(last_data.notnull(), None)
#     last_data["birth"] = last_data["birth"].replace("", None)
#     last_data["birth"] = last_data['birth'].apply(lambda x: str_to_date(x))
#     last_data.drop(last_data[last_data["birth"] == "error_birth"].index, inplace=True, axis=0)
#
#     last_data["statistic_time"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     last_data = last_data.to_dict("records")
#     # 准备入库
#     all_data = []
#     for data in last_data:
#         # data["vertify_status"] = None if data["vertify_status"] == "nan" else data["vertify_status"]
#         # data["vip_grade"] = None if data["vip_grade"] == "nan" else data["vip_grade"]
#         # data["serpro_grade"] = None if data["serpro_grade"] == "nan" else data["serpro_grade"]
#         # data["serpro_status"] = None if data["serpro_status"] == "nan" else data["serpro_status"]
#         # data["exclusive"] = None if data["exclusive"] == "nan" else data["exclusive"]
#         # data["capacity"] = None if data["capacity"] == "nan" else data["capacity"]
#         # data["birth"] = None if data["birth"] == "" else data["birth"]
#         # logger.info("处理后data:%s" %data)
#         #
#         # # 生日类型的数据处理
#         # if data["birth"]:
#         #     birth_data = list(data["birth"])
#         #     birth_data.insert(4,"-")
#         #     birth_data.insert(-2,"-")
#         #     birth_data = "".join(birth_data)
#         #     logger.info(birth_data)
#         #     try:
#         #         datetime.datetime.strptime(birth_data, "%Y-%m-%d")
#         #     except:
#         #         logger.info("生日转换报错了 不要了")
#         #         continue
#
#
#         params = (data["unionid"],data["parentid"],data["phone"],data["status"],data["nickname"],data["name"],
#                   data["sex"],data["birth"],data["address"],data["nationality"],data["vertify_status"],
#                   data["vip_grade"],data["vip_starttime"],data["vip_endtime"],data["serpro_grade"],data["serpro_status"],data["serpro_starttime"],
#                   0,data["capacity"],data["exclusive"],data["addtime"],data["operatename"],data["statistic_time"])
#
#         all_data.append(params)
#
#     logger.info("准备入库")
#     insert_sql = '''insert into crm_user_im
#             values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
#     analyze_cursor.executemany(insert_sql,all_data)
#     conn_analyze.commit()
#     logger.info("入库成功")
#     logger.info("-------------------------------------")
#
#
# except:
#     logger.info(traceback.format_exc())
# finally:
#     conn_analyze.close()
#     conn_crm.close()
# end_time = int(time.time())
# logger.info(end_time-start_time)
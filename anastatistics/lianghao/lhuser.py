# -*- coding: utf-8 -*-

# @Time : 2021/11/22 15:10

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : lhuser.py
import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
from analyzeserver.common import *
import traceback
import time

start_time = int(time.time())

def str_to_date(x):
    try:
        if x:
           return pd.to_datetime(x).strftime('%Y-%m-%d')
        else:
            pass
    except:
       return "error_birth"

try:
    conn_read = direct_get_conn(lianghao_mysql_conf)
    sql = '''select id lh_user_id,`status` lh_status,phone,create_time addtime,update_time updtime from lh_user where del_flag = 0'''
    lh_datas = pd.read_sql(sql,conn_read)
    conn_read.close()

    logger.info(lh_datas.shape)
    logger.info(lh_datas.loc[0])

    crm_mysql_conf["db"] = "luke_sincerechat"
    conn_crm = direct_get_conn(crm_mysql_conf)
    crm_datas = ""
    with conn_crm.cursor() as cursor:
        sql = '''select sex,id unionid,pid parentid,phone,nickname,`name` from user where phone is not null or phone != ""'''
        cursor.execute(sql)
        crm_datas = pd.DataFrame(cursor.fetchall())
        logger.info(len(crm_datas))
        logger.info(crm_datas.loc[0])

    user_datas = lh_datas.merge(crm_datas,how="left",on="phone")
    conn_crm.close()

    logger.info(user_datas.shape)
    logger.info(user_datas.loc[0])

    #获取用户的实名信息
    crm_mysql_conf["db"] = "luke_crm"
    conn_crm = direct_get_conn(crm_mysql_conf)
    auth_datas = ""
    with conn_crm.cursor() as cursor:
        sql = '''select unionid,types auto_type,status vertify_status,address,birth,nationality from authentication'''
        cursor.execute(sql)
        auth_datas = pd.DataFrame(cursor.fetchall())

    last_datas = user_datas.merge(auth_datas,how="left",on="unionid")

    # 查询运营中心的身份
    sql = '''select phone,capacity,exclusive from luke_lukebus.user'''
    operate_data = pd.read_sql(sql, conn_crm)
    logger.info(operate_data)
    last_datas = last_datas.merge(operate_data,how="left",on="phone")

    vip_datas = ""
    serpro_datas = ""
    with conn_crm.cursor() as cursor:
        sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from user_vip'''
        cursor.execute(sql)
        vip_datas = pd.DataFrame(cursor.fetchall())

        sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from user_serpro'''
        cursor.execute(sql)
        serpro_datas = pd.DataFrame(cursor.fetchall())

    last_datas = last_datas.merge(vip_datas,how="left",on="unionid")
    last_datas = last_datas.merge(serpro_datas,how="left",on="unionid")

    logger.info(last_datas.iloc[0])

    user_data_result = get_all_user_operationcenter(last_datas)
    ok_datas = user_data_result[1]
    logger.info("数据返回成功")
    conn_crm.close()

    logger.info(last_datas.iloc[0])

    ok_datas["vertify_status"] = ok_datas["vertify_status"].astype("object")
    ok_datas["vip_grade"] = ok_datas["vip_grade"].astype("object")
    ok_datas["serpro_grade"] = ok_datas["serpro_grade"].astype("object")
    ok_datas["serpro_status"] = ok_datas["serpro_status"].astype("object")
    ok_datas["exclusive"] = ok_datas["exclusive"].astype("object")
    ok_datas["capacity"] = ok_datas["capacity"].astype("object")

    ok_datas = ok_datas.where(ok_datas.notnull(), None)
    ok_datas["birth"] = ok_datas["birth"].replace("", None)
    ok_datas["birth"] = ok_datas['birth'].apply(lambda x: str_to_date(x))
    ok_datas.drop(ok_datas[ok_datas["birth"] == "error_birth"].index, inplace=True, axis=0)
    ok_datas = ok_datas[ok_datas.phone.apply(lambda x: len(str(x)) == 11)]

    ok_datas["statistic_time"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    logger.info(tomorrow_time)

    create_sql = '''
        CREATE TABLE `lh_user_%s` (
      `unionid` bigint(20) DEFAULT NULL COMMENT 'unionid',
      `parentid` bigint(20) DEFAULT NULL COMMENT '推荐id',
      `lh_user_id` varchar(50) DEFAULT NULL COMMENT '靓号id',
      `lh_status` tinyint(2) DEFAULT NULL COMMENT '启用状态 0否 1是',
      `phone` varchar(11) DEFAULT NULL COMMENT '手机号码',
      `addtime` datetime DEFAULT NULL COMMENT '创建时间',
      `updtime` datetime DEFAULT NULL COMMENT '更新时间',
      `sex` tinyint(1) DEFAULT NULL COMMENT '0未知  1男  2女',
      `nickname` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '昵称',
      `name` varchar(100) DEFAULT NULL COMMENT '真实姓名',
      `auto_type` tinyint(1) DEFAULT NULL COMMENT '1 人工审核  2自动审核',
      `vertify_status` tinyint(1) DEFAULT NULL COMMENT '0待认证 1待审核 2认证中 3失败 4成功',
      `address` text COMMENT '身份证地址',
      `birth` date DEFAULT NULL COMMENT '出生日期',
      `nationality` varchar(50) DEFAULT NULL COMMENT '族',
      `vip_grade` tinyint(2) DEFAULT NULL COMMENT '等级 1普通会员 2vip会员 3至尊VIP',
      `vip_starttime` datetime DEFAULT NULL COMMENT 'vip开始时间',
      `vip_endtime` datetime DEFAULT NULL COMMENT 'vip结束时间',
      `serpro_grade` tinyint(2) DEFAULT NULL COMMENT '服务商等级：1初级 2中级 3高级 4机构服务商',
      `serpro_status` tinyint(2) DEFAULT NULL COMMENT '服务视状态：1正常  0已注销',
      `serpro_starttime` datetime DEFAULT NULL COMMENT '服务商的开通时间',
      `capacity` tinyint(2) DEFAULT NULL COMMENT '1运营中心/公司 2网店主 3带货者 20无身份(普通用户)',
      `exclusive` tinyint(1) DEFAULT NULL COMMENT '网店主类型：1专营店 2自营店',
      `user_type` tinyint(1) DEFAULT '0' COMMENT '0:正常用户 1：开发用户 2：运营 3：测试',
      `operatename` varchar(100) DEFAULT NULL COMMENT '用户对应的运营中心',
      `statistic_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '同步时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    ''' %(tomorrow_time)
    conn_ana = direct_get_conn(analyze_mysql_conf)
    cursor_ana = conn_ana.cursor()
    cursor_ana.execute(create_sql)
    conn_ana.commit()
    conn_ana.close()
    logger.info("建表成功")

    #准备入库
    logger.info(analyze_mysql_conf)
    conn_rw = sqlalchemy_conn(analyze_mysql_conf)
    logger.info(conn_rw)
    table_name = "lh_user_%s" %tomorrow_time
    logger.info(table_name)
    ok_datas.to_sql(table_name, con=conn_rw, if_exists="append", index=False)
    logger.info("写入成功")
except:
    logger.exception(traceback.format_exc())

end_time = int(time.time())
logger.info("run %s second" %(end_time-start_time))
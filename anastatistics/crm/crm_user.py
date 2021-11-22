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

start_time = int(time.time())
try:
    conn_crm = direct_get_conn(crm_mysql_conf)
    conn_analyze = direct_get_conn(analyze_mysql_conf)
    analyze_cursor = conn_analyze.cursor()
    logger.info(conn_analyze)
    sql = '''select id unionid,pid parentid,phone,status,nickname,name,sex,FROM_UNIXTIME(addtime,"%Y-%m-%d %H:%i:%s") addtime from luke_sincerechat.user'''
    logger.info(sql)
    crm_datas = pd.read_sql(sql,conn_crm)
    logger.info(crm_datas.shape)

    sql = '''select unionid,status vertify_status,address,birth,nationality from luke_crm.authentication'''
    auth_datas = pd.read_sql(sql,conn_crm)
    crm_datas = crm_datas.merge(auth_datas, how="left", on="unionid")

    # conn = direct_get_conn(crm_mysql_conf)
    vip_datas = ""
    serpro_datas = ""
    # pandas read_sql 不支持sql时间戳格式化FROM_UNIXTIME 所以这里改用sql查询方式 然后进行pandas 的动态拼接
    with conn_crm.cursor() as cursor:
        sql = '''select unionid,grade vip_grade,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') vip_starttime,FROM_UNIXTIME(endtime,'%Y-%m-%d %H:%i:%s') vip_endtime from luke_crm.user_vip'''
        cursor.execute(sql)
        vip_datas = pd.DataFrame(cursor.fetchall())

        sql = '''select unionid,grade serpro_grade,status serpro_status,FROM_UNIXTIME(starttime,'%Y-%m-%d %H:%i:%s') serpro_starttime from luke_crm.user_serpro'''
        cursor.execute(sql)
        serpro_datas = pd.DataFrame(cursor.fetchall())



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


    # 第一次入库走这个
    # conn = sqlalchemy_conn(analyze_mysql_conf)
    # last_data.to_sql("crm_user_pd", con=conn, if_exists="append", index=False)
    # logger.info("写入成功")

    #后续要走批量更新
    #手机号码为空的过滤
    logger.info(last_data.shape)
    ok_data = last_data[last_data["phone"].notna()]
    logger.info(ok_data.shape)
    ok_data = ok_data.to_dict("records")
    ok_data_len = len(ok_data)

    for i,data in enumerate(ok_data):
        logger.info("当前到:%s 总数:%s" %(i,ok_data_len))
        logger.info(data)
        logger.info("------------------------------------")
        select_sql = '''select * from crm_user_pd where phone = %s'''
        analyze_cursor.execute(select_sql,(data["phone"]))
        dd = analyze_cursor.fetchone()
        if dd:
            update_sql = '''update crm_user_pd set unionid=%s,parentid=%s,status=%s,nickname=%s,name=%s,sex=%s,birth=%s,address=%s,
            nationality=%s,vertify_status=%s,vip_grade=%s,vip_starttime=%s,vip_endtime=%s,serpro_grade=%s,serpro_status=%s,
            serpro_starttime=%s,capacity=%s,exclusive=%s,addtime=%s,operatename=%s,statistic_time=%s where phone = %s
            '''
            # logger.info("update_sql:%s" %update_sql)
            params = [data["unionid"],data["parentid"],data["status"],data["nickname"],data["name"],data["sex"],data["birth"],data["address"],
                      data["nationality"],data["vertify_status"],data["vip_grade"],data["vip_starttime"],data["vip_endtime"],data["serpro_grade"],
                      data["serpro_status"],data["serpro_starttime"],data["capacity"],data["exclusive"],data["addtime"],data["operatename"],datetime.datetime.now(),data["phone"]]

            analyze_cursor.execute(update_sql,params)
            # analyze_cursor.execute(update_sql)
            conn_analyze.commit()
            logger.info("更新成功")
        else:
            insert_sql = '''insert into crm_user_pd 
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s，%s)'''
            params = [data["unionid"], data["parentid"], data["phone"], data["status"], data["nickname"], data["name"],
                      data["sex"], data["birth"], data["address"], data["nationality"], data["vertify_status"],
                      data["vip_grade"], data["vip_starttime"], data["vip_endtime"], data["serpro_grade"],
                      data["serpro_status"], data["serpro_starttime"],
                      0, data["capacity"], data["exclusive"], data["addtime"], data["operatename"],
                      data["statistic_time"],0]
            analyze_cursor.execute(insert_sql, params)
            conn_analyze.commit()
            logger.info("插入成功")

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
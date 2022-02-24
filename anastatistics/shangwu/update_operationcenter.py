# -*- coding: utf-8 -*-
# @Time : 2021/12/30  18:11
# @Author : shihong
# @File : .py
# --------------------------------------
# 更新运营中心数据---每十分钟同步一次
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from config import *
from analyzeserver.common import *
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime
from datetime import timedelta
from functools import reduce
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token

# 运营中心的 0/13 跑

# 新数据插入
def insert_operatecenter():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_crm = conn_crm.cursor()
        cursor_analyze = conn_analyze.cursor()

        if not conn_analyze or not conn_crm:
            return "数据库连接失败"
        # 查找旧数据
        new_data_sql = '''select t1.*, t2.nickname from 
            (select * from luke_lukebus.operationcenter) t1
            left join
            (select id, nickname from luke_sincerechat.user) t2
            on t1.unionid = t2.id'''

        old_data_sql = '''
            select * from lh_analyze.operationcenter
        '''
        new_data = pd.read_sql(new_data_sql, conn_crm)
        old_data = pd.read_sql(old_data_sql, conn_analyze)

        new_data_id_list = new_data['id'].tolist()
        old_data_id_list = old_data['id'].tolist()
        sub_id_list = list(set(new_data_id_list) - set(old_data_id_list))
        # 如果没有数据不插入
        if not sub_id_list:
            return "暂无更新数据"
        logger.info('开始更新数据')
        new_data_df = new_data[new_data['id'].isin(sub_id_list)]
        new_data_df['create_time'] = new_data_df['addtime'].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)))
        new_data_df.drop('addtime', axis=1, inplace=True)
        conn_an = pd_conn(analyze_mysql_conf)

        # area_id = new_data_df["area"].to_list()
        new_data_df["province"] = ""
        new_data_df["city"] = ""
        new_data_df["region"] = ""
        new_data_df["town"] = ""


        # 省市区
        pro_sql = '''select * from province'''
        city_sql = '''select * from city'''
        region_sql = '''select * from region'''

        pro_data = pd.read_sql(pro_sql,conn_analyze)
        city_data = pd.read_sql(city_sql,conn_analyze)
        region_data = pd.read_sql(region_sql,conn_analyze)

        new_data_df = new_data_df.to_dict("records")
        for data in new_data_df:

            #找地址 如果地址为0 跳过
            if int(data["area"]) == 0:
                continue

            sql = '''
            WITH RECURSIVE temp as (
                SELECT t.* FROM luke_crm.address t WHERE id = %s
                UNION ALL
                SELECT t.* FROM luke_crm.address t INNER JOIN temp ON t.id = temp.pid
            )
            SELECT name FROM temp where pid != 0;
            '''
            cursor_crm.execute(sql,(data["area"]))
            area_datas = cursor_crm.fetchall()

            # 当前查出的地址不等于3 不要
            if len(area_datas) != 3:
                continue

            logger.info(area_datas)
            #判断是否有直辖市
            for addr in ["北京市","天津市","上海市","重庆市"]:
                if addr.find(area_datas[2]["name"]) > -1:
                    area_datas[1] = {"name":"市辖区"}

            for i in range(0,len(area_datas)):
                logger.info(i)
                try:
                    if i == 0:
                        new_region_data = region_data[region_data["name"].str.contains(area_datas[i]["name"])]
                        if new_region_data.shape[0] == 0:
                            break
                        data["region_code"] = new_region_data["code"].values[0]
                    elif i == 1:
                        logger.info(area_datas[i]["name"])
                        if area_datas[i]["name"] == "市辖区":
                            logger.info(area_datas[i+1])
                            current_pro_sql = '''select * from province where name like "%s"''' %("%"+area_datas[i+1]["name"]+"%")
                            logger.info(current_pro_sql)
                            cursor_analyze.execute(current_pro_sql)
                            new_pro_data = cursor_analyze.fetchone()
                            new_pro_code = new_pro_data[1]

                            current_city_sql = '''select * from region where name like "%s" and province_code = %s''' %("%"+area_datas[i-1]["name"]+"%",new_pro_code)
                            cursor_analyze.execute(current_city_sql)
                            new_city_data = cursor_analyze.fetchone()
                            if not new_city_data:
                                break
                            data["city_code"] = new_city_data[2]
                        else:
                            new_city_data = city_data[city_data["name"].str.contains(area_datas[i]["name"])]
                            if new_city_data.shape[0] == 0:
                                break
                            data["city_code"] = new_city_data["code"].values[0]
                    elif i == 2:
                        new_pro_data = pro_data[pro_data["name"].str.contains(area_datas[i]["name"])]
                        if new_pro_data.shape[0] == 0:
                            break
                        data["province_code"] = new_pro_data["code"].values[0]
                    else:
                        pass
                except:
                    logger.info(area_datas)
                    logger.info("报错啦")
            logger.info(data)
            logger.info("-----------------------------")

        new_data_df = pd.DataFrame(new_data_df)
        logger.info(new_data_df)
        new_data_df.to_sql('operationcenter', con=conn_an, if_exists='append', index=False)
        return '更新完成，更新%s个运营中心' % len(sub_id_list)
    except:
        logger.error(traceback.format_exc())
        return "更新失败"
    finally:
        try:
            conn_crm.close()
            conn_analyze.close()
        except:
            pass

# 老数据更新
def update_operatecenter():
    try:
        conn_crm = direct_get_conn(crm_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor_crm = conn_crm.cursor()
        cursor_analyze = conn_analyze.cursor()

        if not conn_analyze or not conn_crm:
            return "数据库连接失败"
        # 查找旧数据
        new_data_sql = '''select t1.*, t2.nickname from 
            (select * from luke_lukebus.operationcenter) t1
            left join
            (select id, nickname from luke_sincerechat.user) t2
            on t1.unionid = t2.id'''

        cursor_crm.execute(new_data_sql)
        new_datas = cursor_crm.fetchall()

        for data in new_datas:
            logger.info(data)

            params = []
            # 地址 营业制造 其他认证走自己的
            update_sql = '''
            update operationcenter set achievement_id=%s,unionid=%s,punionid=%s,capacity=%s,name=%s,nickname=%s,telephone=%s,operatename=%s,unifiedsocial=%s,
            authnumber=%s,is_factory=%s,area=%s,status=%s,crm=%s where id = %s
            '''
            params.append(data["achievement_id"])
            params.append(data["unionid"])
            params.append(data["punionid"])
            params.append(data["capacity"])
            params.append(data["name"])
            params.append(data["nickname"])
            params.append(data["telephone"])
            params.append(data["operatename"])
            params.append(data["unifiedsocial"])
            params.append(data["authnumber"])
            params.append(data["is_factory"])
            params.append(data["area"])
            params.append(data["status"])
            params.append(data["crm"])
            params.append(data["id"])
            cursor_analyze.execute(update_sql,params)
            conn_analyze.commit()
            logger.info("更新完成")

        return '更新结束'
    except:
        conn_analyze.rollback()
        logger.error(traceback.format_exc())
        return "更新失败"
    finally:
        try:
            conn_crm.close()
            conn_analyze.close()
        except:
            pass

if __name__ == '__main__':
    result = insert_operatecenter()
    logger.info(result)

    # result = update_operatecenter()
    # logger.info(result)
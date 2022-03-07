# -*- coding: utf-8 -*-

# @Time : 2022/3/1 14:40

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : user_grow_82_insert.py
import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
from datetime import timedelta,date
from functools import reduce
from analyzeserver.common import *
import time


'''用户增长统计表 8位二手 第一次'''


'''用户注册时间 用户最后一次登录时间 以及转让市场总额度'''
def user_mes():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        cusor_lh = conn_lh.cursor()
        sql = '''select phone,create_time,last_login_time from lh_user where del_flag = 0'''
        user_data = pd.read_sql(sql,conn_lh)

        #8位特殊用户配置额度
        sql = '''select phone,limits from le_user_special_second_limit where del_flag = 0 '''
        special_limit_data = pd.read_sql(sql,conn_lh)

        user_data = user_data.merge(special_limit_data,how="left",on="phone")

        #如果额度为空走配置表  二手市场采购额度
        sql = '''select param_value from le_config_platform where id = 39'''
        cusor_lh.execute(sql)
        param_value = cusor_lh.fetchone()[0]

        user_data["limits"].fillna(param_value,inplace=True)

        user_data["create_time"] = user_data["create_time"].astype(str)
        user_data["last_login_time"] = user_data["last_login_time"].astype(str)
        user_data["create_time"] = user_data["create_time"].apply(lambda x: x.replace("NaT", ""))
        user_data["last_login_time"] = user_data["last_login_time"].apply(lambda x: x.replace("NaT", ""))
        return True,user_data
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()


# 二手市场的采购额度
def buy_limit():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        sql = '''SELECT phone,sum(total_price) buy_limit FROM le_order WHERE del_flag = 0 AND type = 4  AND STATUS IN (1,0) group by phone'''
        buy_limit_data = pd.read_sql(sql,conn_lh)
        return True,buy_limit_data
    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()

#二手市场回复额度
# def recovery_limit():
#     try:
#         conn_lh = direct_get_conn(lianghao_mysql_conf)
#         df_list = []
#
#         tables = ["0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"]
#         for t in tables:
#             logger.info("table:%s" %t)
#             sql = '''select hold_phone phone,sell_order_sn order_sn from le_pretty_hold_%s where del_flag = 0 and status = 3 group by phone,order_sn''' %t
#             # sql = '''select hold_phone phone,sell_order_sn order_sn,update_time from le_pretty_hold_%s where del_flag = 0 and status = 3 ''' %t
#             data = pd.read_sql(sql,conn_lh)
#             df_list.append(data)
#         hold_data = pd.concat(df_list,axis=0)
#
#         order_sql = '''select order_sn,total_price recovery_limit from le_order where del_flag = 0 and type = 4 and status = 1'''
#         order_data = pd.read_sql(order_sql,conn_lh)
#         recv_data = hold_data.merge(order_data,how="left",on="order_sn")
#         recv_data.drop(["order_sn"],axis=1,inplace=True)
#
#         # recv_limit_data = pd.read_sql(sql, conn_lh)
#         return True, recv_data
#
#     except Exception as e:
#         logger.info(traceback.format_exc())
#         return False, e
#     finally:
#         conn_lh.close()



# 官方采购金额

def official_buy():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)

        kanban_sql = '''select status,time_type,start_time,end_time,inside_publish_phone,inside_recovery_phone from data_board_settings where del_flag = 0 and market_type = 2'''
        kanban_data = pd.read_sql(kanban_sql, conn_analyze).to_dict("records")
        sql = '''SELECT phone,sum(total_price) official_total_money FROM le_order WHERE del_flag = 0 AND type = 0  AND STATUS = 1 '''
        group_sql = ''' group by phone'''
        if kanban_data[0]["inside_publish_phone"][1:-1]:
            sql = sql + ''' sell_phone in (%s) ''' %(kanban_data[0]["inside_publish_phone"][1:-1])
            sql = sql + group_sql
            official_buy_data = pd.read_sql(sql, conn_lh)
        else:
            official_buy_data = pd.DataFrame([{"phone":"","official_total_money":""}])
        return True, official_buy_data

    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()

# 转让市场采购金额
def tran_buy():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        kanban_sql = '''select status,time_type,start_time,end_time,inside_publish_phone,inside_recovery_phone from data_board_settings where del_flag = 0 and market_type = 2'''
        kanban_data = pd.read_sql(kanban_sql, conn_analyze).to_dict("records")
        sql = '''SELECT phone,sum(total_price) official_total_money FROM le_order WHERE del_flag = 0 AND type = 0  AND STATUS = 1 '''
        group_sql = ''' group by phone'''

        sql = '''SELECT phone,sum(total_price) tran_money FROM le_order WHERE del_flag = 0 AND type = 4  AND STATUS = 1 group by phone'''

        if kanban_data[0]["inside_publish_phone"][1:-1]:
            sql = sql + ''' sell_phone not in (%s) ''' %(kanban_data[0]["inside_publish_phone"][1:-1])
            sql = sql + group_sql
        else:
            pass
        return True,

    except Exception as e:
        logger.info(traceback.format_exc())
        return False, e
    finally:
        conn_lh.close()

if __name__ == "__main__":
    user_data = ""
    user_mes_result = user_mes()
    if user_mes_result[0]:
        user_data = user_mes_result[1]
    else:
        logger.info("用户注册时间 用户最后一次登录时间 以及转让市场总额度查询失败了")
        exit()

    buy_result = buy_limit()
    if buy_result[0]:
        user_data = user_data.merge(buy_result[1],on="phone",how="left")
    else:
        logger.info("二手市场的采购额度查询失败了")
        exit()

    user_data["buy_limit"].fillna(0,inplace=True)
    logger.info(user_data)
    # user_data.to_csv("e:/123321456.csv")
    user_data["limits"] = user_data["limits"].astype(int)
    user_data["current_limit"] = user_data["limits"] - user_data["buy_limit"]


    # official_buy_result = official_buy()
    # if official_buy_result[0]:
    #     user_data.merge(official_buy_result[1],on="phone",how="left")
    # else:
    #     logger.info("官方采购金额查询失败了")
    #
    # user_data["official_money"].fillna(0,inplace=True)
    #
    # tran_buy_result = tran_buy()
    # if tran_buy_result[0]:
    #     user_data.merge(tran_buy_result[1],on="phone",how="left")
    # else:
    #     logger.info("转让市场采购金额查询失败了")
    #
    # user_data["tran_money"].fillna(0,inplace=True)

    logger.info("run ok")
    user_data.to_csv("e:/8282.csv")
# -*- coding: utf-8 -*-

# @Time : 2021/12/22 15:22

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : lh8_wholesale_home.py

import sys

import pandas as pd

sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime
from datetime import timedelta
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
from analyzeserver.common import *
import threading


lhpfhome8 = Blueprint('lhpfhome', __name__, url_prefix='/lh/pifa/home')
r = get_redis()

@lhpfhome8.route("deal/person",methods=["GET"])
def deal_person():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        cursor = conn_analyze.cursor()

        try:
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}
            logger.info("token:%s" %token)
            logger.info("user_id:%s" %user_id)
            check_token_result = check_token(token, user_id)
            logger.info(check_token_result)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}




        logger.info(conn_lh)
        #8位个人
        sql = '''select phone,sum(total_price) total_money from le_order where del_flag = 0 and `status`=1 and type in (1) and DATE_FORMAT(create_time,"%Y%m%d") =CURRENT_DATE() group by phone order by total_money desc limit 10'''
        logger.info(sql)
        datas = pd.read_sql(sql,conn_lh)

        phone_lists = datas["phone"].tolist()

        logger.info(phone_lists)
        if not phone_lists:
            return {"code": "0000", "status": "success", "msg": []}
        sql = '''select phone,if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username from crm_user where phone in ({})'''.format(",".join(phone_lists))
        logger.info(sql)
        user_data = pd.read_sql(sql,conn_analyze)
        datas = datas.merge(user_data,on="phone",how="left")
        logger.info(datas)
        datas["username"].fillna("",inplace=True)
        datas = datas.to_dict("records")

        return {"code":"0000","status":"success","msg":datas}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
        conn_analyze.close()


# 运营中心
@lhpfhome8.route("deal/bus",methods=["GET"])
def deal_business():
    try:
        try:
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        conn_lh = direct_get_conn(lianghao_mysql_conf)
        conn_analyze = direct_get_conn(analyze_mysql_conf)


        #先查运营中心的人数

        cursor_lh = conn_lh.cursor()
        cursor_ana = conn_analyze.cursor()

        bus_sql = '''select operatename,contains from operate_relationship_crm where crm = 1'''

        logger.info(bus_sql)
        cursor_ana.execute(bus_sql)
        operate_datas = cursor_ana.fetchall()

        return_lists = []

        #取出今天的订单表
        sql = '''
        select phone,sum(total_price) total_money from le_order where del_flag = 0 and type in (1) and `status`=1 and date_format(create_time,"%Y%m%d") = CURRENT_DATE() group by phone 
        '''
        cursor_lh.execute(sql)
        order_datas = pd.DataFrame(cursor_lh.fetchall())
        # 如果暂无交易数据，返回空
        if order_datas.shape[0] > 0:
            for ol in reversed(operate_datas):
                ol_dict = {}
                phone_list = json.loads(ol[1])
                ol_dict["operatename"] = ol[0]
                order_data_phone = order_datas[order_datas[0].isin(phone_list)][1]
                ol_dict["total_money"] = order_data_phone.sum() if len(order_data_phone)>0 else 0
                return_lists.append(ol_dict)

            logger.info(return_lists)
            return_lists = pd.DataFrame(return_lists)
            return_lists.sort_values(by="total_money",ascending=False,inplace=True)
            return_lists['total_money'] = return_lists['total_money'].astype(float)
            logger.info(return_lists.iloc[0:10])
            return_data = return_lists.iloc[0:10].to_dict("records")
        else:
            return_data = return_lists
        return {"code":"0000","status":"success","msg":return_data}

    except Exception as e:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_analyze.close()
        conn_lh.close()



# 交易数据中心 今日
@lhpfhome8.route("order/datacenter",methods=["GET"])
def order_data_center():
    try:
        try:
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}


        conn_lh = direct_get_conn(lianghao_mysql_conf)
        sql='''select count(*) person_count,sum(total_money) total_money,sum(order_count) order_count,sum(total_count) total_count from(
select phone,sum(total_price) total_money,count(*) order_count,sum(count) total_count from le_order where del_flag = 0 and type in (1) and `status` = 1 and DATE_FORMAT(create_time,'%Y-%m-%d') = CURRENT_DATE() group by phone) t2'''
        logger.info(sql)
        data = pd.read_sql(sql,conn_lh)
        data = data.to_dict("records")[0]
        logger.info(data)
        return {"code":"0000","status":"success","msg":data}

    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()

# 今日交易实时动态
@lhpfhome8.route('/today/dynamic/transaction', methods=["GET"])
def today_dynamic_transaction():
    try:
        try:
            logger.info(request.json)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}

        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_analyze or not conn_lh:
            return {"code": "10002", "status": "failer", "msg": message["10002"]}

        # 八位
        # sell_order_sql_8 = '''
        #         select t1.sub_time, t1.phone, t2.pretty_type_name from
        #         (select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, phone, sell_id from lh_pretty_client.le_order
        #         where del_flag=0 and type in (1, 4) and (phone is not null or phone !='') and `status`=1
        #         and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE
        #         order by pay_time desc
        #         limit 10
        #         ) t1
        #         left join
        #         (select id, pretty_type_name from lh_pretty_client.le_second_hand_sell
        #         where id in
        #         (select sell_id from lh_pretty_client.le_order where del_flag=0 and type in (1) and (phone is not null or phone !='') and `status`=1
        #         and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE)
        #         union all
        #         select id, pretty_type_name from lh_pretty_client.le_sell
        #         where id in
        #         (select sell_id from lh_pretty_client.le_order where del_flag=0 and type in (1) and (phone is not null or phone !='') and `status`=1
        #         and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE)
        #         ) t2
        #         on t1.sell_id = t2.id
        #     '''
        sell_order_sql_8 = '''
            select t1.sub_time, t1.phone, t2.pretty_type_name from
            (select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, phone, sell_id from lh_pretty_client.le_order
            where del_flag=0 and type in (1) and (phone is not null or phone !='') and `status`=1
            and DATE_FORMAT(pay_time,"%Y-%m-%d") = CURRENT_DATE
            order by pay_time desc
            limit 10
            ) t1
            left join
            (select id, pretty_type_name from lh_pretty_client.le_sell
            where id in
            (select sell_id from lh_pretty_client.le_order where del_flag=0 and type in (1) and (phone is not null or phone !='') and `status`=1
            and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE)
            ) t2
            on t1.sell_id = t2.id
        '''

        search_name_sql = '''
                select phone, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username from lh_analyze.crm_user where phone = "%s"
            '''

        order_df_8 = pd.read_sql(sell_order_sql_8, conn_lh)
        if order_df_8.shape[0] > 0:
            order_df_8['sub_time'] = round(order_df_8['sub_time'], 0).astype(int)
            sell_phone_list = order_df_8['phone'].tolist()
            sell_df_list = []
            for phone in set(sell_phone_list):
                sell_df_list.append(pd.read_sql(search_name_sql % (phone), conn_analyze))
            sell_df = pd.concat(sell_df_list, axis=0)
            sell_fina_df = order_df_8.merge(sell_df, how='left', on='phone')
            sell_fina_df.sort_values('sub_time', ascending=True, inplace=True)
            sell_fina_df = sell_fina_df[:3]
            sell_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            sell_fina_df["username"].fillna("",inplace=True)
            sell_list = sell_fina_df.to_dict("records")
        else:
            sell_list = []

        return_data = {
            "sell_dynamic": sell_list,
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
            conn_analyze.close()
        except:
            pass

# 今日发布实时动态
@lhpfhome8.route('/today/dynamic/publish', methods=["GET"])
def today_dynamic_publish():
    try:
        try:
            logger.info(request.json)
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh or not conn_analyze:
            return {"code": "10002", "status": "failed", "message": message["10002"]}

        # 用户名称搜索
        search_name_sql = '''
                select phone, if(`name` is not null and `name`!='',`name`,if(nickname is not null,nickname,"")) username from lh_analyze.crm_user where phone = "%s"
            '''
        # 转卖 + 二手
        # publish_order_sql = '''
        # (select TIMESTAMPDIFF(second,up_time,now())/60 sub_time, sell_phone phone, pretty_type_name
        # from lh_pretty_client.le_sell
        # where del_flag=0 and (sell_phone is not null or sell_phone != '')
        # and DATE_FORMAT(up_time,"%Y-%m-%d") = CURRENT_DATE
        # order by up_time desc
        # limit 10)
        # union all
        # (select TIMESTAMPDIFF(second,create_time,now())/60 sub_time, sell_phone phone, pretty_type_name
        # from lh_pretty_client.le_second_hand_sell
        # where del_flag=0 and (sell_phone is not null or sell_phone != '')
        # and DATE_FORMAT(create_time,"%Y-%m-%d") = CURRENT_DATE
        # order by create_time desc
        # limit 10)
        # order by sub_time
        # limit 3'''
        publish_order_sql = '''
            select TIMESTAMPDIFF(second,up_time,now())/60 sub_time, sell_phone phone, pretty_type_name
            from lh_pretty_client.le_sell
            where del_flag=0 and (sell_phone is not null or sell_phone != '')
            and DATE_FORMAT(up_time,"%Y-%m-%d") = CURRENT_DATE
            order by up_time desc
            limit 3
        '''

        publish_order_df = pd.read_sql(publish_order_sql, conn_lh)
        if publish_order_df.shape[0] > 0:
            publish_order_df['sub_time'] = round(publish_order_df['sub_time'], 0).astype(int)

            publish_phone_list = publish_order_df['phone'].to_list()
            publish_df_list = []
            for phone in set(publish_phone_list):
                publish_df_list.append(pd.read_sql(search_name_sql % (phone), conn_analyze))
            publish_df = pd.concat(publish_df_list, axis=0)
            publish_fina_df = publish_order_df.merge(publish_df, how='left', on='phone')
            publish_fina_df["username"].fillna("", inplace=True)
            publish_fina_df.sort_values('sub_time', ascending=False, inplace=True)
            publish_list = publish_fina_df.to_dict("records")
        else:
            publish_list = []

        # for pl in publish_list:
        #     if pl["phone"]:
        #         pl["phone"] = pl["phone"][0:4]+len(pl["phone"][4:])*"*"
        #     if pl["username"]:
        #         pl["username"] = pl["username"][0]+len(pl["username"][1:])*"*"

        return_data = {
            "publish_dynamic": publish_list,
        }
        return {"code": "0000", "status": "success", "msg": return_data}
    except Exception as e:
        logger.error(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_lh.close()
            conn_analyze.close()
        except:
            pass

# 今日类型排行版
@lhpfhome8.route("deal/top",methods=["GET"])
def deal_top():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        try:
            token = request.headers["Token"]
            user_id = request.args.get("user_id")

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except:
            return {"code": "10004", "status": "failed", "msg": message["10004"]}

        sql = '''select pretty_type_name,unit_price,sum(count) total_count,sum(total_price) total_price from (
                select s.pretty_type_name,o.unit_price,o.count,o.total_price from le_order o
                left join le_sell s on o.sell_id = s.id
                where DATE_FORMAT(o.create_time,"%Y%m%d") = CURRENT_DATE
                and o.del_flag = 0 and o.type=1 and o.`status` = 1
                order by o.create_time desc) t group by pretty_type_name order by total_count desc
                limit 3
        '''

        data = (pd.read_sql(sql, conn_lh)).to_dict("records")
        return {"code": "0000", "status": "success", "msg": data}
    except:
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        conn_lh.close()
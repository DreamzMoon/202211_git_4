# -*- coding: utf-8 -*-
# @Time : 2021/12/28  17:31
# @Author : shihong
# @File : .py
# 运营中心管理增删改查
# --------------------------------------
# 查
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from analyzeserver.common import *
from config import *
import traceback
from util.help_fun import *
import pandas as pd
from analyzeserver.user.sysuser import check_token


operateconbp = Blueprint('operatecon', __name__, url_prefix='/operatecon')

# 运营中心管理数据查看
@operateconbp.route("/check", methods=["POST"])
def check_operate_data():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 8:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
            # 关键词搜索
            keyword = request.json['keyword'].strip()
            # 状态 1正常 2关闭
            status = request.json['status']
            # crm状态 0不支持 1支持
            crm_status = request.json['crm_status']
            # 创建起始时间
            create_start_time = request.json['create_start_time']
            # 创建结束时间
            create_end_time = request.json['create_end_time']
            page = request.json['page']
            size = request.json['size']
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_an = direct_get_conn(analyze_mysql_conf)
        if not conn_an:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        base_sql = '''select id, telephone phone, unionid, name, operatename, address, authnumber, unifiedsocial, create_time, crm, status from lh_analyze.operationcenter where capacity=1'''
        # sql 拼接
        if keyword:
            keyword_sql = ''' and (operatename like "%{keyword}%" or name like "%{keyword}%" or telephone like "%{keyword}%" or unionid like "%{keyword}%")'''.format(keyword=keyword)
        else:
            keyword_sql = ''
        base_sql += keyword_sql
        if status:
            status_sql = ''' and status=%s''' % status
        else:
            status_sql = ''
        base_sql += status_sql
        if crm_status == 0 or crm_status == 1:
            crm_status_sql = ''' and crm=%s''' % crm_status
        else:
            crm_status_sql = ''
        base_sql += crm_status_sql
        if create_start_time and create_end_time:
            create_time_sql = ''' and create_time>=%s and create_time<=%s''' % (create_start_time, create_end_time)
        else:
            create_time_sql = ''
        base_sql += create_time_sql
        logger.info(base_sql)
        center_data = pd.read_sql(base_sql, conn_an)
        logger.info(center_data.shape)
        logger.info(center_data.head())
        if page and size:
            start_index = (page - 1) * size
            end_index = page * size
            return_data = center_data[start_index:end_index]
        else:
            return_data = center_data
        return_data['create_time'] = return_data['create_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        return_data.fillna('', inplace=True)
        return {"code": "0000", "status": "success", "msg": return_data.to_dict("records"), "count": center_data.shape[0]}
    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
    finally:
        try:
            conn_an.close()
        except:
            pass

@operateconbp.route("/check/detail", methods=["POST"])
def check_operate_detail_data():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 2:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
            operate_id = request.json['id']
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_an = direct_get_conn(analyze_mysql_conf)
        if not conn_an:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        detail_data_sql = '''select telephone phone, unionid, name, operatename, address, authnumber, unifiedsocial, create_time, crm, status from lh_analyze.operationcenter where capacity=1 and id=%s''' % operate_id
        detail_data = pd.read_sql(detail_data_sql, conn_an)
        detail_data['create_time'] = detail_data['create_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        detail_data.fillna('', inplace=True)
        return {"code": "0000", "status": "success", "msg": detail_data.to_dict("records")}
    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
    finally:
        try:
            conn_an.close()
        except:
            pass
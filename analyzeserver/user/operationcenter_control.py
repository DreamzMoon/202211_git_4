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
        base_sql = '''select id, telephone phone, unionid, name, operatename, address, authnumber, unifiedsocial, create_time, crm, status from lh_analyze.operationcenter'''
        # sql 拼接
        condition_sql_list = []
        if keyword:
            keyword_sql = ''' (operatename like "%{keyword}%" or name like "%{keyword}%" or telephone like "%{keyword}%" or unionid like "%{keyword}%")'''.format(keyword=keyword)
            condition_sql_list.append(keyword_sql)
        if status:
            status_sql = ''' status=%s''' % status
            condition_sql_list.append(status_sql)
        if crm_status == 0 or crm_status == 1:
            crm_status_sql = ''' crm=%s''' % crm_status
            condition_sql_list.append(crm_status_sql)
        if create_start_time and create_end_time:
            create_time_sql = ''' create_time>="%s" and create_time<="%s"''' % (create_start_time, create_end_time)
            condition_sql_list.append(create_time_sql)
        # sql合并
        len_sql = len(condition_sql_list)
        if len_sql == 1:
            base_sql = base_sql + ' where' + condition_sql_list[0]
        elif len_sql > 1:
            base_sql = base_sql + ' where' + condition_sql_list[0]
            for condition_sql in condition_sql_list[1:]:
                base_sql = base_sql + ' and' + condition_sql
        else:
            base_sql = base_sql
        return base_sql

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

# 详情
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
        detail_data_sql = '''select id, telephone phone, unionid, name, operatename, address, authnumber, unifiedsocial, create_time, crm, status from lh_analyze.operationcenter where id=%s''' % operate_id
        detail_data = pd.read_sql(detail_data_sql, conn_an)
        detail_data['create_time'] = detail_data['create_time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        detail_data.fillna('', inplace=True)
        return {"code": "0000", "status": "success", "msg": detail_data.to_dict("records")[0]}
    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
    finally:
        try:
            conn_an.close()
        except:
            pass

# 修改数据
@operateconbp.route("/update", methods=["POST"])
def update_operate_detail_data():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 12:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
            operate_id = request.json['id']
            # 地址
            address = request.json['address']
            # 授权编码
            authnumber = request.json['authnumber']
            create_time = request.json['create_time']
            # crm状态
            crm = request.json['crm']
            # name = request.json['name']
            # 运营中心名称
            operatename = request.json['operatename']
            # 手机号
            phone = request.json['phone']
            # 状态
            status = request.json['status']
            # 统一信用编码
            unifiedsocial = request.json['unifiedsocial']
            # unionid = request.json['unionid']
        except:
            # 参数名错误
            logger.info(traceback.format_exc())
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_an = direct_get_conn(analyze_mysql_conf)
        if not conn_an:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        # 数据对比
        oringinal_data_sql = '''select telephone phone, operatename, address, authnumber, unifiedsocial, crm, status from lh_analyze.operationcenter where id=%s''' % operate_id
        oringinal_data = pd.read_sql(oringinal_data_sql, conn_an)
        # 进行数据判断
        # 修改手机号，其余unionid与姓名从crm_user表拿。如果用户不存在，则不让修改，统一信用编码为唯一，不能重复
        crm_data_sql = '''select unionid, if(`name` != "",`name`,if(nickname is not null,nickname,"")) name from lh_analyze.crm_user where phone=%s''' % phone
        crm_data = pd.read_sql(crm_data_sql, conn_an)
        # 判断用户是否存在
        if crm_data.shape[0] == 0:
            return {"code": "11029", "status": "failed", "msg": message["11029"]}
        new_unionid = crm_data['unionid'].values[0]
        new_name = crm_data['name'].values[0]
        update_data_sql = '''update lh_analyze.operationcenter set telephone=%s, unionid=%s, name=%s, operatename=%s, address=%s, authnumber=%s, unifiedsocial=%s, create_time=%s, crm=%s, status=%s where id=%s'''
        update_list = [phone, new_unionid, new_name, operatename, address, authnumber, unifiedsocial, create_time, crm, status, operate_id]
        logger.info(update_list)
        cursor = conn_an.cursor()
        cursor.execute(update_data_sql, update_list)
        conn_an.commit()

        # 日志功能
        #日志列表
        log_list = []
        if phone != oringinal_data['phone'].values[0]:
            msg = '手机号由 %s 改 %s' % (oringinal_data['phone'].values[0], phone)
            log_list.append(msg)
        if operatename != oringinal_data['operatename'].values[0]:
            msg = '公司名称由 %s 改 %s' % (oringinal_data['operatename'].values[0], operatename)
            log_list.append(msg)
        if address != oringinal_data['address'].values[0]:
            msg = '公司地址由 %s 改 %s' % (oringinal_data['address'].values[0], address)
            log_list.append(msg)
        if authnumber != oringinal_data['authnumber'].values[0]:
            msg = '授权编码由 %s 改 %s' % (oringinal_data['authnumber'].values[0], authnumber)
            log_list.append(msg)
        if unifiedsocial != oringinal_data['unifiedsocial'].values[0]:
            msg = '信用代码由 %s 改 %s' % (oringinal_data['unifiedsocial'].values[0], unifiedsocial)
            log_list.append(msg)
        if crm != oringinal_data['crm'].values[0]:
            crm_status_type = {
                1: "支持",
                0: "不支持"
            }
            msg = 'crm状态由 %s 改 %s' % (crm_status_type.get(oringinal_data['crm'].values[0]), crm_status_type.get(crm))
            log_list.append(msg)
        if status != oringinal_data['status'].values[0]:
            status_type = {
                1: "正常",
                2: "关闭"
            }
            msg = '账户状态由 %s 改 %s' % (status_type.get(oringinal_data['status'].values[0]), status_type.get(status))
            log_list.append(msg)
        if log_list:
            insert_sql = '''insert into sys_log (user_id,log_url,log_req,log_action,remark) values (%s,%s,%s,%s,%s)'''
            params = []
            params.append(user_id)
            params.append("/operatecon/update")
            params.append(json.dumps(request.json))
            params.append("修改运营中心数据数据")
            # params.append(json.dumps(compare,ensure_ascii=False))
            params.append("<br>".join(log_list))
            logger.info(params)
            cursor.execute(insert_sql,params)
            conn_an.commit()

        return {"code": "0000", "status": "success", "msg": "更新成功"}
    except:
        # 回滚
        conn_an.rollback()
        logger.exception(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_an.close()
        except:
            pass
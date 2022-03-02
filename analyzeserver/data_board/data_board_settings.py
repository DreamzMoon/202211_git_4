# -*- coding: utf-8 -*-
# @Time : 2022/3/1  18:38
# @Author : shihong
# @File : .py
# --------------------------------------
''' 数据看板配置'''
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import pandas as pd
import datetime
from analyzeserver.user.sysuser import check_token

boardbp = Blueprint("board", __name__, url_prefix='/board')

# 看板配置查看
@boardbp.route('/settings/check', methods=["GET"])
def board_settings_check():
    try:
        try:
            logger.info(request.json)
            # token校验
            token = request.headers["Token"]
            user_id = request.args["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        board_settings_sql = '''
            select market_type, status, time_type, if(start_time is not null or start_time!='', date_format(start_time, "%Y-%m-%d %H:%i:%S"), '') start_time,
            if(end_time is not null or end_time!='', date_format(end_time, "%Y-%m-%d %H:%i:%S"), '') end_time, inside_publish_phone, inside_recovery_phone
            from lh_analyze.data_board_settings where del_flag=0
        '''
        board_settings_data = pd.read_sql(board_settings_sql, conn_analyze)
        board_settings_data['inside_publish_phone'] = board_settings_data['inside_publish_phone'].apply(lambda x: json.loads(x) if x else [])
        board_settings_data['inside_recovery_phone'] = board_settings_data['inside_recovery_phone'].apply(lambda x: json.loads(x) if x else [])
        return {"code": "0000", "status": "success", "msg": board_settings_data.to_dict("records")}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass

# 看板数据设置
@boardbp.route('/settings/edit', methods=["POST"])
def board_settings_edit():
    try:
        try:
            logger.info(request.json)
            # 参数个数错误
            if len(request.json) != 8:
                return {"code": "10004", "status": "failed", "msg": message["10004"]}

            # token校验
            token = request.headers["Token"]
            user_id = request.json["user_id"]

            if not user_id and not token:
                return {"code": "10001", "status": "failed", "msg": message["10001"]}

            check_token_result = check_token(token, user_id)
            if check_token_result["code"] != "0000":
                return check_token_result

            market_type = request.json['market_type'] # 市场类型
            status = request.json['status'] # 看板状态
            time_type = request.json['time_type'] # 时间段设置
            start_time = request.json['start_time'] if request.json['start_time'] else None# 起始时间
            end_time = request.json['end_time'] if request.json['end_time'] else None # 结束时间
            inside_publish_phone = request.json['inside_publish_phone'] # 内部发布手机号
            inside_recovery_phone = request.json['inside_recovery_phone'] # 内部回收手机号
        except Exception as e:
            # 参数名错误
            logger.error(e)
            return {"code": "10009", "status": "failed", "msg": message["10009"]}
        if time_type == 1 and not start_time and not end_time:
            return {"code": "10016", "status": "failed", "msg": message["10016"]}
        # 数据库连接
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}

        inside_publish_phone = [] if len(inside_publish_phone) == 0 else inside_publish_phone
        inside_recovery_phone = [] if len(inside_recovery_phone) == 0 else inside_recovery_phone

        # 原活动数据
        old_data_sql = '''select market_type, status, time_type, if(start_time is not null or start_time!='', date_format(start_time, "%Y-%m-%d %H:%i:%S"), '') start_time,
            if(end_time is not null or end_time!='', date_format(end_time, "%Y-%m-%d %H:%i:%S"), '') end_time, inside_publish_phone, inside_recovery_phone
            from lh_analyze.data_board_settings where market_type = {}'''.format(market_type)
        old_settings = pd.read_sql(old_data_sql, conn_analyze)
        old_settings = old_settings.to_dict("records")[0]

        old_status = old_settings["status"]
        old_time_type = old_settings['time_type']
        old_start_time = old_settings["start_time"] if old_settings["start_time"] else None
        old_end_time = old_settings["end_time"] if old_settings["end_time"] else None
        old_publish_phone = json.loads(old_settings["inside_publish_phone"]) if old_settings["inside_publish_phone"] else []
        old_recovery_phone = json.loads(old_settings["inside_recovery_phone"]) if old_settings["inside_recovery_phone"] else []

        compare = []
        if status != old_status:
            status_map = {
                0: "关闭",
                1: "开启"
            }
            compare.append("看板状态由 %s 变更为 %s" % (status_map.get(old_status), status_map.get(status)))
        if time_type != old_time_type:
            time_type_map = {
                0: "默认当前时间",
                1: "自定义时间"
            }
            compare.append("时间段设置由 %s 变更为 %s" % (time_type_map.get(old_time_type), time_type_map.get(time_type)))
        if old_start_time != start_time:
            compare.append("开始时间 %s 变更为 %s" % (old_start_time, start_time))
        if old_end_time != end_time:
            compare.append("结束时间 %s 变更为 %s" % (old_end_time, end_time))
        if old_publish_phone != inside_publish_phone:
            compare.append("内部发布手机号 %s 变更为 %s" % (old_publish_phone, inside_publish_phone))
        if old_recovery_phone != inside_recovery_phone:
            compare.append("内部回收手机号 %s 变更为 %s" % (old_recovery_phone, inside_recovery_phone))

        update_sql = '''
            update lh_analyze.data_board_settings set status=%s, time_type=%s, start_time=%s, end_time=%s, inside_publish_phone=%s, inside_recovery_phone=%s where market_type = %s
        '''
        with conn_analyze.cursor() as cursor:
            cursor.execute(update_sql, (
            status, time_type, start_time, end_time, json.dumps(inside_publish_phone), json.dumps(inside_recovery_phone), market_type))

            if compare:
                market_type_map = {
                    1: "个人名片转让市场数据看板配置",
                    2: "商业名片二手市场数据看板配置"
                }
                compare.insert(0, '%s:' % market_type_map.get(market_type))
                insert_sql = '''insert into sys_log (user_id,log_url,log_req,log_action,remark) values (%s,%s,%s,%s,%s)'''
                params = []
                params.append(user_id)
                params.append("/lh/board/settings/edit")
                params.append(json.dumps(request.json))
                params.append("修改活动数据")
                params.append("<br>".join(compare))
                logger.info(params)
                cursor.execute(insert_sql, params)

        conn_analyze.commit()
        return {"code": "0000", "status": "success", "msg": '更新成功'}
    except:
        conn_analyze.rollback()
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "failed", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass

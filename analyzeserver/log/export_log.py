# -*- coding: utf-8 -*-
# @Time : 2022/2/16  14:51
# @Author : shihong
# @File : .py
# --------------------------------------
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

exportbp = Blueprint('export', __name__, url_prefix='/export')


# 查看日志
@exportbp.route('/check', methods=["POST"])
def check_export_log():
    try:
        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        page = request.json.get("page")
        size = request.json.get("size")

        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "failed", "msg": message["10002"]}
        cursor_analyze = conn_analyze.cursor()

        count_sql = '''select count(*) count from export_log'''
        cursor_analyze.execute(count_sql)
        count = cursor_analyze.fetchone()[0]

        export_log_sql = '''
            select sys.username, date_format(ex.log_time, "%%Y-%%m-%%d %%H:%%i:%%s") log_time, ex.log_url from export_log ex
            left join sys_user sys
            on ex.user_id = sys.id
            order by log_time desc
            limit %s,%s
        ''' % ((page-1)*size,size)
        export_df = pd.read_sql(export_log_sql, conn_analyze)

        return {"code": "0000", "status": "success", "msg": export_df.to_dict("records"), "count": count}
    except:
        logger.info(traceback.format_exc())
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass

# 日志记录
@exportbp.route('/table', methods=["POST"])
def export_table():
    try:
        token = request.headers["Token"]
        user_id = request.json.get("user_id")
        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        # 数据路径
        log_url = request.json.get("log_url")
        # 请求参数
        log_req = request.json.get("log_req")
        # 导出表名
        table_name = request.json.get("table_name")
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        if not conn_analyze:
            return {"code": "10002", "status": "success", "msg": message["10002"]}
        cursor_analyze = conn_analyze.cursor()
        remark = '导出数据'
        log_action = '导出了 %s' % table_name
        # 数据插入
        insert_sql = '''insert into export_log (user_id, log_url, log_req, log_action, remark) values (%s, %s, %s, %s, %s)'''
        cursor_analyze.execute(insert_sql, (user_id, log_url, json.dumps(log_req), log_action, remark))
        conn_analyze.commit()
        return {"code": "0000", "status": "success", "msg": "更新成功"}
    except:
        logger.info(traceback.format_exc())
        conn_analyze.rollback()
        return {"code": "10000", "status": "success", "msg": message["10000"]}
    finally:
        try:
            conn_analyze.close()
        except:
            pass
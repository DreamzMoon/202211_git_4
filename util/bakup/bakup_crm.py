# -*- coding: utf-8 -*-

# @Time : 2021/10/27 9:58

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : bakup_crm.py

import pymysql
import time
import datetime
import os
import traceback
import oss2
import os
from config import *
from util.help_fun import *

logger.info("ddddd")
auth = oss2.Auth('LTAI4FmMu531TSvt6jJPJejw', 'cl5uauXWgkbqQclu9KRmpcReybLOWO')
bucket = oss2.Bucket(auth, 'https://oss-cn-beijing.aliyuncs.com', 'luke-analyze')


backup_dir = r'.'
backup_date = time.strftime("%Y%m%d")
#查出MySQL中所有的数据库名称
select_sql = "show databases"
try:
    conn = direct_get_conn(crm_mysql_conf)
    logger.info(conn)
    cur = conn.cursor()
    cur.execute(select_sql)
    alldatabase = cur.fetchall()
    logger.info(alldatabase)
    logger.info('The database backup to start! %s' %time.strftime('%Y-%m-%d %H:%M:%S'))
    for db in alldatabase:
        db_name = db["Database"]
        if db["Database"].find("luke") > -1:
            pass
        else:
            continue
        fileName = '%s/%s_%s.sql' %(backup_dir,backup_date,db_name)
        if os.path.exists(fileName):
            os.remove(fileName)
        logger.info(backup_dir)

        logger.info("%s--开始备份" %db_name)
        # 本地不需要80
        sql_cmd = "mysqldump -h%s -u%s -p%s -P %s %s --default_character-set=%s > %s/%s.sql" %(crm_mysql_conf["host"],crm_mysql_conf["user"],crm_mysql_conf["password"],crm_mysql_conf["port"],db_name,crm_mysql_conf["charset"],backup_dir,db_name)
        logger.info(sql_cmd)
        os.system(sql_cmd)
        # os.system("mysqldump80 -h%s -u%s -p%s -P %s %s --default_character-set=%s > %s/%s.sql" %(crm_mysql_conf["host"],crm_mysql_conf["user"],crm_mysql_conf["password"],crm_mysql_conf["port"],db_name,crm_mysql_conf["charset"],backup_dir,db_name))
        logger.info("%s--备份完成" %db_name)
        logger.info("------------------------------")


    #打包
    os.system("tar -zcvf crm.tar.gz ./*.sql")

    # 上传oss

    upload_filename = "crm.tar.gz"
    # bucket.put_object_from_file('bakup/crawlcard/signal/%s/%s' %(backup_date,upload_filename), backup_dir+"/"+upload_filename)
    bucket.put_object_from_file('sqlbakup/%s/%s' %(backup_date,upload_filename), "./crm.tar.gz")


    logger.info("上传oss完成")
    logger.info("-----------------------------------------")
    logger.info('The database backup success! %s' %time.strftime('%Y-%m-%d %H:%M:%S'))
#异常
except Exception as e:
    logger.exception(traceback.format_exc())
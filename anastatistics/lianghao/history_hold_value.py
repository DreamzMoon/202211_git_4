# -*- coding: utf-8 -*-

# @Time : 2021/10/25 14:02

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : history_hold_value.py

'''
历史个人靓号持有的估值 按天按人统计 统计今天的 这个要实时的更新
每一个小时跑一次统计一条数据
'''
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

conn_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
sql = '''
SELECT
  hold_user_id lh_user_id,
	hold_phone phone,
	sum( c.sum_price ) total_money,
	sum( type_count ) hold_count
FROM
	(
	SELECT
		* 
	FROM
		(
		SELECT
			a.*,
			lh_config_guide.guide_price,
			lh_config_guide.guide_price * a.type_count sum_price,
			lh_config_guide.date 
		FROM
			(
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_0 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and create_time <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_1 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_2 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_3 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_4 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_5 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_6 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 )
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_7 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_8 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_9 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_a 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_b 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_c 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_d 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_e 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id UNION ALL
			SELECT
			  hold_user_id,
				hold_nick_name,
				hold_phone,
				pretty_type_id,
				count(*) type_count 
			FROM
				lh_pretty_hold_f 
			WHERE
				del_flag = 0 
				AND `status` IN ( 0, 2, 1 ) 
				and DATE_FORMAT(create_time,"%Y%m%d") <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			GROUP BY
				hold_phone,
				pretty_type_id 
			) a
			LEFT JOIN lh_config_guide ON a.pretty_type_id = lh_config_guide.pretty_type_id 
		WHERE
			lh_config_guide.date <= date_add(CURRENT_DATE(),INTERVAL -1 day)
			AND lh_config_guide.del_flag = 0 
		GROUP BY
			a.hold_phone,
			lh_config_guide.pretty_type_id,
			lh_config_guide.date 
		ORDER BY
			lh_config_guide.date DESC 
		) b 
	GROUP BY
		b.hold_phone,
		b.pretty_type_id 
	) c 
GROUP BY
	hold_phone 
having hold_phone != ""
ORDER BY
	total_money DESC
'''

try:
    datas = pd.read_sql(sql, conn_read)
    logger.info(datas)
    logger.info("-------")
    conn_read.close()

    logger.info(datas.shape)
    #准备进入数据拼接获取用户信息 获取crm拼接 数据要一条一条查不然有出入 数据匹配不对
    crm_mysql_conf["db"] = "luke_sincerechat"
    conn_crm = direct_get_conn(crm_mysql_conf)
    logger.info(conn_crm)
    if not conn_crm:
        exit()

    crm_datas = ""
    with conn_crm.cursor() as cursor:
        sql = '''select id unionid,`name`,nickname,phone from user where phone is not null or phone != ""'''
        cursor.execute(sql)
        crm_datas = cursor.fetchall()
        crm_datas = pd.DataFrame(crm_datas)

    conn_crm.close()

    ok_datas = datas.merge(crm_datas, how="left", on="phone")

    ok_datas.fillna("", inplace=True)
    # ok_datas["statistic_time"] = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
    ok_datas["statistic_time"] = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
    # 删除unionid为空
    last_datas = ok_datas.drop(ok_datas[ok_datas["unionid"] == ""].index)

    # 因为要实时调用 所以要走查询更新 或者查询插入操作
    conn_rw = ssh_get_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
    last_datas = last_datas.values.tolist()
    # logger.info(last_datas[0])
    logger.info(len(last_datas))

    # # 批量插入
    with conn_rw.cursor() as cursor:
        insert_sql = '''insert into lh_history_hold_value (lh_user_id,phone,total_money,hold_count,unionid,name,nickname,statistic_time) values (%s,%s,%s,%s,%s,%s,%s,%s)'''
        cursor.executemany(insert_sql,last_datas)
        conn_rw.commit()

    conn_rw.close()
    logger.info("入库成功")
except:
    logger.exception(traceback.format_exception())
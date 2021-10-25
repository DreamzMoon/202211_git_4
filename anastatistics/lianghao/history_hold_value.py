# -*- coding: utf-8 -*-

# @Time : 2021/10/25 14:02

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : history_hold_value.py

'''
历史个人靓号持有的估值 按天按人统计 统计今天的
每天00:30:00 跑
'''

import sys
sys.path.append(".")
sys.path.append("../")
sys.path.append("../../")
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
			GROUP BY
				hold_phone,
				pretty_type_id 
			) a
			LEFT JOIN lh_config_guide ON a.pretty_type_id = lh_config_guide.pretty_type_id 
		WHERE
			lh_config_guide.date <= CURRENT_DATE 
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

    #准备进入数据拼接获取用户信息 获取crm拼接 数据要一条一条查不然有出入 数据匹配不对
    crm_mysql_conf["db"] = "luke_sincerechat"
    conn_crm = direct_get_conn(crm_mysql_conf)
    logger.info(conn_crm)
    if not conn_crm:
        exit()

    with conn_crm.cursor() as cursor:
        for i in range(datas.shape[0]):
            logger.info("i:%s" %i)
            sql = '''select id unionid,`name`,nickname  from user where phone = %s'''
            phone = datas.loc[i,"phone"]
            logger.info("phone:%s" %phone)
            cursor.execute(sql,(phone))
            data = cursor.fetchone()
            logger.info(data)
            if data:
                datas.loc[i,["unionid","name","nickname"]] = data.values()
                logger.info(datas)
            else:
                pass


    conn_crm.close()

    datas.fillna("",inplace=True)
    datas["statistic_time"] = [(date.today()).strftime("%Y-%m-%d %H:%M:%S")]*len(datas)
    #删除unionid为空
    last_datas = datas.drop(datas[datas["unionid"]==""].index)


    # 通过sqlclchemy创建的连接无需关闭
    logger.info("准备写入的数据")
    logger.info(datas)
    conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
    datas.to_sql("lh_history_hold_value",con=conn_rw,if_exists="append",index=False)
    logger.info("写入成功")
except:
    logger.exception(traceback.format_exception())
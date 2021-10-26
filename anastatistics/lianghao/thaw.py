# -*- coding: utf-8 -*-
'''
解冻
'''
import sys, os
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
import traceback
from datetime import date
from util.help_fun import *


def thaw_count_data(mode='update'):
    try:
        # 重复订单查询
        conn_lh_read = ssh_get_conn(lianghao_ssh_conf, lianghao_mysql_conf)
        if mode == 'init':
            logger.info('进行表数据初始化')
            thaw_count_sql = '''
                select statistic_time, sum(thaw_count) thaw_count from
            (
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_0
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_1
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_2
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_3
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_4
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_5
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_6
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_7
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_8
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_9
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_a
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_b
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_c
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_d
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_e
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
                union all
                (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_f
                where del_flag = 0 and thaw_time < curdate()
                group by statistic_time)
            ) t1
        group by statistic_time
        having statistic_time is not null
        and statistic_time != '0000-00-00'
        order by statistic_time desc
        '''
        elif mode != 'update':
            return logger.info('模式错误')
        else:
            logger.info('进行数据更新')
            thaw_count_sql = '''
                select statistic_time, sum(thaw_count) thaw_count from
                (
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_0
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_1
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_2
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_3
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_4
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_5
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_6
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_7
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_8
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_9
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_a
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_b
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_c
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_d
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_e
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                    union all
                    (select date_format(thaw_time, '%Y-%m-%d') as statistic_time, count(*) as thaw_count from lh_pretty_hold_f
                    where del_flag = 0 and thaw_time < curdate() and thaw_time >= date_sub(curdate(), interval 1 day)
                    group by statistic_time)
                ) t1
                group by statistic_time
            '''
        logger.info('解冻靓号统计查询')
        count_data = pd.read_sql(thaw_count_sql, conn_lh_read)
        conn_lh_read.close()
        logger.info('解冻靓号统计查询结束')

        logger.info("准备写入")
        # 通过sqlclchemy创建的连接无需关闭
        conn_rw = ssh_get_sqlalchemy_conn(lianghao_ssh_conf,lianghao_rw_mysql_conf)
        logger.info(conn_rw)
        count_data.to_sql("lh_thaw_count", con=conn_rw, if_exists="append", index=False)
        logger.info("解冻靓号统计写入成功")
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('解冻靓号统计写入失败')


thaw_count_data(mode='init')

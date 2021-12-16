# -*- coding: utf-8 -*-
# @Time : 2021/12/15  10:18
# @Author : shihong
# @File : .py
# --------------------------------------

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
import pandas as pd
from analyzeserver.common import *
import numpy as np
import time

# 发布对应转让中
def public_lh():
    try:
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        sql = '''
        select day_time,hold_phone,sum(public_count) public_count,sum(public_price) public_price from (select DATE_FORMAT(create_time,"%Y-%m-%d") day_time,sell_phone hold_phone, sum(count) public_count,sum(total_price) public_price from lh_sell where del_flag = 0 and `status` = 0 group by day_time, hold_phone
        union all
        select DATE_FORMAT(lsrd.update_time,"%Y-%m-%d") day_time,lsr.retail_user_phone hold_phone,count(*) public_count,sum(lsrd.unit_price) public_price from lh_sell_retail lsr left join lh_sell_retail_detail lsrd
        on lsr.id = lsrd.retail_id where lsr.del_flag = 0 and lsrd.retail_status = 0
        group by day_time,hold_phone ) t group by day_time,hold_phone having day_time != current_date order by day_time desc
        '''
        public_data = pd.read_sql(sql,conn_lh)
        return True,public_data
    except:
        return False,""
    finally:
        conn_lh.close()

def use_lh():
    try:
        conn_lh = direct_get_conn()
        sql = '''
                select DATE_FORMAT(b.statistic_time,"%Y-%m-%d") day_time,hold_phone,sum(unit_price) total_price from (
        (
        select hold_0.hold_phone hold_phone,if(hold_0.unit_price,hold_0.unit_price,0) unit_price,hold_0.pretty_id pretty_id from lh_pretty_hold_0 hold_0 where hold_0.del_flag = 0 and (hold_0.`status` = 1 or hold_0.is_open_vip=1) union all 
        select hold_1.hold_phone hold_phone,if(hold_1.unit_price,hold_1.unit_price,0) unit_price,hold_1.pretty_id pretty_id from lh_pretty_hold_1 hold_1 where hold_1.del_flag = 0 and (hold_1.`status` = 1 or hold_1.is_open_vip=1) union all 
        select hold_2.hold_phone hold_phone,if(hold_2.unit_price,hold_2.unit_price,0) unit_price,hold_2.pretty_id pretty_id from lh_pretty_hold_2 hold_2 where hold_2.del_flag = 0 and (hold_2.`status` = 1 or hold_2.is_open_vip=1)  union all 
        select hold_3.hold_phone hold_phone,if(hold_3.unit_price,hold_3.unit_price,0) unit_price,hold_3.pretty_id pretty_id from lh_pretty_hold_3 hold_3 where hold_3.del_flag = 0 and (hold_3.`status` = 1 or hold_3.is_open_vip=1)  union all 
        select hold_4.hold_phone hold_phone,if(hold_4.unit_price,hold_4.unit_price,0) unit_price,hold_4.pretty_id pretty_id from lh_pretty_hold_4 hold_4 where hold_4.del_flag = 0 and (hold_4.`status` = 1 or hold_4.is_open_vip=1)  union all 
        select hold_5.hold_phone hold_phone,if(hold_5.unit_price,hold_5.unit_price,0) unit_price,hold_5.pretty_id pretty_id from lh_pretty_hold_5 hold_5 where hold_5.del_flag = 0 and (hold_5.`status` = 1 or hold_5.is_open_vip=1)  union all 
        select hold_6.hold_phone hold_phone,if(hold_6.unit_price,hold_6.unit_price,0) unit_price,hold_6.pretty_id pretty_id from lh_pretty_hold_6 hold_6 where hold_6.del_flag = 0 and (hold_6.`status` = 1 or hold_6.is_open_vip=1)  union all 
        select hold_7.hold_phone hold_phone,if(hold_7.unit_price,hold_7.unit_price,0) unit_price,hold_7.pretty_id pretty_id from lh_pretty_hold_7 hold_7 where hold_7.del_flag = 0 and (hold_7.`status` = 1 or hold_7.is_open_vip=1)  union all 
        select hold_8.hold_phone hold_phone,if(hold_8.unit_price,hold_8.unit_price,0) unit_price,hold_8.pretty_id pretty_id from lh_pretty_hold_8 hold_8 where hold_8.del_flag = 0 and (hold_8.`status` = 1 or hold_8.is_open_vip=1)  union all 
        select hold_9.hold_phone hold_phone,if(hold_9.unit_price,hold_9.unit_price,0) unit_price,hold_9.pretty_id pretty_id from lh_pretty_hold_9 hold_9 where hold_9.del_flag = 0 and (hold_9.`status` = 1 or hold_9.is_open_vip=1)  union all 
        select hold_a.hold_phone hold_phone,if(hold_a.unit_price,hold_a.unit_price,0) unit_price,hold_a.pretty_id pretty_id from lh_pretty_hold_a hold_a where hold_a.del_flag = 0 and (hold_a.`status` = 1 or hold_a.is_open_vip=1)  union all 
        select hold_b.hold_phone hold_phone,if(hold_b.unit_price,hold_b.unit_price,0) unit_price,hold_b.pretty_id pretty_id from lh_pretty_hold_b hold_b where hold_b.del_flag = 0 and (hold_b.`status` = 1 or hold_b.is_open_vip=1)  union all 
        select hold_c.hold_phone hold_phone,if(hold_c.unit_price,hold_c.unit_price,0) unit_price,hold_c.pretty_id pretty_id from lh_pretty_hold_c hold_c where hold_c.del_flag = 0 and (hold_c.`status` = 1 or hold_c.is_open_vip=1)  union all 
        select hold_d.hold_phone hold_phone,if(hold_d.unit_price,hold_d.unit_price,0) unit_price,hold_d.pretty_id pretty_id from lh_pretty_hold_d hold_d where hold_d.del_flag = 0 and (hold_d.`status` = 1 or hold_d.is_open_vip=1)  union all 
        select hold_e.hold_phone hold_phone,if(hold_e.unit_price,hold_e.unit_price,0) unit_price,hold_e.pretty_id pretty_id from lh_pretty_hold_e hold_e where hold_e.del_flag = 0 and (hold_e.`status` = 1 or hold_e.is_open_vip=1)  union all 
        select hold_f.hold_phone hold_phone,if(hold_f.unit_price,hold_f.unit_price,0) unit_price,hold_f.pretty_id pretty_id from lh_pretty_hold_f hold_f where hold_f.del_flag = 0 and (hold_f.`status` = 1 or hold_f.is_open_vip=1)
        ) a
        left join 
        (select min(use_time) statistic_time,pretty_id  from lh_bind_pretty_log  group by pretty_id) b on a.pretty_id = b.pretty_id
        ) 
        group by day_time,hold_phone
        order by day_time desc
        '''
        use_data = pd.read_sql(sql,conn_lh)
        return True,use_data
    except :
        logger.info(traceback.format_exc())
        return False,""
    finally:
        conn_lh.close()

# 转让持有总
def tran_hold():
    try:
        conn_lh = direct_get_conn()



        #可转让 持有
        sql = '''
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_0 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_1 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_2 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_3 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_4 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_5 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_6 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_7 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_8 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_9 where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_a where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_b where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_c where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_d where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_e where del_flag=0 and `status` in (0,1,2)   union all
        select hold_phone,pretty_type_id,thaw_time from lh_pretty_hold_f where del_flag=0 and `status` in (0,1,2) 
        '''
        data = pd.read_sql(sql,conn_lh)

    except:
        logger.info(traceback.format_exc())
        return False, ""
    finally:
        conn_lh.close()

if __name__ == "__main__":
    # logger.info(public_lh())
    use_lh()
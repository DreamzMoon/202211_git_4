# -*- coding: utf-8 -*-
# @Time : 2021/12/13  11:35
# @Author : shihong
# @File : .py
# --------------------------------------
# 用户每日订单数据统计报表---每天00:01分同步数据
# 获取用户信息
import sys, os, time
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from functools import reduce
from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback

# 数据库连接
def quota_data():
    try:
        conn_analyze = direct_get_conn(analyze_mysql_conf)
        conn_lh = direct_get_conn(lianghao_mysql_conf)
        if not conn_lh or not conn_analyze:
            return False, '数据库连接错误'
        cursor = conn_analyze.cursor()

        # 转让市场额度
        tran_market_sql = '''
            select t1.phone, t1.create_time, t1.last_login_time, if(t1.is_shop_label=0, 0, if(t1.is_market=1, t1.market_buy_limit, t2.market_buy_limit)) market_buy_limit from lh_pretty_client.lh_user t1
            left join
            lh_pretty_client.lh_config_grade t2
            on t1.business_grade_id=t2.id
            where t1.del_flag=0 and t1.phone is not null and t1.phone !=''
        '''
        tran_market_df = pd.read_sql(tran_market_sql, conn_lh)
        logger.info('读取转让市场额度数据')
        # 订单数据
        order_sql = '''
            select phone, sell_phone, pretty_price, total_price,status, type from lh_order where del_flag=0 and status in (0,1) and type in (1,4)
        '''
        order_df = pd.read_sql(order_sql, conn_lh)
        logger.info('读取订单数据')
        # 出售数据
        # publish_sql = '''
        #     select sell_phone phone, count publish_count, total_price publish_total_price, if(up_time is not null,up_time,create_time) up_time from lh_sell where del_flag=0 and status=2 and sell_phone is not null and sell_phone != ''
        # '''
        # publish_df = pd.read_sql(publish_sql, conn_lh)

        table_label_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 'a', 'b', 'c', 'd', 'e', 'f']
        # 持有表数据
        hold_sql = '''
            select hold_phone phone,unit_price from lh_pretty_hold_%s where `status` in (0,1,2) and pay_type != 0 and order_type in (1,4) and del_flag = 0
        '''
        # 查询官方
        inside_recovery_phone_sql = '''
            select inside_recovery_phone, inside_publish_phone from lh_analyze.data_board_settings where del_flag=0 and market_type=1
        '''
        # 官方上架与回收
        cursor.execute(inside_recovery_phone_sql)
        official_data = cursor.fetchone()
        inside_recovery_phone = json.loads(official_data[0]) if official_data[0] else []
        inside_publish_phone = json.loads(official_data[1]) if official_data[1] else []
        # 订单详情
        # order_detail_sql = '''
        #     select order_sn, pretty_account, unit_price from lh_order_detail_%s where del_flag=0
        # '''
        hold_df = []
        # order_detail_df = []
        for label in table_label_list:
            logger.info(label)
            hold_df.append(pd.read_sql(hold_sql % label, conn_lh))
            # order_detail_df.append(pd.read_sql(order_detail_sql % label, conn_lh))
        hold_df = pd.concat(hold_df, axis=0, ignore_index=True)
        # order_detail_df = pd.concat(order_detail_df, axis=0)

        df_list = []
        # 转让市场额度处理
        # tran_market_df['create_time'].fillna('', inplace=True)
        # tran_market_df['last_login_time'].fillna('', inplace=True)
        tran_market_df['market_buy_limit'].fillna(0, inplace=True)
        # 无限额度用户数据--当前额度也为无限额度
        max_money_user = tran_market_df.loc[tran_market_df['market_buy_limit'] == -1]
        max_money_user['surplus_money'] = max_money_user['market_buy_limit']
        # 剔除无限额度用户数据
        tran_market_df = tran_market_df.drop(tran_market_df[tran_market_df['market_buy_limit'] == -1].index,
                                             axis=0).reset_index(drop=True)

        # 已使用额度计算
        hold_df_group = hold_df.groupby('phone').agg({"unit_price": "sum"}).reset_index()
        # 处理中订单
        process_order_df = order_df[(order_df['status'] == 0) & (order_df['type'] == 1)].groupby('phone').agg(
            {"pretty_price": "sum"}).reset_index()
        use_money_df = hold_df_group.merge(process_order_df, how='left', on='phone')
        use_money_df.fillna(0, inplace=True)
        use_money_df['use_money'] = use_money_df['unit_price'] + use_money_df['pretty_price']
        use_money_df.drop(['unit_price', 'pretty_price'], axis=1, inplace=True)

        fina_df = tran_market_df.merge(use_money_df, how='left', on='phone')
        max_money_user = max_money_user.merge(use_money_df, how='left', on='phone')
        fina_df['use_money'].fillna(0, inplace=True)
        max_money_user['use_money'].fillna(0, inplace=True)

        fina_df['surplus_money'] = fina_df['market_buy_limit'] - fina_df['use_money']
        # 合并无限额度用户
        fina_df = pd.concat([fina_df, max_money_user], axis=0, ignore_index=True)
        df_list.append(fina_df)

        # 官方采购
        official_buy_df = order_df[(order_df['sell_phone'].isin(inside_publish_phone)) & (order_df['status'] == 1)].groupby('phone').agg(
            {"total_price": "sum"}).reset_index()
        official_buy_df.columns = ['phone', 'official_buy_money']
        df_list.append(official_buy_df)
        # 市场采购
        market_buy_df = order_df[~(order_df['sell_phone'].isin(inside_publish_phone)) & (order_df['status'] == 1)].groupby('phone').agg(
            {"total_price": "sum"}).reset_index()
        market_buy_df.columns = ['phone', 'market_buy_money']
        df_list.append(market_buy_df)
        # 市场出售
        market_publish_df = order_df[~order_df['sell_phone'].isin(inside_publish_phone)].groupby('sell_phone').agg(
            {"total_price": "sum"}).reset_index()
        market_publish_df.columns = ['phone', 'market_publish_money']
        df_list.append(market_publish_df)

        fina_df = reduce(lambda left, right: pd.merge(left, right, how='left', on=['phone']), df_list)
        fina_df['addtime'] = datetime.datetime.now().strftime("%Y-%m-%d")
        for column in [columns for columns in fina_df.columns if 'money' in columns]:
            fina_df[column].fillna(0, inplace=True)

        # 当前时间年月
        year_month_time = datetime.datetime.now().strftime("%Y%m")

        show_tables = '''show tables'''
        tables_df = pd.read_sql(show_tables, conn_analyze)
        tables_df.columns = ['table_name']
        table_name = 'quota_' + year_month_time
        exists_table = tables_df[tables_df['table_name'] == table_name].shape[0]

        conn_an = pd_conn(analyze_mysql_conf)
        if exists_table == 0: # 创建表格
            logger.info('创建表格')
            create_table_sql = '''
                CREATE TABLE `%s` (
                `phone` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '手机号',
                `create_time` timestamp NULL DEFAULT NULL COMMENT '注册时间',
                `last_login_time` timestamp NULL DEFAULT NULL COMMENT '最后登录时间',
                `market_buy_limit` decimal(20,2) DEFAULT '0.00' COMMENT '转让市场额度(-1为无限额度)',
                `use_money` decimal(20,2) DEFAULT '0.00' COMMENT '已使用额度',
                `surplus_money` decimal(20,2) DEFAULT '0.00' COMMENT '当前额度',
                `official_buy_money` decimal(20,2) DEFAULT '0.00' COMMENT '渠道采购额度',
                `market_buy_money` decimal(20,2) DEFAULT '0.00' COMMENT '市场采购金额',
                `market_publish_money` decimal(20,2) DEFAULT '0.00' COMMENT '市场出售金额',
                `addtime` date DEFAULT NULL COMMENT '入库时间'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
            ''' % table_name
            cursor.execute(create_table_sql)
            conn_analyze.commit()
        logger.info('插入数据')
        fina_df.to_sql(table_name, con=conn_an, if_exists="append", index=False)
        return True, '插入成功'
    except Exception as e:
        logger.error(traceback.format_exc())
        conn_analyze.rollback()
        conn_lh.close()
        conn_analyze.close()
        return False, e


if __name__ == '__main__':
    start_time = time.time()
    result = quota_data()
    if not result:
        logger.error(result[1])
    else:
        logger.info(result[1])
    logger.info(time.time() - start_time)

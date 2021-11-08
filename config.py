# -*- coding: utf-8 -*-

# @Time : 2021/9/27 15:37

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : config.py

#设置日志等级
import pymysql
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)

from env import *

if ENV == "test":
    # 资讯测试
    zx_api_url = 'http://xsnews.com/open/content/python'
    zx_api_access_key = 'R4NoCsK4qMwvUisGW5o0odCr7GA2D9Yn' # access-key
    zx_type_id = 14
    card_news_category_ids = 55  # 卡新闻栏目id
    intergral_category_ids = 56  # 积分活动栏目id
    discount_category_ids = 'xx'  # 优惠资讯栏目id--测试未给栏目
    financil_activity_category_ids = 'xx'  # 理财生活栏目id--测试未给栏目
elif ENV == "pro":
    # 资讯正式
    zx_api_url = 'http://xs.lkkjjt.com/open/content/python'  # api_url
    zx_api_access_key = '2M4FqX5Z7IaQX1Q88c7cM73uspX4qGVk'  # apiaccess-key
    zx_type_id = 15
    card_news_category_ids = 86  # 卡新闻栏目id
    intergral_category_ids = 87  # 积分活动栏目id
    discount_category_ids = 88  # 优惠资讯栏目id--测试未给栏目
    financil_activity_category_ids = 89  # 理财生活栏目id--测试未给栏目
else:
    pass

# 本地的先做测试 目前的redis要配置通道还未封装
redis_host = "127.0.0.1"
redis_port = 6379
redis_password = ""
# redis_password = "_3:wOk!:)W]sak0EcWYYang"


yun_redis_host = "r-uf63qf6ypl3joxnq3l.redis.rds.aliyuncs.com"
yun_redis_port = 6379
yun_redis_password = "operation_analyze:YdusaSKHwUBMEYe6"
yun_redis_db = 71

lianghao_ssh_conf = {
    "host":"47.117.125.39",
    "ssh_username":"office",
    "ssh_password":"luke2020",
    "port":22
}

#公共模块
lianghao_mysql_conf = {
  "host": 'pc-uf6p512w5q51z3k72.rwlb.rds.aliyuncs.com',
  "port": 3306,
  "user": 'lh_read',
  "password": 'fBaVM4MMS8Myx9g6',
  "db": 'lh_pretty_client',
  "charset": "utf8mb4"
}

lianghao_rw_mysql_conf = {
  "host": 'pc-uf6p512w5q51z3k72.rwlb.rds.aliyuncs.com',
  "port": 3306,
  "user": 'lh_analyze',
  "password": 'Z4qEu8FHaphqMd6i',
  "db": 'lh_analyze',
  "charset": "utf8mb4"
}

expect3_mysql_conf = {
    "host":"pc-uf6p512w5q51z3k72.rwlb.rds.aliyuncs.com",
    "port":3306,
    "user":"expect3",
    "password":"beXhrrATb1PmJ541",
    "db":"expect3",
    "charset": "utf8mb4"
}


qw3_mysql_conf = {
    "host":"47.98.131.135",
    "port":43306,
    "user":"root",
    "password":"luke@20193306",
    "db":"expect_dev",
    "charset": "utf8mb4"
}

crm_mysql_conf = {
    "host":"47.97.115.105",
    "port":3308,
    "user":"copy",
    "password":"459915476",
    "db":"luke_crm",
    "charset":"utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}



#默认配置
ssh_conf = {}
mysql_conf = {}

# 七牛云
qn_access_key = '1-72BchKgU9rHSfMS4WzT9RIeecYWlPUewwPEJ_2'  # 七牛云AccessKey
qn_secre_key = '2fauZ0LZJoCZR--rtHEQeUu-Y8w4GbJEuhCKgohz'  # 七牛云SecretKey
qn_bucket = 'xiansuonews'  # 七牛云上传空间
qn_url = 'http://static.xiansuoapp.com/'  # 七牛云域名
# 资讯采集页数
zx_collect_page = 1
# 关键字
zx_pass_word_list = ['我爱卡', '微信搜索', '公众号', '动态横版二维码', '来源']



#服务端相应的数据
message = {
    "0": "数据同步成功",
    "10000": "网络异常，请稍后重试",
    "10001": "参数不能为空",
    "10002": "数据建立连接失败",
    "10003": "暂无crm数据，无法同步",
    "10004": "参数个数不对，请检查参数哦",
    "10005": "暂无禄可商务数据，无法同步",
    "10006": "获取crm信息失败",
    "10007": "订单类型不正确",
    "10008": "靓号数据库连接失败",
    "10009": "参数值错误，请检查参数",
    "10010": "未查询到相关数据，请重新查询",
    "10011": "匹配错误",
    "11009": "时间类型不正确",
    "11010": "该用户暂无法登录，请授权",
    "11011": "该用户已注册，请不要重新注册",
    "11012":"用户登录已失效，请重新登录",
    "11013":"用户令牌不正确",
    "11014":"暂无该用户",
    "11015":"暂无该运营中心的数据",
    "11016":"暂无用户数据",
    "11017":"时间选择不能超过一年",
    "11018":"时间选择不能超过一个月",
    "11019":"最后采购时间范围不能小于首次采购时间范围"
}

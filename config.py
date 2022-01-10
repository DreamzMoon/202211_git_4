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
from datetime import timedelta,date
from env import *
import warnings
warnings.filterwarnings('ignore')

if ENV == "test":
    # 资讯测试
    zx_api_url = 'http://xsnews.com/open/content/python'
    zx_api_access_key = 'R4NoCsK4qMwvUisGW5o0odCr7GA2D9Yn' # access-key
    zx_type_id = 14
    card_news_category_ids = 55  # 卡新闻栏目id
    intergral_category_ids = 56  # 积分活动栏目id
    discount_category_ids = 'xx'  # 优惠资讯栏目id--测试未给栏目
    financil_activity_category_ids = 'xx'  # 理财生活栏目id--测试未给栏目

    redis_host = "127.0.0.1"
    redis_port = 6379
    redis_password = "analyze.qq123"
    redis_db = 0

    analyze_mysql_conf = {
        "host": '127.0.0.1',
        "port": 3306,
        "user": 'root',
        "password": 'root',
        "db": 'lh_analyze',
        "charset": "utf8mb4"
    }
elif ENV == "pro":
    # 资讯正式
    zx_api_url = 'http://xs.lkkjjt.com/open/content/python'  # api_url
    zx_api_access_key = '2M4FqX5Z7IaQX1Q88c7cM73uspX4qGVk'  # apiaccess-key
    zx_type_id = 15
    card_news_category_ids = 86  # 卡新闻栏目id
    intergral_category_ids = 87  # 积分活动栏目id
    discount_category_ids = 88  # 优惠资讯栏目id--测试未给栏目
    financil_activity_category_ids = 89  # 理财生活栏目id--测试未给栏目


    # redis_host = "r-uf63qf6ypl3joxnq3l.redis.rds.aliyuncs.com"
    redis_host = "luke-rc-1.redis.rds.aliyuncs.com"
    redis_port = 6379
    redis_password = "operation_analyze:YdusaSKHwUBMEYe6"
    redis_db = 71

    analyze_mysql_conf = {
        "host": 'luke-mc.rwlb.rds.aliyuncs.com',
        "port": 3306,
        "user": 'lh_analyze',
        "password": 'Z4qEu8FHaphqMd6i',
        "db": 'lh_analyze',
        "charset": "utf8mb4"
    }
elif ENV == 'local':
    redis_host = "127.0.0.1"
    redis_port = 16379
    redis_password = "analyze.qq123"
    # redis_password = ""
    redis_db = 0

    analyze_mysql_conf = {
        "host": '127.0.0.1',
        "port": 13306,
        "user": 'root',
        "password": 'root',
        "db": 'lh_analyze',
        "charset": "utf8mb4"
    }
else:
    pass


analyze_pro = {
    "host": 'luke-mc.rwlb.rds.aliyuncs.com',
    "port": 3306,
    "user": 'lh_analyze',
    "password": 'Z4qEu8FHaphqMd6i',
    "db": 'lh_analyze',
    "charset": "utf8mb4"
}



lianghao_ssh_conf = {
    "host": "47.117.125.39",
    "ssh_username": "office",
    "ssh_password": "luke2020",
    "port": 22
}

# 公共模块
lianghao_mysql_conf = {
    "host": 'luke-mc.rwlb.rds.aliyuncs.com',
    "port": 3306,
    "user": 'lh_read',
    "password": 'fBaVM4MMS8Myx9g6',
    "db": 'lh_pretty_client',
    "charset": "utf8mb4"
}



crm_mysql_conf = {
    "host": "47.97.115.105",
    "port": 3308,
    "user": "copy",
    "password": "459915476",
    "db": "luke_crm",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

#靓号的配置数据
plat_lh_total_count_seven = 11331349


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

# 靓号已关闭运营中心数据是否在列表展示
check_close_operate = False

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
    "10012": "时间区间选则错误,起始时间大于结束时间",
    "10013": "时间区间选择错误",
    "10014": "时间标签类型错误",
    "10015": "时间选择不能超过24小时",
    "10016": "时间选择不能为空",
    "10017": "数据量过大,暂不支持导出",
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
    "11019":"最后采购时间范围不能小于首次采购时间范围",
    "11020":"开始时间不能大于结束时间",
    "11022":"用户已注销",
    "11024":"用户令牌不正确",
    "11025":"获取数据失败，请稍后重试",
    "11026":"用户不存在，请注册",
    "11027":"用户密码不正确，请检查后在登陆",
    "11028":"不正确的上级关系，不支持修改哦",
    "11029":"系统暂无该用户",
    "11030":"参数类型不对，暂不支持该类型"

}


# tomorrow_time = (date.today() + timedelta(days=+1)).strftime("%Y%m%d")
yesterday_time = (date.today() + timedelta(days=-1)).strftime("%Y%m%d")
tomorrow_time = (date.today() + timedelta(days=+1)).strftime("%Y%m%d")
current_time = (date.today()).strftime("%Y%m%d")



#aliyun
AccessKeyID = "LTAI4FmMu531TSvt6jJPJejw"
AccessKeySecret = "cl5uauXWgkbqQclu9KRmpcReybLOWO"
endpoint = "https://oss-cn-beijing.aliyuncs.com"
# -*- coding: utf-8 -*-

# @Time : 2021/10/8 10:02

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : senddingding.py

import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
import json
import sys
sys.path.append("../")
from config import *

URL ="https://oapi.dingtalk.com/robot/send?access_token=01ce5d3420b992ff73243e469d1a2e82d0d512876860cc871dd06e30ef52736f"

 # 钉钉机器人文档说明
 # https://ding-doc.dingtalk.com/doc#/serverapi2/qf2nxq

def get_timestamp_sign():
    timestamp = str(round(time.time() * 1000))
    secret = "SEC200c93201eb9c3cb87dd22fc672002f0bde09238db83f926af078d9ada97498c" #
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc,
                         digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return (timestamp, sign)


def get_signed_url():
    timestamp, sign = get_timestamp_sign()
    webhook = URL + "&timestamp="+timestamp+"&sign="+sign
    return webhook

def get_webhook(mode):

    if mode == 0: # only 敏感字
       webhook = URL
    elif mode == 1 or mode ==2 : # 敏感字和加签 或 # 敏感字+加签+ip
        # 加签： https://oapi.dingtalk.com/robot/send?access_token=XXXXXX&timestamp=XXX&sign=XXX
        webhook = get_signed_url()
    else:
        webhook = ""
    return webhook

def get_message(content,is_send_all):
    # 和类型相对应，具体可以看文档 ：https://ding-doc.dingtalk.com/doc#/serverapi2/qf2nxq
    # 可以设置某个人的手机号，指定对象发送
    message = {
        "msgtype": "text", # 有text, "markdown"、link、整体跳转ActionCard 、独立跳转ActionCard、FeedCard类型等
        "text": {
            "content": content # 消息内容
        },
        "at": {
            "atMobiles": [
                 # "13559436425",
             ],
            "isAtAll": is_send_all # 是否是发送群中全体成员
        }
    }
    logger.info(message)
    return message

def send_ding_message(content,is_send_all):
    # 请求的URL，WebHook地址
    webhook = get_webhook(1) # 主要模式有 0：敏感字 1：# 敏感字 +加签 3：敏感字+加签+IP
    # 构建请求头部
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    # 构建请求数据
    message = get_message(content,is_send_all)
    # 对请求的数据进行json封装
    message_json = json.dumps(message)
    # 发送请求
    info = requests.post(url=webhook, data=message_json, headers=header)
    # 打印返回的结果
    logger.info(info.text)

if __name__ == "__main__":
    content = "大家好，我是加签且带不敏感字的订阅机器人-新架构"
    is_send_all = False
    send_ding_message(content,is_send_all)

    # 建议版本
    # for i in range(1, 6):
    #     send_mes = []
    #     send_mes.append(u"清理当前通道中，测试请忽略-%s" % i)
    #     if send_mes:
    #         url = "https://oapi.dingtalk.com/robot/send?access_token=eeceb505673c185ffe5f49a22599d9f247d691c682e5b9ab8ef642fd7485da26"
    #         HEADERS = {"Content-Type": "application/json;charset=utf-8"}
    #         send_mes.insert(0, u"业务预警：")
    #         data = {
    #             "msgtype": "text",
    #             "text": {
    #                 "content": "\n".join(send_mes)
    #             },
    #             "at": {
    #                 # "isAtAll": True
    #                 "atMobiles": [
    #                     ""  # 需要填写自己的手机号，钉钉通过手机号@对应人
    #                 ],
    #                 "isAtAll": False  # 是否@所有人，默认否
    #             }
    #         }
    #         json_data = json.dumps(data).encode(encoding='utf-8')
    #         header_encoding = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    #                            "Content-Type": "application/json"}
    #         req = requests.post(url=url, data=json_data, headers=header_encoding)
    #         logger.info(req.text)
    #         logger.info(req.status_code)
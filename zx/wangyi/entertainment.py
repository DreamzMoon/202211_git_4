# -*- coding: utf-8 -*-

# @Time : 2022/1/7 16:39

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : entertainment.py

import requests
from bs4 import BeautifulSoup
import sys, os, time
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
import time
import json
from config import *
import traceback
import re

from util.help_fun import send_dingding

def get_proxy():
    try:
        proxy = "http://http.tiqu.letecs.com/getip3?num=1&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=&gm=4"
        proxy_res = requests.get(proxy)
        proxy = proxy_res.text.strip()
        return True,proxy
    except:
        return False,""

# 网易时尚
# https://fashion.163.com/special/002688FE/fashion_datalist_03.js 时尚


main_url = "https://ent.163.com/special/000380VU/newsdata_index_"
#翻页次数
i = 1
count = 9
timeout = 5
type = "文娱"
crawlcount = 0

detail_urls = []

# 第一获取代理

proxies = ""
proxy_result = get_proxy()
proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""

while i <= count:
    try:
        current_url = ""
        if i == 1:
            current_url = main_url[0:-1]+".js"
        else:
            page = "0"+str(i) if len(str(i))<2 else str(i)
            logger.info(page)
            logger.info("%s time" %i)
            current_url = main_url + page + ".js"
        logger.info(current_url)
        res = requests.get(url=current_url,proxies=proxies,timeout=timeout)
        if res.status_code != 200:
            time.sleep(2)
            proxy_result = get_proxy()
            proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""
        time.sleep(1)
        soup = BeautifulSoup(res.text,"lxml")
        logger.info(soup)
        a_lists = re.findall(r'docurl\":\"(.*?)\"',res.text)
        logger.info(a_lists)
        for a in a_lists:
            detail_urls.append(a)
    except:
        logger.info("当前主地址不要")
        continue
    i += 1

logger.info(detail_urls)
logger.info(len(detail_urls))

detail_urls = list(set(detail_urls))

logger.info("去重后")
logger.info(detail_urls)
logger.info(len(detail_urls))
time.sleep(2)


# 换代理
proxy_result = get_proxy()
proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""

for i,detail_url in enumerate(detail_urls):
    detail = {}
    logger.info("i:%s detail_url:%s" %(i,detail_url))
    try:
        res = requests.get(url=detail_url,proxies=proxies,timeout=timeout)
        if res.status_code != 200:
            time.sleep(2)
            proxy_result = get_proxy()
            proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""
        soup = BeautifulSoup(res.text, "lxml")
        detail["title"] = soup.select("h1.post_title")[0].text
        detail["public_time"],detail["source"] = soup.select("div.post_info")[0].text.strip().split("来源")
        detail["public_time"] = detail["public_time"]
        detail["source"] = detail["source"].split("\n")[0].split(":")[1].strip()
        try:
            detail["author"] = soup.select("div.post_author a img")[0].get("alt")
        except:
            detail["author"] = ""

        detail["body_text"] = ""
        bodys = soup.select("div.post_body p")
        for body in bodys:
            detail["body_text"] = detail["body_text"] + str(body)
        logger.info(detail["body_text"])

        detail["exposure"] = soup.select("a.post_top_tie_count")[0].text
        detail["des"] = ""
        detail["show_count"] = soup.select("a.post_top_tie_count")[0].text
        #标签
        detail["keyword_or_label"] = ""
        tags = soup.select("div.post_crumb a")
        for tag in tags:
            if tag.text.find("网易") > -1 or tag.text.find("正文"):
                continue
            else:
                detail["keyword_or_label"] = tag.text.strip()
        detail["detail_url"] = detail_url
        imgs = soup.select("div.post_body p img")
        detail["imgs"] = []
        for img in imgs:
            detail["imgs"].append(img.get("src"))

        logger.info(detail)
        logger.info("-----------------------")
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.info("超时了不要了")
        continue


    if detail:
        logger.info(detail["public_time"])
        url = "http://xs.lkkjjt.com/open/content/collection"
        headers = {"access-key": "skv6lYagMGf0nwWB460CzeYiRJdMKn4n"}
        post_data = {
            "source": detail["source"], "author": detail["author"], "type": type,
            "title": detail["title"],
            "describes": detail["des"], "tags": detail["keyword_or_label"], "visits_virtual": "",
            "exposure": detail["show_count"], "public_time": detail["public_time"],
            "ori_url": detail["detail_url"],
            "sub_images": json.dumps(detail["imgs"])[1:-1].replace("\"", ""),
            "body_text": str(detail["body_text"])
        }

        logger.info("title:%s--url:%s" % (detail["title"], detail["detail_url"]))
        res = requests.post(url=url, headers=headers, data=post_data)
        logger.info("zixun_res:%s" % res.status_code)
        logger.info(res.text)
        if res.status_code == 200:
            logger.info("推送成功")
            crawlcount = crawlcount + 1

logger.info("一共推送了:%s" %crawlcount)

send_dingding(["资讯--文娱板块去重后一共推送 :%s" %crawlcount])
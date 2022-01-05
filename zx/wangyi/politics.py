# -*- coding: utf-8 -*-

# @Time : 2022/1/4 10:53

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : politics.py

import requests
from bs4 import BeautifulSoup
import sys, os, time
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])


import time
import json
from config import *
import traceback



def get_proxy():
    try:
        proxy = "http://http.tiqu.letecs.com/getip3?num=1&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=&gm=4"
        proxy_res = requests.get(proxy)
        proxy = proxy_res.text.strip()
        return True,proxy
    except:
        return False,""

# 光明
# https://politics.gmw.cn/node_9840.htm

main_url = "https://politics.gmw.cn/node_9840"
#翻页次数
i = 1
count = 10
timeout = 5
type = "时政"
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
            current_url = main_url+".htm"
        else:
            page = str(i) if len(str(i))<2 else str(i)
            logger.info(page)
            logger.info("%s time" %i)
            current_url = main_url + "_" +page + ".htm"
        logger.info(current_url)
        res = requests.get(url=current_url,proxies=proxies,timeout=timeout)
        if res.status_code != 200:
            time.sleep(2)
            proxy_result = get_proxy()
            proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""
        time.sleep(1)
        soup = BeautifulSoup(res.text,"lxml")
        # logger.info(soup)
        a_lists = soup.select("ul.channel-newsGroup li a")
        # logger.info(a_lists)
        for a in a_lists:
            detail_urls.append("https://politics.gmw.cn/"+a.get("href"))
        logger.info(detail_urls)
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
# detail_urls = ["https://politics.gmw.cn/2022-01/02/content_35422783.htm"]

for i,detail_url in enumerate(detail_urls):
    detail = {}
    logger.info("i:%s detail_url:%s" %(i,detail_url))
    try:
        res = requests.get(url=detail_url, proxies=proxies, timeout=timeout)

        if res.status_code != 200:
            time.sleep(2)
            proxy_result = get_proxy()
            proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""
        res = res.content.decode(encoding=res.apparent_encoding)
        with open("e:/123321.html","w+",encoding="utf-8") as f:
            f.write(res)
        soup = BeautifulSoup(res, "lxml")
        detail["title"] = soup.select("h1.u-title")[0].text
        detail["public_time"] = soup.select("span.m-con-time")[0].text.strip()
        detail["source"] = soup.select("span.m-con-source a")[0].text.strip()
        try:
            detail["author"] = soup.select("span.liability")[0].text.split("：")[1]
        except:
            detail["author"] = ""
        detail["body_text"] = ""
        bodys = soup.select("div.u-mainText p")
        for body in bodys:
            detail["body_text"] = detail["body_text"] + str(body)
        logger.info(detail["body_text"])

        detail["des"] = ""
        detail["show_count"] = 0
        #标签
        detail["keyword_or_label"] = ""

        detail["detail_url"] = detail_url
        imgs = soup.select("div.u-mainText p img")
        detail["imgs"] = []
        for img in imgs:
            detail["imgs"].append(img.get("src"))

        logger.info(detail)
        logger.info('detail["author"]:%s' %detail["author"])
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
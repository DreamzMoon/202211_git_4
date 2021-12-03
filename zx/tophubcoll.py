# -*- coding: utf-8 -*-

# @Time : 2021/12/3 11:01

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : tophubcoll.py

import sys, os, time
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

import requests
from bs4 import BeautifulSoup
from config import *

'''
采集模块 科技 财经 报刊
'''
# coll_urls = ["https://tophub.today/c/tech","https://tophub.today/c/ent","https://tophub.today/c/finance","https://tophub.today/c/epaper"]

headers = {}
headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
url = "https://tophub.today/c/tech"

# crawl_lists = []
crawl_dict = {}

for i in range(1,11):
    url = url+"?"+ "p=%s" %i
    # logger.info("url:%s" %url)
    res = requests.get(url=url,headers=headers)

    soup = BeautifulSoup(res.text,"lxml")
    module = soup.select("div.bc-cc div.cc-cd")


    source_list = soup.select("div.cc-cd-lb")
    source_content_list = soup.select("div.bc-cc div.cc-cd div.cc-cd-cb div.nano-content")

    #遍历来源
    for i,sl in enumerate(source_list):
        # logger.info(sl.text)
        if sl.text not in crawl_dict.keys():
            crawl_dict[sl.text] = []
        a_href = soup.select("div.bc-cc div.cc-cd div.cc-cd-cb div.nano-content")[i].select("a")
        for a in a_href:
            detail_url = a.get("href")
            crawl_dict[sl.text].append(detail_url)



detail_dict = {}
logger.info(crawl_dict)
for cl in crawl_dict.keys():
    logger.info(cl)
    logger.info("-------------------")
    detail_urls = crawl_dict[cl]
    for d_url in detail_urls:
        detail_res = requests.get(url=d_url,headers=headers)
        soup_detail = BeautifulSoup(detail_res.text,"lxml")

        #创业邦
        if cl == "创业邦":
            detail_dict["source"] = cl
            detail_dict["author"] = soup_detail.select("span.name")[0].text.split("：")[1]
            # 先写死
            detail_dict["article_type"] = "科技"
            detail_dict["title"] = soup_detail.select("h1.article-tit")[0].text
            detail_dict["des"] = ""
            detail_dict["keyword_or_label"] = ""
            detail_dict["show_count"] = ""
            detail_dict["public_time"] = soup_detail.select("span.date-time")[1].text.strip()
            detail_dict["detail_url"] = d_url
            detail_dict["home_img"] = soup_detail.select("div.article-content img")[0].get("src")
            detail_dict["content"] = soup_detail.select("div.article-content")[0]
        elif cl == "36氪":
            detail_dict["source"] = cl
            detail_dict["author"] = soup_detail.select("a.title-icon-item")[0].text
            # 先写死
            detail_dict["article_type"] = "科技"
            detail_dict["title"] = soup_detail.select("a.title-icon-item")[0].text
            detail_dict["des"] = soup_detail.select("div.summary")[0].text
            detail_dict["keyword_or_label"] = ""
            detail_dict["show_count"] = ""
            detail_dict["public_time"] = soup_detail.select("span.date-time")[1].text.strip()
            detail_dict["detail_url"] = d_url
            detail_dict["home_img"] = soup_detail.select("div.article-content img")[0].get("src")
            detail_dict["content"] = soup_detail.select("div.article-content")[0]
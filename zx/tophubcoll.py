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
import re
import json
import random
'''
采集模块 科技 财经 报刊
'''
# coll_urls = ["https://tophub.today/c/tech","https://tophub.today/c/ent","https://tophub.today/c/finance","https://tophub.today/c/epaper"]

#代理获取
# proxy_lists = []
# for i in range(0,20):
#     proxy_url = "http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&pack=125663&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
#     proxy_res = requests.get(proxy_url)
#     proxy = proxy_res.text.strip()
#     proxy_lists.append(proxy)
#     time.sleep(3)
# logger.info(proxy_lists)
# logger.info(proxy)

proxy_lists = ['42.84.166.232:4251', '58.243.205.212:4243', '101.27.207.235:4220', '114.99.22.166:4225', '101.74.3.43:4220', '110.80.160.76:4245', '49.89.202.4:4231', '113.237.187.134:4252', '106.40.144.10:4285', '42.54.89.152:4213', '223.215.177.99:4254', '101.72.133.201:4254', '110.230.215.155:4210', '42.84.170.99:4264', '42.56.238.233:4275', '117.27.25.131:4232', '171.15.65.2:4230', '222.241.70.96:4210', '124.113.193.167:4231', '42.57.91.51:4231']
proxies = {'http': random.choice(proxy_lists)}
headers = {}
user_agent = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1",
              "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
              "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
              "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
              "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)",
              "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)",
              "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.0)",
              "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
              "Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12",
              "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)",
              "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)",
              "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.33 Safari/534.3 SE 2.X MetaSr 1.0",
              "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)",
              "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201",
              "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E) QQBrowser/6.9.11079.201"]


# headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
headers["User-Agent"] = random.choice(user_agent)
url = "https://tophub.today/c/tech"

# crawl_lists = []
crawl_dict = {}
sssource = "科技"
for i in range(2,5):
    main_url = url+"?"+ "p=%s" %i
    logger.info(main_url)
    proxies = {'http': random.choice(proxy_lists), 'https': random.choice(proxy_lists)}
    headers["User-Agent"] = random.choice(user_agent)

    res = requests.get(url=main_url,headers=headers,proxies=proxies,timeout=5)

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
            look_count = a.select("span.e")[0].text
            crawl_dict[sl.text].append(detail_url+"=_="+look_count)


detail_list = []

for cl in crawl_dict.keys():

    if cl.strip() not in ["创业邦","36氪","少数派","IT之家","爱范儿","科普中国网","极客公园"]:
        continue
    logger.info("source:%s" % cl)
    detail_urls = crawl_dict[cl]
    for d_url in detail_urls:

        proxies = {'http': 'http://' + random.choice(proxy_lists)}
        headers["User-Agent"] = random.choice(user_agent)

        url,view = d_url.split("=_=")
        logger.info("----------")
        headers["User-Agent"] = random.choice(user_agent)
        try:
            logger.info(url)
            detail_res = requests.get(url=url,headers=headers,proxies=proxies,timeout=5)
            with open("e:/tototo.html","w+",encoding="utf-8") as f:
                f.write(detail_res.text)
            logger.info(detail_res.text)
            logger.info(detail_res.status_code)
            soup_detail = BeautifulSoup(detail_res.text, "lxml")
            cl = cl.strip()
        except:
            continue

        #创业邦
        try:
            detail_dict = {}
            if cl == "创业邦":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("span.name")[0].text.split("：")[1]
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("h1.article-tit")[0].text
                detail_dict["des"] = ""
                detail_dict["keyword_or_label"] = ""
                detail_dict["show_count"] = ""
                detail_dict["public_time"] = soup_detail.select("span.date-time")[1].text.strip()
                detail_dict["detail_url"] = url
                detail_dict["home_img"] = soup_detail.select("div.article-content img")[0].get("src")
                detail_dict["content"] = soup_detail.select("div.article-content")[0]
            elif cl == "36氪":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("a.title-icon-item")[0].text
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("h1.article-title")[0].text
                detail_dict["des"] = soup_detail.select("div.summary")[0].text
                detail_dict["keyword_or_label"] = ""
                detail_dict["show_count"] = ""
                detail_dict["public_time"] = soup_detail.select("span.title-icon-item")[0].text
                detail_dict["detail_url"] = url
                detail_dict["imgs"] = []
                imgs = soup_detail.select("div.common-width")[3].select("img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))
                detail_dict["content"] = soup_detail.select("div.common-width")[3]
            elif cl == "少数派":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("span.nickname")[0].text
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("div.title")[0].text
                detail_dict["des"] = ""
                detail_dict["keyword_or_label"] = view
                detail_dict["show_count"] = ""
                detail_dict["public_time"] = soup_detail.select("div.timer")[0].text
                detail_dict["detail_url"] = url

                detail_dict["content"] = soup_detail.select("div.content")[0]
                detail_dict["imgs"] = []
                imgs = soup_detail.select("div.content img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))
                if soup_detail.select("div.banner"):
                    detail_dict["imgs"].insert(0,re.findall("url\((.*?)\)", soup_detail.select("div.banner")[0].get("style"))[0])
            elif cl == "虎嗅网":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("span.author-info__username")[0].text
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("h1.article__title")[0].text
                detail_dict["des"] = ""
                detail_dict["keyword_or_label"] = ""
                detail_dict["show_count"] = view
                detail_dict["public_time"] = soup_detail.select("span.article__time")[0].text
                detail_dict["detail_url"] = url

                detail_dict["content"] = soup_detail.select("div#article-content")[0]
                detail_dict["imgs"] = []
                imgs = soup_detail.select("div#article-content img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))
            elif cl == "IT之家":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("span.news-author span")[0].text
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("h1.title")[0].text
                detail_dict["des"] = ""
                detail_dict["keyword_or_label"] = ""
                detail_dict["show_count"] = view
                detail_dict["public_time"] = soup_detail.select("span.news-time")[0].text
                detail_dict["detail_url"] = url

                detail_dict["content"] = soup_detail.select("div#news-content")[0]
                detail_dict["imgs"] = []
                imgs = soup_detail.select("div#news-content img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))
            elif cl == "爱范儿":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("p.c-article-header-meta__category")[0].text
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("h1.c-single-normal__title")[0].text
                detail_dict["des"] = ""
                detail_dict["keyword_or_label"] = view
                detail_dict["show_count"] = ""
                detail_dict["public_time"] = soup_detail.select("p.c-article-header-meta__time")[0].text
                detail_dict["detail_url"] = url

                detail_dict["content"] = soup_detail.select("article.o-single-content__body__content")[0]
                detail_dict["imgs"] = []
                imgs = soup_detail.select("article.o-single-content__body__content img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))
            elif cl == "科普中国网":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("p.tips span")[0].text
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("h1")[0].text
                detail_dict["des"] = ""
                detail_dict["keyword_or_label"] = view
                detail_dict["show_count"] = ""
                detail_dict["public_time"] = soup_detail.select("p.tips span")[0].text
                detail_dict["detail_url"] = url

                detail_dict["content"] = soup_detail.select("div.TRS_Editor")[0]
                detail_dict["imgs"] = []
                imgs = soup_detail.select("div.TRS_Editor img")
                for img in imgs:
                    img = "https://www.kepuchina.cn/more/"+img.get("src")[4:10]+img.get("src")[1:]
                    detail_dict["imgs"].append(img)
            elif cl == "极客公园":
                detail_dict["source"] = cl
                detail_dict["author"] = soup_detail.select("div.user-info span")[0].text
                # 先写死
                detail_dict["article_type"] = sssource
                detail_dict["title"] = soup_detail.select("h1.topic-title")[0].text
                detail_dict["des"] = ""
                detail_dict["keyword_or_label"] = ""
                detail_dict["show_count"] = ""
                detail_dict["public_time"] = soup_detail.select("span.release-date")[0].text
                detail_dict["detail_url"] = url

                detail_dict["content"] = soup_detail.select("div#article-body")[0]
                detail_dict["imgs"] = []
                imgs = soup_detail.select("div#article-body img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))

            else:
                continue
            # logger.info(detail_dict)
            # time.sleep(2)
            if detail_dict:
                url = "http://xs.lkkjjt.com/open/content/collection"
                headers = {"access-key":"skv6lYagMGf0nwWB460CzeYiRJdMKn4n"}
                post_data = {
                "source":detail_dict["source"],"author":detail_dict["author"],"type":detail_dict["article_type"],"title":detail_dict["title"],
                "describes":detail_dict["des"],"tags":detail_dict["keyword_or_label"],"visits_virtual":"","exposure":detail_dict["show_count"],"public_time":detail_dict["public_time"],
                "ori_url":detail_dict["detail_url"],"sub_images":detail_dict["imgs"],"body_text":str(detail_dict["content"])
                }
                res = requests.post(url=url,headers=headers,data=post_data)
                logger.info("-------------------")
                logger.info(res.text)
                logger.info(res.status_code)
                logger.info("推送成功")
                logger.info(post_data)
                time.sleep(10)
                detail_list.append(detail_dict)
        except Exception as e:
            import traceback
            logger.exception(traceback.format_exc())
            pass

logger.info(len(detail_list))
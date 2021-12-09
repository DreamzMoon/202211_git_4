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
from fake_useragent import UserAgent

'''
采集模块 科技 财经 报刊
'''
# coll_urls = ["https://tophub.today/c/tech","https://tophub.today/c/ent","https://tophub.today/c/finance","https://tophub.today/c/epaper"]

ua = UserAgent()


#代理获取
proxy_lists = []
for i in range(0,10):
    proxy_url = "http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&pack=125663&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
    proxy_res = requests.get(proxy_url)
    proxy = proxy_res.text.strip()
    logger.info(proxy)
    proxy_lists.append(proxy)
    time.sleep(3)

logger.info(proxy_lists)

user_agent = []
for i in range(0,100):
    user_agent.append(ua.chrome)
    user_agent.append(ua.ie)
    user_agent.append(ua.random)

proxies = {'http': 'http://'+random.choice(proxy_lists)}
headers = {}
# headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
headers["User-Agent"] = random.choice(user_agent)
url = "https://tophub.today/c/tech"

# crawl_lists = []
crawl_dict = {}
sssource = "科技"
for i in range(1,6):
    main_url = url+"?"+ "p=%s" %i
    logger.info(main_url)
    res = requests.get(url=main_url,headers=headers,proxies=proxies)

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



detail_dict = {}
detail_list = []
logger.info(crawl_dict.keys())
for cl in crawl_dict.keys():

    # if cl.strip() not in ["创业邦","36氪","少数派","IT之家","爱范儿","科普中国网","极客公园"]:
    if cl.strip() not in ["爱范儿"]:
        continue

    logger.info("source:%s" %cl)
    logger.info("url_length:%s" %len(crawl_dict[cl]))

    detail_urls = crawl_dict[cl]
    for d_url in detail_urls:
        proxies = {'http': 'http://'+proxy}
        # view 可能是曝光 可能是标签
        url,view = d_url.split("=_=")
        logger.info("url:%s" %url)
        proxies = {'http': 'http://' + random.choice(proxy_lists)}
        headers["User-Agent"] = random.choice(user_agent)
        detail_res = requests.get(url=url,headers=headers,proxies=proxies)
        logger.info(detail_res.status_code)
        with open("e:/to.html","w+",encoding="utf-8") as f:
            f.write(detail_res.text)
        soup_detail = BeautifulSoup(detail_res.text,"lxml")
        cl = cl.strip()
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
                if soup_detail.select("a.title-icon-item"):
                    detail_dict["author"] = soup_detail.select("a.title-icon-item")[0].text
                else:
                    detail_dict["author"] = ""
                detail_dict["article_type"] = sssource

                if soup_detail.select("h1.article-title"):
                    detail_dict["title"] = soup_detail.select("h1.article-title")[0].text
                elif soup_detail.select("a.item-title"):
                    detail_dict["title"] = soup_detail.select("a.item-title")[0].text
                else:
                    detail_dict["title"] = ""

                if soup_detail.select("div.summary"):
                    detail_dict["des"] = soup_detail.select("div.summary")[0].text
                else:
                    detail_dict["des"] = ""

                detail_dict["keyword_or_label"] = ""
                detail_dict["show_count"] = ""

                if soup_detail.select("span.title-icon-item"):
                    detail_dict["public_time"] = soup_detail.select("span.title-icon-item")[0].text
                elif soup_detail.select("span.time"):
                    detail_dict["public_time"] = soup_detail.select("span.time")[0].text
                else:
                    detail_dict["public_time"] = ""
                detail_dict["detail_url"] = url
                detail_dict["imgs"] = []

                imgs = []
                detail_dict["imgs"] = []
                if soup_detail.select("div.common-width"):
                    imgs = soup_detail.select("div.common-width")[3].select("img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))

                if soup_detail.select("div.common-width"):
                    detail_dict["content"] = soup_detail.select("div.common-width")[3]
                elif soup_detail.select("div.item-desc"):
                    detail_dict["content"] = soup_detail.select("div.item-desc")[0]
                else:
                    detail_dict["content"] = ""
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

                detail_dict["content"] = soup_detail.select("div.content")
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

                detail_dict["content"] = soup_detail.select("div#article-content")
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

                detail_dict["content"] = soup_detail.select("div#news-content")
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

                detail_dict["content"] = soup_detail.select("article.o-single-content__body__content")
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

                detail_dict["content"] = soup_detail.select("div.TRS_Editor")
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

                detail_dict["content"] = soup_detail.select("div#article-body")
                detail_dict["imgs"] = []
                imgs = soup_detail.select("div#article-body img")
                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))

            else:
                continue

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

                detail_list.append(detail_dict)
            # logger.info(detail_list)
        except Exception as e:
            import traceback
            logger.exception(traceback.format_exc())
            pass
        # logger.info(detail_dict)

# logger.info(len(detail_list))
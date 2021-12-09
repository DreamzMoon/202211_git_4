# -*- coding: utf-8 -*-

# @Time : 2021/12/8 10:07

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : tophubtech.py

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
import traceback
ua = UserAgent()

proxy_lists = []
for i in range(0,30):
    proxy_url = "http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&pack=125663&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
    proxy_res = requests.get(proxy_url)
    proxy = proxy_res.text.strip()
    logger.info(proxy)
    proxy_lists.append(proxy)
    time.sleep(3)

# proxy_lists = ['117.70.40.249:4258', '1.84.253.29:4258', '223.242.21.28:4267', '113.94.19.89:4231', '59.62.54.124:4261', '223.156.166.128:4272', '120.35.178.237:4242', '183.94.230.167:4220', '58.241.203.26:4231', '123.54.45.244:4210']

logger.info(proxy_lists)
# proxy_lists = ['113.77.87.141:4221', '42.56.238.140:4278', '114.239.120.36:4231', '59.58.19.195:4235', '27.158.236.134:4224']

user_agent = []
for i in range(0,300):
    user_agent.append(ua.chrome)
    user_agent.append(ua.ie)
    user_agent.append(ua.random)

proxies = {'http': 'http://'+random.choice(proxy_lists)}
headers = {}
headers["User-Agent"] = random.choice(user_agent)

url_dict = {"36氪":["https://tophub.today/n/Q1Vd5Ko85R","https://tophub.today/n/KqndgapoLl","https://tophub.today/n/qndgNRZvLl","https://tophub.today/n/aqeED7Md9R",
                   "https://tophub.today/n/3adqr3xeng","https://tophub.today/n/5PdMQGldmg","https://tophub.today/n/anop0GZolZ"],
            "少数派":["https://tophub.today/n/Y2KeDGQdNP","https://tophub.today/n/NaEdZZXdrO","https://tophub.today/n/Q0orQa2e8B"],
            "爱范儿":["https://tophub.today/n/proPGY0eq6","https://tophub.today/n/Jndkp4ye3V","https://tophub.today/n/74KvxK7okx"],
            "IT之家":["https://tophub.today/n/74Kvx59dkx","https://tophub.today/n/m4ejb73exE","https://tophub.today/n/1yjvQDjobg","https://tophub.today/n/qENeY0RdY4","https://tophub.today/n/DpQvNGzdNE"],
            "极客公园":["https://tophub.today/n/NRrvWYDe5z","https://tophub.today/n/qndgkK0dLl"],
            "数字尾巴":["https://tophub.today/n/JndkEzQo3V","https://tophub.today/n/n3moBA1eN5"]

            }

# url_dict = {"数字尾巴":["https://tophub.today/n/JndkEzQo3V","https://tophub.today/n/n3moBA1eN5"]}

type = "科技"

detail_url_dict = {}

for ud in url_dict.keys():
    source = ud
    detail_url_dict[source] = []
    main_urls = url_dict[source]
    for main_url in main_urls:
        try:
            main_res = requests.get(url=main_url,proxies=proxies,headers=headers,timeout=3)
            main_soup = BeautifulSoup(main_res.text,"lxml")
            # a_href_htmls = main_soup.select("div.jc-c")[0].select("tbody tr td a")
            a_href_htmls = main_soup.select("div.jc-c")[0].select("tbody tr")
            # a_href_htmls = main_soup.select("div.jc-c")[0].select("tbody tr")
            for a in a_href_htmls:
                if source in ["36氪","少数派","爱范儿","IT之家","极客公园","数字尾巴"]:
                    # detail_url_dict[source].append("https://tophub.today"+a.get("href"))
                    # 拼接url+浏览量
                    detail_url_dict[source].append("https://tophub.today" + a.select("td a")[0].get("href")+"=+_+="+a.select("td")[2].text.split("评")[0])
                else:
                    continue
            detail_url_dict[source] = list(set(detail_url_dict[source]))
        except:
            continue

# logger.info("总爬虫个数:%s" %len(detail_url_dict["数字尾巴"]))
crawlcount = 0
for ss in detail_url_dict.keys():
    source = ss
    urls = detail_url_dict[ss]
    for url in reversed(urls):
        try:
            u,view = url.split("=+_+=")
            logger.info("crawl:%s" %u)
            # view 可能是曝光 可能是标签
            proxies = {'http': 'http://' + random.choice(proxy_lists)}
            headers["User-Agent"] = random.choice(user_agent)
            detail_res = requests.get(url=u, headers=headers, proxies=proxies,timeout=3)

            if detail_res.status_code != 200:
                continue
            soup_detail = BeautifulSoup(detail_res.text, "lxml")
            detail_dict = {}
            if source == "36氪":
                detail_dict["source"] = ss
                if soup_detail.select("a.title-icon-item"):
                    detail_dict["author"] = soup_detail.select("a.title-icon-item")[0].text
                else:
                    detail_dict["author"] = ""
                detail_dict["article_type"] = type

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
                    detail_dict["public_time"] = soup_detail.select("span.title-icon-item")[0].text.split("·")[1]
                elif soup_detail.select("span.time"):
                    detail_dict["public_time"] = soup_detail.select("span.time")[0].text
                else:
                    detail_dict["public_time"] = ""
                detail_dict["detail_url"] = detail_res.url
                detail_dict["imgs"] = []

                imgs = []

                if soup_detail.select("div.common-width"):
                    imgs = soup_detail.select("div.common-width")[3].select("img")
                elif soup_detail.select("div.item-desc"):
                    imgs = soup_detail.select("div.item-desc")[0].select("img")

                for img in imgs:
                    detail_dict["imgs"].append(img.get("src"))
                logger.info(len(detail_dict["imgs"]))

                if soup_detail.select("div.common-width"):
                    detail_dict["content"] = soup_detail.select("div.common-width")[3]
                elif soup_detail.select("div.item-desc"):
                    detail_dict["content"] = soup_detail.select("div.item-desc")[0]
                else:
                    detail_dict["content"] = ""

            elif source == "少数派":
                try:
                    detail_dict["source"] = ss
                    detail_dict["author"] = soup_detail.select("span.nickname")[0].text
                    detail_dict["article_type"] = type
                    detail_dict["title"] = soup_detail.select("div.title")[0].text
                    detail_dict["des"] = ""
                    detail_dict["keyword_or_label"] = ""
                    detail_dict["show_count"] = ""
                    detail_dict["public_time"] = soup_detail.select("div.timer")[0].text
                    detail_dict["detail_url"] = detail_res.url

                    detail_dict["content"] = soup_detail.select("div.content")[0]
                    detail_dict["imgs"] = []
                    imgs = soup_detail.select("div.content img")
                    for img in imgs:
                        detail_dict["imgs"].append(img.get("src"))
                    if soup_detail.select("div.banner"):
                        detail_dict["imgs"].insert(0, re.findall("url\((.*?)\)",soup_detail.select("div.banner")[0].get("style"))[0])
                except:
                    continue
            elif source == "爱范儿":
                try:
                    detail_dict["source"] = source
                    detail_dict["author"] = soup_detail.select("p.c-article-header-meta__category")[0].text
                    detail_dict["article_type"] = type
                    detail_dict["title"] = soup_detail.select("h1.c-single-normal__title")[0].text
                    detail_dict["des"] = ""
                    detail_dict["keyword_or_label"] = ""
                    detail_dict["show_count"] = ""
                    detail_dict["public_time"] = soup_detail.select("p.c-article-header-meta__time")[0].text
                    detail_dict["detail_url"] = detail_res.url

                    detail_dict["content"] = soup_detail.select("article.o-single-content__body__content")[0]
                    detail_dict["imgs"] = []
                    imgs = soup_detail.select("article.o-single-content__body__content img")
                    for img in imgs:
                        detail_dict["imgs"].append(img.get("src"))

                    if soup_detail.select("div#article-header"):
                        detail_dict["imgs"].insert(0, soup_detail.select("div#article-header img")[0].get("src"))
                except Exception as e:
                    logger.info("u:%s 采集可能有问题" %u)
            elif source == "IT之家":
                try:
                    detail_dict["source"] = source
                    detail_dict["author"] = soup_detail.select("span.news-author span")[0].text
                    detail_dict["article_type"] = type
                    detail_dict["title"] = soup_detail.select("h1.title")[0].text
                    detail_dict["des"] = ""
                    detail_dict["keyword_or_label"] = ""
                    detail_dict["show_count"] = view
                    detail_dict["public_time"] = soup_detail.select("span.news-time")[0].text
                    detail_dict["detail_url"] = detail_res.url
                    detail_dict["content"] = soup_detail.select("div.news-content")[0]
                    detail_dict["imgs"] = []
                    imgs = soup_detail.select("div.news-content img")
                    for img in imgs:
                        detail_dict["imgs"].append(img.get("src"))
                except:
                    logger.info("出错了:%s" %url)
            elif source == "极客公园":
                try:
                    detail_dict["source"] = source
                    detail_dict["author"] = soup_detail.select("div.user-info span")[0].text
                    # 先写死
                    detail_dict["article_type"] = type
                    detail_dict["title"] = soup_detail.select("h1.topic-title")[0].text
                    detail_dict["des"] = ""
                    detail_dict["keyword_or_label"] = ""
                    detail_dict["show_count"] = ""
                    detail_dict["public_time"] = soup_detail.select("span.release-date")[0].text
                    detail_dict["detail_url"] = detail_res.url

                    detail_dict["content"] = soup_detail.select("div#article-body")[0]
                    detail_dict["imgs"] = []
                    imgs = soup_detail.select("div#article-body img")
                    for img in imgs:
                        detail_dict["imgs"].append(img.get("src"))
                except:
                    logger.info("可能出错了:%s" %url)
            elif source == "数字尾巴":
                try:
                    detail_dict["source"] = source
                    detail_dict["author"] = ""
                    detail_dict["article_type"] = type
                    detail_dict["title"] = soup_detail.select("h1.title")[0].text
                    detail_dict["des"] = ""
                    detail_dict["keyword_or_label"] = ""
                    detail_dict["show_count"] = ""
                    detail_dict["public_time"] = ""
                    detail_dict["detail_url"] = detail_res.url

                    detail_dict["content"] = soup_detail.select("div.articles-comment-left")[0]
                    detail_dict["imgs"] = []
                    imgs = soup_detail.select("div.articles-comment-left img")
                    for img in imgs:
                        detail_dict["imgs"].append(img.get("src"))

                    if soup_detail.select("div.community_post-recommend-banner"):
                        detail_dict["imgs"].insert(0, soup_detail.select("div.community_post-recommend-banner img")[0].get("src"))
                except Exception as e:
                    logger.info("url:%s 采集可能有问题" %url)

                except Exception as e:
                    logger.info("xinlang u:%s 采集可能有问题" %u)
            else:
                continue
            if detail_dict:
                url = "http://xs.lkkjjt.com/open/content/collection"
                headers = {"access-key": "skv6lYagMGf0nwWB460CzeYiRJdMKn4n"}
                post_data = {
                    "source": detail_dict["source"], "author": detail_dict["author"], "type": detail_dict["article_type"],
                    "title": detail_dict["title"],
                    "describes": detail_dict["des"], "tags": detail_dict["keyword_or_label"], "visits_virtual": "",
                    "exposure": detail_dict["show_count"], "public_time": detail_dict["public_time"],
                    "ori_url": detail_dict["detail_url"], "sub_images": json.dumps(detail_dict["imgs"])[1:-1].replace("\"",""),
                    "body_text": str(detail_dict["content"])
                }

                logger.info("title:%s--url:%s" %(detail_dict["title"],detail_dict["detail_url"]))
                res = requests.post(url=url, headers=headers, data=post_data)
                if res.status_code == 200:
                    logger.info("推送成功")
                    crawlcount = crawlcount + 1
        except:
            continue

logger.info("采集入库个数:%s" %crawlcount)
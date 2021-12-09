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

def send_dingding(send_mes):
    if send_mes:
        url = "https://oapi.dingtalk.com/robot/send?access_token=f67d6a4d005654a96b904a7bafb9c469b70c013bfd17b120ae5a420eb509b101"
        HEADERS = {"Content-Type": "application/json;charset=utf-8"}
        send_mes.insert(0, u"业务预警：")
        data = {
            "msgtype": "text",
            "text": {
                "content": "\n".join(send_mes)
            },
            "at": {
                # "isAtAll": True
                "atMobiles": [
                    ""  # 需要填写自己的手机号，钉钉通过手机号@对应人
                ],
                "isAtAll": False  # 是否@所有人，默认否
            }
        }
        json_data = json.dumps(data).encode(encoding='utf-8')
        header_encoding = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
                           "Content-Type": "application/json"}
        req = requests.post(url=url, data=json_data, headers=header_encoding)
        print(req.text)
        print(req.status_code)


proxy_lists = []
proxy_url = "http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=100017&port=1&pack=125663&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=2&regions="
for i in range(0,15):

    proxy_res = requests.get(proxy_url)
    proxy = proxy_res.text.strip()
    proxy_lists.append(proxy)
    time.sleep(3)
logger.info(proxy_lists)

user_agent = []
for i in range(0,300):
    user_agent.append(ua.chrome)
    user_agent.append(ua.ie)
    user_agent.append(ua.random)

logger.info("ua成功")
# user_agent = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1",
#               "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0",
#               "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
#               "Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50",
#               "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)",
#               "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)",
#               "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.0)",
#               "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
#               "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)",
#               "Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12",
#               "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)",
#               "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)",
#               "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.33 Safari/534.3 SE 2.X MetaSr 1.0",
#               "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)",
#               "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201",
#               "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E) QQBrowser/6.9.11079.201",
#               "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)",
#               "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"]

proxies = {'http': 'http://'+random.choice(proxy_lists)}
headers = {}
headers["User-Agent"] = random.choice(user_agent)

url_dict = {
            "36氪":["https://tophub.today/n/Q1Vd5Ko85R","https://tophub.today/n/KqndgapoLl","https://tophub.today/n/qndgNRZvLl","https://tophub.today/n/aqeED7Md9R",
                   "https://tophub.today/n/3adqr3xeng","https://tophub.today/n/5PdMQGldmg","https://tophub.today/n/anop0GZolZ"],
            "少数派":["https://tophub.today/n/Y2KeDGQdNP","https://tophub.today/n/NaEdZZXdrO","https://tophub.today/n/Q0orQa2e8B"],
            "爱范儿":["https://tophub.today/n/proPGY0eq6","https://tophub.today/n/Jndkp4ye3V","https://tophub.today/n/74KvxK7okx"],
            "IT之家":["https://tophub.today/n/74Kvx59dkx","https://tophub.today/n/m4ejb73exE","https://tophub.today/n/1yjvQDjobg","https://tophub.today/n/qENeY0RdY4","https://tophub.today/n/DpQvNGzdNE"],
            "极客公园":["https://tophub.today/n/NRrvWYDe5z","https://tophub.today/n/qndgkK0dLl"],
            "数字尾巴":["https://tophub.today/n/JndkEzQo3V","https://tophub.today/n/n3moBA1eN5"],
            "ZAKER":["https://tophub.today/n/5VaobJgoAj","https://tophub.today/n/BwdG02wePx","https://tophub.today/n/ARe1Zylo7n"],
            "留学世界":["https://tophub.today/n/YqoXQwXvOD"]
            }


type = "科技"

detail_url_dict = {}

for ud in url_dict.keys():
    source = ud
    detail_url_dict[source] = []
    main_urls = url_dict[source]
    for main_url in reversed(main_urls):
        try:
            main_res = requests.get(url=main_url,proxies=proxies,headers=headers,timeout=3)
            main_soup = BeautifulSoup(main_res.text,"lxml")
            # a_href_htmls = main_soup.select("div.jc-c")[0].select("tbody tr td a")
            a_href_htmls = main_soup.select("div.jc-c")[0].select("tbody tr")
            for a in a_href_htmls:
                if source in ["36氪","少数派","爱范儿","IT之家","极客公园","数字尾巴","ZAKER","留学世界"]:
                    # detail_url_dict[source].append("https://tophub.today"+a.get("href"))
                    # 拼接url+浏览量
                    detail_url_dict[source].append("https://tophub.today" + a.select("td a")[0].get("href")+"=+_+="+a.select("td")[2].text.split("评")[0])
                else:
                    continue
            detail_url_dict[source] = list(set(detail_url_dict[source]))
        except:
            continue
logger.info("开始采集啦")

crawlcount = 0
for ss in detail_url_dict.keys():
    source = ss
    urls = detail_url_dict[ss]
    for url in urls:
        try:
            u,view = url.split("=+_+=")
            logger.info("crawl:%s" %u)
            # view 可能是曝光 可能是标签
            proxies = {'http': 'http://' + random.choice(proxy_lists)}
            headers["User-Agent"] = random.choice(user_agent)
            detail_res = requests.get(url=u, headers=headers, proxies=proxies,timeout=3)

            logger.info(detail_res.status_code)
            # if detail_res.status_code != 200:
            #     proxy_lists.remove(proxies["http"].split("//")[1])
            #     proxy_res = requests.get(proxy_url)
            #     proxy = proxy_res.text.strip()
            #     proxy_lists.append(proxy)
            #     time.sleep(3)
            #     logger.info("换代理了")
            #     continue
            soup_detail = BeautifulSoup(detail_res.text, "lxml")
            # if soup_detail.select("h1.font-weight-normal"):
            #     if soup_detail.select("font-weight-normal")[0].text == "人机交互验证":
            #         proxy_lists.remove(proxies["http"].split("//")[1])
            #         proxy_res = requests.get(proxy_url)
            #         proxy = proxy_res.text.strip()
            #         proxy_lists.append(proxy)
            #         time.sleep(3)
            #         logger.info("换代理了")
            #         continue

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
                    logger.info("url:%s 采集可能有问题" %u)
            elif source == "ZAKER":
                try:
                    detail_dict["source"] = source
                    detail_dict["author"] = soup_detail.select("a.article-auther")[0].text
                    detail_dict["article_type"] = type
                    detail_dict["title"] = soup_detail.select("h1.article-title")[0].text
                    detail_dict["des"] = ""
                    detail_dict["keyword_or_label"] = ""
                    detail_dict["show_count"] = ""
                    detail_dict["public_time"] = ""
                    detail_dict["detail_url"] = detail_res.url

                    detail_dict["content"] = soup_detail.select("div#content")[0]
                    detail_dict["imgs"] = []
                    imgs = soup_detail.select("div#content img")
                    for img in imgs:
                        detail_dict["imgs"].append(img.get("src"))
                except Exception as e:
                    logger.info("url:%s 采集可能有问题" %u)
            elif source == "留学世界":
                try:
                    detail_dict["source"] = source
                    detail_dict["author"] = ""
                    detail_dict["article_type"] = type
                    detail_dict["title"] = soup_detail.select("h1.article-title a")[0].text
                    detail_dict["des"] = ""
                    detail_dict["keyword_or_label"] = ""
                    detail_dict["show_count"] = ""
                    try:
                        detail_dict["keyword_or_label"] = soup_detail.select("div.article-meta span")[1].text.split("：")[1]
                        detail_dict["show_count"] = re.findall(r"\((.*?)\)",soup_detail.select("div.article-meta span")[2].text)[0]
                    except:
                        try:
                            detail_dict["keyword_or_label"] = soup_detail.select("div.article-meta span")[2].text.split("：")[1]
                            detail_dict["show_count"] = re.findall(r"\((.*?)\)", soup_detail.select("div.article-meta span")[3].text)[0]
                        except:
                            pass
                    detail_dict["public_time"] = ""
                    detail_dict["detail_url"] = detail_res.url

                    detail_dict["content"] = soup_detail.select("article.article-content")[0]
                    detail_dict["imgs"] = []
                    imgs = soup_detail.select("article.article-content img")
                    for img in imgs:
                        detail_dict["imgs"].append(img.get("src"))
                except Exception as e:
                    logger.exception(traceback.format_exc())
                    logger.info("url:%s 采集可能有问题" %u)

            else:
                continue
            # 如果title等于空可能被强了  钉钉推送
            if not detail_dict["title"]:
                send_dingding("爬虫网站被墙啦")

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
                logger.info("zixun_res:%s" %res.status_code)
                logger.info(res.text)
                if res.status_code == 200:
                    logger.info("推送成功")
                    crawlcount = crawlcount + 1

        except:
            continue

logger.info("采集入库个数:%s" %crawlcount)
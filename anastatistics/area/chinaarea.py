# -*- coding: utf-8 -*-

# @Time : 2022/1/10 14:40

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : chinaarea.py
import os
import sys

father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

from config import *
from util.help_fun import *
import json
import datetime
from datetime import timedelta,date
import traceback
import pandas as pd
from analyzeserver.common import *
import numpy as np
import time
from bs4 import BeautifulSoup
import random

def get_proxy():
    try:
        proxy = "http://http.tiqu.letecs.com/getip3?num=1&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=&gm=4"
        proxy_res = requests.get(proxy)
        proxy = proxy_res.text.strip()
        return True,proxy
    except:
        return False,""



main_url = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2020/index.html"

timeout = 10
proxies = ""
proxy_result = get_proxy()
proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""


res = requests.get(url=main_url,proxies=proxies)
res.content.decode(encoding=res.apparent_encoding)

# 获取省
soup = BeautifulSoup(res.content,"lxml")
pro_lists = soup.select("table.provincetable tr td a")

pro_datas = []
city_datas = []
region_datas = []
town_datas = []

logger.info(len(pro_lists))
pro_list = [pro_lists[30]]
logger.info(pro_list)
for p in pro_list:

    logger.info(p)

    p_dict = {}
    p_dict["url"] = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2020/"+p.get("href")
    p_dict["name"] = p.text
    p_dict["code"] = p.get("href").split(".")[0]
    logger.info(p_dict["url"])
    logger.info("--------------------------------------")
    # 城市结果
    try:
        city_res = requests.get(url=p_dict["url"],proxies=proxies,timeout=timeout)
    except:
        logger.info("省份换代理")
        time.sleep(2)
        proxy_result = get_proxy()
        proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""
        city_res = requests.get(url=p_dict["url"], proxies=proxies, timeout=timeout)

    city_res.content.decode(encoding=city_res.apparent_encoding)
    city_soup = BeautifulSoup(city_res.content,"lxml")
    city_lists = city_soup.select("table.citytable tr td a")
    for i in range(0,len(city_lists)):
        c_dict = {}
        if i%2 != 0:
            c_dict["url"] = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2020/"+ city_lists[i].get("href")
            c_dict["name"] = city_lists[i].text
            c_dict["code"] = str(city_lists[i].get("href").split("/")[1].split(".")[0])+"0"*8
            c_dict["province_code"] = p_dict["code"]

            #请求区域
            logger.info("area:%s" %c_dict["url"])
            logger.info("c_dict[\"name\"]:%s" %c_dict["name"])
            logger.info("================================")
            try:
                region_res = requests.get(url=c_dict["url"],proxies=proxies,timeout=timeout)
            except:
                logger.info("城市换代理")
                time.sleep(2)
                proxy_result = get_proxy()
                proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""
                region_res = requests.get(url=c_dict["url"], proxies=proxies, timeout=timeout)
            try:
                region_res.content.decode(encoding=region_res.apparent_encoding)
            except:
                region_res.content.decode(encoding='gb18030')
            region_soup = BeautifulSoup(region_res.content,"lxml")
            region_list = region_soup.select("table.countytable tr td a")
            for j in range(0, len(region_list)):
                r_dict = {}
                if j % 2 != 0:
                    r_dict["url"] = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2020/" + str(p_dict["code"]) +"/"+ region_list[j].get("href")
                    r_dict["name"] = region_list[j].text
                    r_dict["code"] = str(region_list[j].get("href").split("/")[1].split(".")[0]) + "0" * 6
                    r_dict["city_code"] = c_dict["code"]
                    r_dict["province_code"] = p_dict["code"]

                    #请求镇
                    # logger.info("zhen:%s" %r_dict["url"])
                    try:
                        town_res = requests.get(url=r_dict["url"],proxies=proxies,timeout=timeout)
                    except:
                        logger.info("区域换代理")
                        time.sleep(2)
                        proxy_result = get_proxy()
                        proxies = {'http': 'http://' + proxy_result[1]} if proxy_result[0] else ""
                        town_res = requests.get(url=r_dict["url"], proxies=proxies, timeout=timeout)

                    try:
                        town_res.content.decode(encoding=town_res.apparent_encoding)
                    except:
                        pass
                    town_soup = BeautifulSoup(town_res.content,"lxml")
                    town_list = town_soup.select("table.towntable tr td a")
                    # logger.info(town_list)
                    for k in range(0, len(town_list)):
                        t_dict = {}
                        if k % 2 != 0:
                            # t_dict["url"] = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2020/" +town_list[k].text[0:2]+"/"+town_list[k].text[2:4] + "/" + town_list[k].get("href")
                            t_dict["name"] = town_list[k].text
                            t_dict["code"] = str(town_list[k].get("href").split("/")[1].split(".")[0]) + "0" * 3
                            t_dict["city_code"] = c_dict["code"]
                            t_dict["province_code"] = p_dict["code"]
                            t_dict["region_code"] = r_dict["code"]
                        else:
                            continue

                        town_datas.append(t_dict)

                    region_datas.append(r_dict)
                else:
                    continue
            city_datas.append(c_dict)
        else:
            continue


    pro_datas.append(p_dict)



logger.info("省：%s" %pro_datas)
logger.info("市:%s" %city_datas)
logger.info("区:%s" %region_datas)
logger.info("镇:%s" %town_datas)

pro_datas = pd.DataFrame(pro_datas)
city_datas = pd.DataFrame(city_datas)
region_datas = pd.DataFrame(region_datas)
town_datas = pd.DataFrame(town_datas)

pro_datas.drop("url",axis=1,inplace=True)
city_datas.drop("url",axis=1,inplace=True)
region_datas.drop("url",axis=1,inplace=True)

conn_analyze = sqlalchemy_conn(analyze_mysql_conf)
pro_datas.to_sql("province",conn_analyze,if_exists="append", index=False)
city_datas.to_sql("city",conn_analyze,if_exists="append", index=False)
region_datas.to_sql("region",conn_analyze,if_exists="append", index=False)
town_datas.to_sql("town",conn_analyze,if_exists="append", index=False)

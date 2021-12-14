# -*- coding: utf-8 -*-
# @Time : 2021/12/6  16:10
# @Author : shihong
# @File : .py
# --------------------------------------
import random
import sys, os, time
import traceback

father_dir = os.path.dirname(__file__).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])

import requests
from bs4 import BeautifulSoup
from lxml import etree
from config import *
from fake_useragent import UserAgent
import re
import json


'''
采集模块： 财经
'''

'''
采集范围：  雪球, 华尔街见闻, 新华网
'''

def get_requests(url):
    headers = {}
    headers["User-Agent"] = random.choice(user_agent_pool)
    # headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
    for i in range(3):
        proxies = random.choice(proxy_pool)
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=30)

            #     proxy_pool.clear()
            #     proxy_pool.append(get_proxy())
            #     # proxy_pool.remove(proxies)
            #     continue
            return response
        except Exception as e:
            logger.error(u'URL：%s请求失败，重新请求' % url)
            logger.info('重新获取代理')
            proxy_pool.clear()
            proxy_pool.append(get_proxy())
            time.sleep(3)

# 获取代理
def get_proxy():
    proxy_url = "http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&pack=125663&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
    proxy_res = requests.get(proxy_url)
    proxy = proxy_res.text.strip()
    proxies = {'http': 'http://'+ proxy}
    logger.info(proxies)
    return proxies

# 消息通知
def save_data(data):
    access_dict = {
        'access_key': 'skv6lYagMGf0nwWB460CzeYiRJdMKn4n',
        'api_url': 'http://xs.lkkjjt.com/open/content/collection'
    }

    logger.info(u'%s进行数据通知' % data["title"])
    headers = {
        'access-key': access_dict['access_key']
    }
    try:
        for i in range(3):
            try:
                response = requests.post(access_dict['api_url'], headers=headers, data=data)
                logger.info(u"%s通知结果：%s" % (data['title'], response.status_code))
                if response.status_code == 200:
                    logger.info(u'%s通知完成' % data["title"])
                    global success_save
                    success_save +=1
                else:
                    logger.info(u'%s通知失败: %s' % (data["title"], response.text))
                break
            except:
                continue
    except Exception as e:
        logger.error(u'%s 通知失败,失败原因：%s' % (data["title"], e))

# 雪球
def xue_qiu(url, source):
    # 排行榜--》今日话题，热榜
    xpath_ele_list = []
    today_topic_response = get_requests(url)
    today_topic_xpath = etree.HTML(today_topic_response.text)
    xpath_ele_list.append(today_topic_xpath)
    # 热帖url获取
    rt_url = base_url + today_topic_xpath.xpath('//h2[@class="cc-dc-Cb"]/a[contains(text(), "热帖")]')[0].xpath('./@href')[0]
    rt_response = get_requests(url=rt_url)
    rt_rank = etree.HTML(rt_response.text)
    xpath_ele_list.append(rt_rank)
    logger.info('对象个数： %s' % len(xpath_ele_list))

    # 获取排行榜下文章url
    all_article_url_list = []
    for xpath_ele in xpath_ele_list:
        article_url_list = [base_url + article_url for article_url in xpath_ele.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[1]//td[@class="al"]//a/@href')]
        logger.info('文章数：%s' % len(article_url_list))
        all_article_url_list.extend(article_url_list)

    logger.info('文章数： %s' % len(all_article_url_list))
    # 遍历所有文章url
    for article_url in all_article_url_list:
        try:
            data = {
                "source": source,
                "type": article_type,
                "ori_url": "",
                "describes": "",
                "tags": "",
                "visits_virtual": "",
                "exposure": "",
                "public_time": "",
            }
            article_response = get_requests(article_url)
            data["ori_url"] = article_response.url
            article_soup = BeautifulSoup(article_response.text, 'lxml')
            data["title"] = article_soup.select('h1.article__bd__title')[0].get_text().strip()
            data["author"] = article_soup.find_all('a', class_='name')[0].get('data-screenname').strip()
            data["body_text"] = str(article_soup.select('div.article__bd__detail')[0])
            img_soup = article_soup.select('div.article__bd__detail p img.ke_img ')
            if img_soup:
                img_list = []
                for img in img_soup:
                    img_url = img.get('src')
                    img_list.append(img_url)
                data["sub_images"] = json.dumps(img_list)[1:-1].replace("\"", '')
            else:
                data["sub_images"] = ""
            logger.info(data)
            # 数据通知
            save_data(data)
        except Exception as e:
            logger.info('爬取异常，异常错误： %s' % e)
            logger.info(article_url)
            time.sleep(10)

# 华尔街见闻
def hua_er_jie(url, source):
    # 排行榜--》日排行，最新资讯，周排行
    # 获取排行榜对象
    xpath_ele_list = []
    logger.info(url)
    start_response = get_requests(url)
    start_ele_xpath = etree.HTML(start_response.text)
    xpath_ele_list.append(start_ele_xpath)
    # 排行榜url
    rank_url_list = [base_url + rank_url for rank_url in start_ele_xpath.xpath('//h2[@class="cc-dc-Cb"]/a/@href')]

    # 获取排行榜xpath对象
    for rank_url in rank_url_list[1:]:
        rank_response = get_requests(rank_url)
        rank_ele_xpath = etree.HTML(rank_response.text)
        xpath_ele_list.append(rank_ele_xpath)

    logger.info('对象个数： %s' % len(xpath_ele_list))

    # 获取排行榜下文章url
    all_article_list = []
    for xpath_ele in xpath_ele_list:
        tr_xpath_list = xpath_ele.xpath('/html/body/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[1]//tr')
        for tr_xpath in tr_xpath_list:
            # 剔除音频
            title = tr_xpath.xpath('./td[@class="al"]/a/text()')[0]
            if 'FM-Radio' in title:
                continue
            article_base = {}
            article_base['url'] = base_url + tr_xpath.xpath('./td[@class="al"]/a/@href')[0]
            article_base['title'] = title
            exposure = tr_xpath.xpath('./td[3]/text()')
            if exposure:
                article_base['exposure'] = exposure[0]
            else:
                article_base['exposure'] = ""
            all_article_list.append(article_base)

    logger.info('文章数： %s' % len(all_article_list))
    # 遍历所有文章url
    for article in all_article_list:
        for i in range(3):
            try:
                data = {
                    "source": source,
                    "type": article_type,
                    "ori_url": "",
                    "describes": "",
                    "tags": "",
                    "visits_virtual": "",
                    "exposure": "",
                    "public_time": "",
                }
                article_response = get_requests(article.get('url'))
                article_soup = BeautifulSoup(article_response.text, 'lxml')
                data["ori_url"] = article_response.url
                data["title"] = article.get('title')
                data['exposure'] = article.get('exposure')
                data["author"] = article_soup.select('span.blk')[0].get_text()
                describes = article_soup.select('div.article-summary')
                if describes:
                    data['describes'] = describes[0].get_text().strip()
                data["body_text"] = str(article_soup.select('div.rich-text')[0])
                img_soup = article_soup.select('div.rich-text img')
                if img_soup:
                    img_list = []
                    for img in img_soup:
                        img_url = img.get('src')
                        img_list.append(img_url)
                    data["sub_images"] = json.dumps(img_list)[1:-1].replace("\"", '')
                else:
                    data["sub_images"] = ""
                save_data(data)
                break
            except Exception as e:
                logger.info('爬取异常，异常错误： %s' % e)
                logger.info(article.get('url'))
                time.sleep(10)
                continue

def xin_hua(url, source):
    start_response = get_requests(url)
    start_ele_xpath = etree.HTML(start_response.text)
    article_url_list = [base_url + article_url for article_url in start_ele_xpath.xpath(
        '/html/body/div[1]/div[2]/div[2]/div[1]/div[2]/div/div[1]//td[@class="al"]//a/@href')]
    logger.info('文章数：%s' % len(article_url_list))
    for article_url in article_url_list[50:]:
        data = {
            "source": '',
            "type": article_type,
            "ori_url": "",
            "describes": "",
            "tags": "",
            "visits_virtual": "",
            "exposure": "",
            "public_time": "",
        }
        try:
            article_response = get_requests(article_url)
            article_response.encoding = 'utf-8'
            data["ori_url"] = article_response.url
            article_soup = BeautifulSoup(article_response.text, 'lxml')
            # 跳过有视频文章
            if article_soup.select('p.video'):
                continue
            data['source'] = article_soup.select('div.source')[0].get_text().strip().split('：')[1]
            data["title"] = article_soup.select('span.title')[0].get_text().strip()
            authors = article_soup.select('span.editor')[0].get_text()
            data["author"] = authors.split(':')[1].split('】')[0].strip() # 【责任编辑:尹世杰】
            year = article_soup.select('span.year')[0].get_text().strip()
            day = article_soup.select('span.day')[0].get_text().strip()
            day = day.replace('/ ', '-')
            _time = article_soup.select('span.time')[0].get_text().strip()
            data['public_time'] = year  + '-' + day + ' ' + _time
            data["body_text"] = str(article_soup.select('div#detail')[0])
            img_soup = article_soup.select('div#detail p img')
            if img_soup:
                ori_url_split = data["ori_url"].split('/')
                img_base_url = 'http://www.news.cn/fortune/' + ori_url_split[4] + '/' + ori_url_split[5] + '/'
                img_list = []
                for img in img_soup:
                    img_url = img.get('src')
                    fina_img_url = img_base_url + img_url
                    # 文章url替换
                    data['body_text'].replace(img_url, fina_img_url)
                    img_list.append(fina_img_url)
                data["sub_images"] = json.dumps(img_list)[1:-1].replace("\"", '')
            else:
                data["sub_images"] = ""
            logger.info(data)
            save_data(data)
            # break
        except Exception as e:
            logger.info('爬取异常，异常错误： %s' % e)
            # logger.info(traceback.format_exc())
            logger.info(article_url)
            time.sleep(10)
            # with open(r'D:/error/%s_%s.html' % (source, int(time.time())), 'w', encoding='utf-8') as f:
            #     f.write(article_response.text)



if __name__ == '__main__':
    success_save = 0
    logger.info('获取代理ip')
    # 代理
    proxy_pool = [{'http': 'http://125.87.95.45:4256'}]
    # 起始请求一个代理
    proxy_pool.append(get_proxy())

    logger.info('获取ua')
    user_agent_pool = []
    for i in range(30):
        ua = UserAgent()
        user_agent_pool.append(ua.random)

    article_type = "财经"
    base_url = 'https://tophub.today'
    base_url_dict = {}
    base_url_dict["雪球"] = 'https://tophub.today/n/X12owXzvNV'
    base_url_dict["新华网"] = 'https://tophub.today/n/KGoRk0xvl6'
    base_url_dict['华尔街见闻'] = 'https://tophub.today/n/G2me3ndwjq'

    for source_name, source_url in base_url_dict.items():
        logger.info(source_name)
        # 获取新闻对象来源
        if source_name == '雪球':
            pass
            # xue_qiu(source_url, source_name)
        elif source_name == '新华网':
            logger.info('开始采集%s数据' % source_name)
            xin_hua(source_url, source_name)
        elif source_name == '华尔街见闻':
            logger.info('开始采集%s数据' % source_name)
            hua_er_jie(source_url, source_name)
    logger.info('成功上传：%s' % success_save)
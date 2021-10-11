from bs4 import BeautifulSoup
from lxml import etree
from qiniu_access import QiniuAccess
from config import *
import requests, re, json, traceback, os, sys, time, hashlib, random


# 获取请求数据
def get_requests(url, mode='other'):
    for i in range(3):
        try:
            response = requests.get(url, timeout=30)
            if mode == 'other':
                return response.text
            elif mode == 'img':
                return response.content
            elif mode == 'xpath':
                return etree.HTML(response.text)
            elif mode == 'json':
                return json.loads(response.text)
        except Exception as e:
            logger.error(f'URL：{url}请求失败，重新请求')
            time.sleep(3)
    return False

# 文章是否包含图片判断及图片url替换
def judge_and_replace_img(soup, prefix_url, pass_word_list=zx_pass_word_list):
    try:
        img_soup_list = soup.select('img')
        article = str(soup)
        if img_soup_list:
            img_replace_list = []
            for img_soup in img_soup_list:
                flag = 0
                if img_soup.get('alt'):
                    for word in pass_word_list:
                        if word in img_soup.get('alt'):
                            flag = 1
                            break
                if flag == 1:
                    continue
                img_url = img_soup.get('src')
                sub_img_url = submit_img(img_url, prefix_url)
                if not sub_img_url:
                    # 图片上传异常，不进行URL替换
                    continue
                img_replace_list.append(sub_img_url)
                article = article.replace(img_url, sub_img_url)
            if img_replace_list:
                return article, img_replace_list[0]
            else:
                return '', None
        else:
            return article, None
    except Exception as e:
        logger.error('替换文章图片url异常，异常信息：%s' % e)
        logger.exception(traceback.format_exc())
        return False

# 广告水印检查
def check_advertising(data, soup, prefix_url):
    # 广告关键词
    try:
        article_list = []
        img_list = []
        for p_soup in soup:
            flag = 0
            for word in zx_pass_word_list:
                try:
                    if '来源' in p_soup.get_text():
                        data['author'] = data['author'] + '/' + p_soup.get_text().split('：')[1]
                except:
                    logger.error('来源获取错误')
                if p_soup.text.find(word) > -1:
                    flag = 1
                    break
            if flag == 1:
                continue
            # 图片验证
            result_list = judge_and_replace_img(p_soup, prefix_url)
            if not result_list[0]:
                # 单个p标签错误
                continue
            if result_list[1]:
                img_list.append(result_list[1])
            article_list.append(str(result_list[0]))
        article = ''.join(article_list)
        return article, img_list, data
    except Exception as e:
        logger.error('检查错误,错误原因：%s' % e)
        return False

# 图片上传
def submit_img(url, prefix_url=""):
    try:
        img_url = prefix_url + url
        img_name = url.split('/')[-1]
        qn = QiniuAccess()
        sub_img_url = qn.upload_img_for_internet(img_url, img_name)
        # print(sub_img_url)
        return sub_img_url
    except Exception as e:
        logger.error('图片上传异常，异常信息：%s' % e)
        logger.exception(traceback.format_exc())
        return False

# 通知数据
def save_data(access_dict, data_list):
    logger.info(f'{access_dict["tag"]}进行数据通知')
    headers = {
        'access-key': access_dict['access_key']
    }
    for data in data_list:
        try:
            for i in range(3):
                requests.post(access_dict['api_url'], headers=headers, data=data)
                break
        except Exception as e:
            logger.error(f'{access_dict["title"]}通知失败,失败原因：%s' % e)
    logger.info(f'{access_dict["tag"]}通知完成')



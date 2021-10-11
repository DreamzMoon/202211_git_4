from bs4 import BeautifulSoup
from lxml import etree
from threading import Thread

from qiniu_access import QiniuAccess
from config import *
import requests, re, json, traceback, os, sys, time, hashlib, random

class ZiXunCollect(object):
    def __init__(self):
        self.access_key = zx_api_access_key
        self.api_url = zx_api_url
        self.collect_page = zx_collect_page

    # 获取请求数据
    def get_requests(self, url, mode='other'):
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
    def judge_and_replace_img(self, soup, prefix_url, pass_word_list):
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
                    sub_img_url = self.submit_img(img_url, prefix_url)
                    if not sub_img_url:
                        # 图片上传异常，不进行URL替换
                        continue
                    img_replace_list.append(sub_img_url)
                    article = article.replace(img_url, sub_img_url)
                print('img_replace_list:%s' % img_replace_list)
                if img_replace_list:
                    return article, img_replace_list[0]
                else:
                    return article, None
            else:
                return article, None
        except Exception as e:
            logger.error('替换文章图片url异常，异常信息：%s' % e)
            logger.exception(traceback.format_exc())
            return False

    # 广告水印检查
    def check_advertising(self, soup, prefix_url):
        # 广告关键词
        try:
            pass_word_list = ['我爱卡', '微信搜索', '公众号', '动态横版二维码']
            article_list = []
            img_list = []
            for p_soup in soup:
                flag = 0
                for word in pass_word_list:
                    if p_soup.text.find(word) > -1:
                        flag = 1
                        break
                if flag == 1:
                    continue
                # 图片验证
                result_list = self.judge_and_replace_img(p_soup, prefix_url, pass_word_list)
                if not result_list[0]:
                    # 单个p标签错误
                    continue
                if result_list[1]:
                    img_list.append(result_list[1])
                # 内容首图,如果失败使用文章第一张,
                article_list.append(str(result_list[0]))
            article = ''.join(article_list)
            return article, img_list
        except:
            return False

    # 图片上传
    def submit_img(self, url, prefix_url=""):
        try:
            img_url = prefix_url + url
            img_name = url.split('/')[-1]
            qn = QiniuAccess()
            sub_img_url = qn.upload_img_for_internet(img_url, img_name)
            # print(sub_img_url)
            return sub_img_url
        except Exception as e:
            logger.error('图片上传异常，异常信息：%s' % e)
            return False

    # 卡新闻
    def card_news(self, tag_url):
        # 图片前缀url
        prefix_url = 'http://www.51kaxun.com'
        # 数据保存
        data_list = []
        try:
            for page in range(1, self.collect_page + 1):
                response = self.get_requests(tag_url % page, mode='xpath')
                li_xpath_list = response.xpath('//ul[@id="news_list"]/li')
                for li in li_xpath_list:
                    data = {
                        'type_id': '14',
                        'title': '',
                        'describes': '',
                        'tags': self.tag,
                        'visits_virtual': random.randint(300, 900),
                        'category_ids': '55',
                        'body_text': '',
                    }
                    describes = li.xpath('./p/text()')[0]
                    data['describes'] = re.sub('\s+', '', describes)
                    article_url = li.xpath('.//a/@href')[0]
                    data['title'] = li.xpath('.//a/text()')[0]
                    article_response = self.get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error(f'网络异常，请求文章数据错误,异常文章URL：{article_url}')
                        continue
                    soup = BeautifulSoup(article_response, 'html.parser')
                    article_soup = soup.select('div.yhym div.right01_nr')[0]
                    result_list = self.judge_and_replace_img(article_soup, prefix_url)
                    if not result_list[0]:
                        logger.error(f'异常文章url: {article_url}')
                        continue
                    # 如果文章有图片
                    if result_list[1]:
                        data['image'] = result_list[1]
                    data['body_text'] = result_list[0]
                    data_list.append(data)
            logger.info(f'采集{self.tag}结束, 条数：{len(data_list)}')
            # 数据入库
            self.save_data(data_list)
        except Exception as e:
            logger.error(f'{self.tag}采集异常，异常信息：{e}')

    # 积分活动或优惠资讯
    def discount_or_interfral(self, tag_url):
        # 图片前缀url
        prefix_url = 'https:'
        data_list = []
        try:
            for page in range(1, self.collect_page + 1):
                # page=1包含1-2页数据
                if page == 2:
                    continue
                response = self.get_requests(tag_url % page, mode='json')
                for info in response['data']['list']:
                    data = {
                        'type_id': '14',
                        'title': '',
                        'describes': '',
                        'tags': self.tag,
                        'visits_virtual': random.randint(300, 900),
                        'category_ids': '56',
                        'body_text': '',
                    }
                    # if self.tag == '优惠资讯':
                    #     data['category_ids'] =
                    data['title'] = info['title']
                    article_url = info['purl']
                    data['describes'] = info['intro']
                    article_response = self.get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error(f'网络异常，请求文章数据错误,异常文章URL：{article_url}')
                        continue
                    soup = BeautifulSoup(article_response, 'lxml')
                    article_soup = soup.select('div.cont p')
                    # ============ 判断是否有广告水印 ============
                    result_list = self.check_advertising(article_soup, prefix_url)
                    if not result_list[0]:
                        logger.error(f'异常文章url: {article_url}')
                        continue
                    # 内容首图,如果失败使用文章第一张,
                    try:
                        imgae = self.submit_img(info['icon'])
                        data['image'] = imgae
                    except:
                        if result_list[1]:
                            data['image'] = result_list[1]
                    data['body_text'] = result_list[0]
                    data_list.append(data)
            logger.info(f'采集{self.tag}结束, 条数：{len(data_list)}')
            # 数据入库
            self.save_data(data_list)
        except Exception as e:
            logger.error(f'{self.tag}采集异常，异常信息：{e}')

    # 理财生活
    def financial(self, tag_url):
        try:
            # 文章前缀
            article_prefix_url = 'https://www.cardbaobao.com'
            # 图片前缀url
            prefix_url = 'https:'
            data_list = []
            for page in range(1, self.collect_page + 1):
                response = self.get_requests(tag_url % page, mode='xpath')
                div_xpath_list = response.xpath('//div[@class="zixun_text"]')
                for div in div_xpath_list:
                    data = {
                        'type_id': '14',
                        'title': '',
                        'describes': '',
                        'tags': self.tag,
                        'visits_virtual': random.randint(300, 900),
                        # 'category_ids': '55',
                        'body_text': '',
                    }
                    url = div.xpath('./a[1]/@href')[0]
                    describes = div.xpath('.//p/text()')[0]
                    data['describes'] = re.sub('\s+', '', describes)
                    article_url = article_prefix_url + url
                    article_response = self.get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error('网络异常，请求文章数据错误')
                        return
                    soup = BeautifulSoup(article_response, 'html.parser')
                    data['title'] = soup.select('h1.detail_title')[0].get_text()
                    article_soup = soup.select('div.text')[0]
                    article = self.judge_and_replace_img(article_soup, prefix_url)
                    if not article:
                        logger.error(f'异常文章url: {article_url}')
                        # return False
                    data['article'] = article
                    data_list.append(data)
            logger.info(f'采集{self.tag}结束, 条数：{len(data_list)}')
            # 数据入库
            self.save_data(data_list)
        except Exception as e:
            logger.error(f'{self.tag}采集异常，异常信息：{e}')

    # 通知数据
    def save_data(self, data_list):
        logger.info(f'{self.tag}进行数据通知')
        # with open(r'C:\Users\V\Desktop\test_img\%s.txt' % self.tag, 'w', encoding='utf-8') as f:
        #     f.write(str(data_list))
        headers = {
            'access-key': self.access_key
        }
        for data in data_list:
            requests.post(self.api_url, headers=headers, data=data)
        logger.info(f'{self.tag}通知成功')

    def start(self):
        logger.info(f'开始采集{self.tag}数据')
        if self.tag == '卡新闻':
            tag_url = 'http://www.51kaxun.com/news/search.php?id=1&p=%s'
            # 图片存放文件夹：只需要子文件夹
            # path = 'cardnews/'
            self.card_news(tag_url)
        elif self.tag == '积分活动' or self.tag == '优惠资讯':
            if self.tag == '积分活动':
                # path = 'intergral_activity/'
                tag_url = 'https://www.51credit.com/info/tag/article?pageNum=%s&tagCode=points'
            elif self.tag == '优惠资讯':
                # path = 'discount/'
                tag_url = 'https://www.51credit.com/info/tag/article?pageNum=%s&tagCode=benefit'
            else:
                return '请输入正确采集标签'
            self.discount_or_interfral(tag_url)
        elif self.tag == '理财生活':
            # path = 'financial_life/'
            tag_url = 'https://www.cardbaobao.com/cardnews/lcsh/list_p%s.html'
            self.financial(tag_url)
        else:
            logger.info('标签输入错误')


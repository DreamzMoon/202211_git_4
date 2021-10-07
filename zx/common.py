from bs4 import BeautifulSoup
from lxml import etree
from threading import Thread
from qiniu_access import QiniuAccess
from config import logger
import requests, re, json, traceback, os, sys, time, hashlib

class ZiXunCollect(object):
    def __init__(self, tag):
        self.tag = tag
        self.collect_page = 10
        # 图片保存地址
        # self.sub_img_base_path = r'C:\Users\V\Desktop\test_img/'

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
                logger.error(e)
                time.sleep(3)
        return False

    # 文章是否包含图片判断及图片url替换
    def judge_and_replace_img(self, soup, prefix_url):
        try:
            img_soup_list = soup.select('img')
            article = str(soup)
            if img_soup_list:
                for img_soup in img_soup_list:
                    img_url = img_soup.get('src')
                    sub_img_url = self.submit_img(prefix_url, img_url)
                    if not sub_img_url:
                        return False
                    article = article.replace(img_url, sub_img_url)
                return article
            else:
                return article
        except Exception as e:
            logger.error('替换文章图片url异常，异常信息：%s' % e)
            return False

    # 图片上传
    def submit_img(self, prefix_url, url):
        try:
            img_url = prefix_url + url
            img_name = url.split('/')[-1]
            # sub_img_url = self.sub_img_base_path + path + img_name
            # img_file = self.get_requests(img_url, 'img')
            # with open(sub_img_url, 'wb') as f:
            #     f.write(img_file)
            qn = QiniuAccess()
            sub_img_url = qn.upload_img_for_internet(img_url, img_name)
            print(sub_img_url)
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
                    article_url = li.xpath('.//a/@href')[0]
                    data = {
                        'tag': self.tag,
                        'title': '',
                        'article': '',
                    }
                    article_response = self.get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error('网络异常，请求文章数据错误')
                        return False
                    data['title'] = re.findall('"nr01_tit">(.*?)</h1', article_response)[0]
                    soup = BeautifulSoup(article_response, 'html.parser')
                    article_soup = soup.select('div.yhym div.right01_nr')[0]
                    article = self.judge_and_replace_img(article_soup, prefix_url)
                    if not article:
                        logger.error(f'异常文章url: {article_url}')
                        return False
                    data['article'] = article
                    data_list.append(data)
            logger.info(f'采集{self.tag}结束, 条数：{len(data_list)}')
            # 数据入库
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
                    article_url = info['purl']
                    data = {
                        'tag': self.tag,
                        'title': '',
                        'article': '',
                    }
                    article_response = self.get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error('网络异常，请求文章数据错误')
                        return
                    soup = BeautifulSoup(article_response, 'html.parser')
                    data['title'] = soup.select('div.cont h2')[0].get_text()
                    article_soup = soup.select('div.cont p')[0]
                    article = self.judge_and_replace_img(article_soup, prefix_url)
                    if not article:
                        logger.error(f'异常文章url: {article_url}')
                        return False
                    data['article'] = article
                    data_list.append(data)
            logger.info(f'采集{self.tag}结束, 条数：{len(data_list)}')
            # 数据入库
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
                    url = div.xpath('./a[1]/@href')[0]
                    article_url = article_prefix_url + url
                    data = {
                        'tag': self.tag,
                        'title': '',
                        'article': '',
                    }
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
                        return False
                    data['article'] = article
                    data_list.append(data)
            logger.info(f'采集{self.tag}结束, 条数：{len(data_list)}')
        except Exception as e:
            logger.error(f'{self.tag}采集异常，异常信息：{e}')

    def save_data(self):
        pass

    def start(self):
        logger.info(f'开始采集{self.tag}数据')
        if self.tag == '"卡"新闻':
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


if __name__ == '__main__':
    tag_list = ['"卡"新闻', '积分活动', '优惠资讯', '理财生活']
    t_list = []
    for tag in tag_list:
        collect = ZiXunCollect(tag)
        t = Thread(target=collect.start)
        t_list.append(t)
    for t in t_list:
        t.start()
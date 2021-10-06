import traceback

import requests
import re
import json
from bs4 import BeautifulSoup
from lxml import etree

class ZiXunCollect(object):
    def __init__(self, tag):
        self.tag = tag
        self.collect_page = 10
        self.sub_img_path = r'C:\Users\V\Desktop\test_img/'

    def get_requests(self, url, mode='other'):
        for i in range(3):
            try:
                response = requests.get(url, timeout=20)
                if mode == 'other':
                    return response.text
                elif mode == 'img':
                    return response.content
                elif mode == 'xpath':
                    return etree.HTML(response.text)
            except:
                print('获取请求超时，重新请求')
                continue
        return False

    def get_one_page(self):
        if self.tag == '"卡"新闻':
            pass
        elif self.tag == '积分活动' or self.tag == '优惠资讯':
            pass
        elif self.tag == '理财生活':
            pass

    def get_two_page_detail(self):
        pass

    def download_img(self, base_url, url):
        img_url = base_url + url
        img_name = url.split('/')[-1]
        our_img_url = img_base_path + img_name
        img_file = self.get_requests(img_url, 'img')
        with open(our_img_url, 'wb') as f:
            f.write(img_file)
        return our_img_url

    def save_data(self):
        pass

    def start(self):
        pass

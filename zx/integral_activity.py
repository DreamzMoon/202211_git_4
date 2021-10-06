import requests
import re
import json
from bs4 import BeautifulSoup
from lxml import etree

tag = '积分活动'
base_url = 'https:'
img_base_path = r'C:\Users\V\Desktop\test_img/intergral_activity/'
collect_page = 2

def get_requests(url, mode='xpath'):
    html = requests.get(url, timeout=20)
    if mode == 'img':
        return html.content
    elif mode == 'xpath':
        response = etree.HTML(html.text)
        return response
    elif mode == 'other':
        return html.text

def get_one_page(response):
    two_url_list = []
    response = json.loads(response)
    for info in response['data']['list']:
        two_url_list.append(info['purl'])
    return two_url_list

def get_two_page_detail(url_list):
    # 数据存储
    data_list = []
    for index, url in enumerate(url_list):
        data = {
            'tag': tag,
            'title': '',
            'article': '',
        }
        response = get_requests(url, mode='other')
        soup = BeautifulSoup(response, 'html.parser')
        data['title'] = soup.select('div.cont h2')[0].get_text()
        article_soup = soup.select('div.cont p')[0]
        if article_soup.select('img'):
            img_soup_list = article_soup.select('img')
            article = str(article_soup)
            for img_soup in img_soup_list:
                img_url = img_soup.get('src')
                our_img_url = download_img(img_url)
                article = article.replace(img_url, our_img_url)
                print(our_img_url)
        else:
            article = str(article_soup)
        data['article'] = article
        print('第%s个： %s' % (index,data))
        print('-' * 40)
        data_list.append(data)
    return data_list

def download_img(url):
    img_url = base_url + url
    img_name = url.split('/')[-1]
    our_img_url = img_base_path + img_name
    img_file = get_requests(img_url, 'img')
    with open(our_img_url, 'wb') as f:
        f.write(img_file)
    return our_img_url

if __name__ == '__main__':
    for i in range(1, collect_page + 1):
        if i == 2:
            continue
        one_url = 'https://www.51credit.com/info/tag/article?pageNum=%s&tagCode=points' % i
        response = get_requests(one_url, mode='other')
        two_url_list = get_one_page(response)
        data_list = get_two_page_detail(two_url_list)
        print(len(data_list))
        print('-' * 40)
# -*- coding: utf-8 -*-

from common import *

# 卡新闻
class CardNews(object):
    def card_news(self, access_dict, tag_url):
        # 图片前缀url
        prefix_url = 'http://www.51kaxun.com'
        # 数据保存
        data_list = []
        try:
            for page in range(1, zx_collect_page + 1):
                response = get_requests(tag_url % page, mode='xpath')
                li_xpath_list = response.xpath('//ul[@id="news_list"]/li')
                for li in li_xpath_list:
                    data = {
                        'type_id': '14',
                        'title': '',
                        'author': '',
                        'describes': '',
                        'tags': access_dict['tag'],
                        'visits_virtual': random.randint(300, 900),
                        'category_ids': access_dict['category_ids'],
                        'body_text': '',
                    }
                    describes = li.xpath('./p/text()')[0]
                    data['describes'] = re.sub('\s+', '', describes)
                    article_url = li.xpath('.//a/@href')[0]
                    data['title'] = li.xpath('.//a/text()')[0]
                    logger.info('title: %s' % data['title'])
                    article_response = get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error(u'网络异常，请求文章数据错误,异常文章URL：%s' % article_url)
                        continue
                    soup = BeautifulSoup(article_response, 'html.parser')
                    author_soup = soup.select('div.yhym p.right01_date')
                    if author_soup:
                        try:
                            authors = author_soup[0].get_text()
                            author = authors.split('：')[2][:4]
                            data['author'] = author
                        except:
                            pass
                    else:
                        pass
                    article_soup = soup.select('div.yhym div.right01_nr')[0]
                    result_list = judge_and_replace_img(article_soup, prefix_url)
                    if not result_list[0]:
                        logger.error(u'异常文章url: %s' % article_url)
                        continue
                    # 如果文章有图片
                    if result_list[1]:
                        data['image'] = result_list[1]
                    data['body_text'] = result_list[0]
                    data_list.append(data)
            logger.info(u'采集%s结束, 条数：{len(data_list)}' % access_dict["tag"])
            # 数据入库
            save_data(access_dict, data_list)
        except Exception as e:
            logger.error(u'%s采集异常，异常信息：%s' % (access_dict["tag"], e))
            logger.exception(traceback.format_exc())



# -*- coding: utf-8 -*-

from common import *

# 理财生活
class FinancialLife(object):
    def financial(self, access_dict, tag_url):
        try:
            # 文章前缀
            article_prefix_url = 'https://www.cardbaobao.com'
            # 图片前缀url
            prefix_url = 'https:'
            data_list = []
            for page in range(1, zx_collect_page + 1):
                response = get_requests(tag_url % page, mode='xpath')
                div_xpath_list = response.xpath('//div[@class="zixun_text"]')
                for div in div_xpath_list:
                    data = {
                        'type_id': zx_type_id,
                        'title': '',
                        'describes': '',
                        'tags': access_dict["tag"],
                        'visits_virtual': random.randint(300, 900),
                        'category_ids': access_dict['category_ids'],
                        'body_text': '',
                    }
                    url = div.xpath('./a[1]/@href')[0]
                    describes = div.xpath('.//p/text()')[0]
                    data['describes'] = re.sub('\s+', '', describes)
                    article_url = article_prefix_url + url
                    article_response = get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error(u'网络异常，请求文章数据错误,异常文章URL：%s' % article_url)
                        return
                    soup = BeautifulSoup(article_response, 'html.parser')
                    data['title'] = soup.select('h1.detail_title')[0].get_text()
                    logger.debug('title: %s' % data['title'])
                    article_soup = soup.select('div.text')[0]
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
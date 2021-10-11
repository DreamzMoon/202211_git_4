# -*- coding: utf-8 -*-

from common import *

# 积分活动或优惠资讯
class DiscountOrInterfral(object):
    def discount_or_interfral(self, access_dict, tag_url):
        # 图片前缀url
        prefix_url = 'https:'
        data_list = []
        try:
            for page in range(1, zx_collect_page + 1):
                # page=1包含1-2页数据
                # if page == 2:
                #     continue
                response = get_requests(tag_url % page, mode='json')
                for info in response['data']['list']:
                    data = {
                        'type_id': '14',
                        'title': '',
                        'describes': '',
                        'tags': access_dict["tag"],
                        'visits_virtual': random.randint(300, 900),
                        'category_ids': access_dict['category_ids'],
                        'body_text': '',
                    }
                    data['title'] = info['title']
                    article_url = info['purl']
                    data['describes'] = info['intro']
                    article_response = get_requests(article_url, mode='other')
                    if not article_response:
                        logger.error(f'网络异常，请求文章数据错误,异常文章URL：{article_url}')
                        continue
                    soup = BeautifulSoup(article_response, 'lxml')
                    article_soup = soup.select('div.cont p')
                    # ============ 判断是否有广告水印 ============
                    result_list = check_advertising(article_soup, prefix_url)
                    if not result_list[0]:
                        logger.error(f'异常文章url: {article_url}')
                        continue
                    # 内容首图,如果失败使用文章第一张,
                    try:
                        imgae = submit_img(info['icon'])
                        data['image'] = imgae
                    except:
                        if result_list[0]:
                            data['image'] = result_list[1]
                    data['body_text'] = result_list[0]
                    data_list.append(data)
            logger.info(f'采集{access_dict["tag"]}结束, 条数：{len(data_list)}')
            # 数据入库
            save_data(access_dict, data_list)
        except Exception as e:
            logger.error(f'{access_dict["tag"]}采集异常，异常信息：{e}')
            logger.exception(traceback.format_exc())
# -*- coding: utf-8 -*-

import sys
sys.path.append("../")

from config import *
from cardnews import CardNews
from discount_intergral import DiscountOrInterfral
from financial_life import FinancialLife
from threading import Thread

# 资讯采集
class ZiXunCollect(object):
    def __init__(self, tag):
        self.access_dict = {
            'tag': tag,
            'access_key': zx_api_access_key,
            'api_url': zx_api_url
        }

    # 卡新闻
    def card_news(self):
        tag_url = 'http://www.51kaxun.com/news/search.php?id=1&p=%s'
        self.access_dict['category_ids'] = card_news_category_ids
        CardNews().card_news(self.access_dict, tag_url)

    # 积分活动或优惠资讯
    def discount_or_interfral(self):
        if self.access_dict["tag"] == '积分活动':
            tag_url = 'https://www.51credit.com/info/tag/article?pageNum=%s&tagCode=points'
            self.access_dict['category_ids'] = intergral_category_ids
        elif self.access_dict["tag"] == '优惠资讯':
            tag_url = 'https://www.51credit.com/info/tag/article?pageNum=%s&tagCode=benefit'
            self.access_dict['category_ids'] = discount_category_ids
        else:
            return
        DiscountOrInterfral().discount_or_interfral(self.access_dict, tag_url)

    # 理财生活
    def financial(self):
        tag_url = 'https://www.cardbaobao.com/cardnews/lcsh/list_p%s.html'
        self.access_dict['category_ids'] = financil_activity_category_ids
        FinancialLife().financial(self.access_dict, tag_url)

    def main(self):
        logger.info(u'开始采集%s数据' % self.access_dict["tag"])
        if self.access_dict["tag"] == '卡新闻':
            self.card_news()
        elif self.access_dict["tag"] == '积分活动' or self.access_dict["tag"] == '优惠资讯':
            self.discount_or_interfral()
        elif self.access_dict["tag"] == '理财生活':
            self.financial()
        else:
            logger.info('标签输入错误')

if __name__ == '__main__':
    if ENV == 'pro':
        tag_list = ['卡新闻', '积分活动', '优惠资讯', '理财生活']
        t_list = []
        for tag in tag_list:
            collect = ZiXunCollect(tag)
            t = Thread(target=collect.main)
            t_list.append(t)
        for t in t_list:
            t.start()
    else:
        tag = '积分活动'
        collect = ZiXunCollect(tag)
        collect.main()


# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class KkkItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    #pass
    id=scrapy.Field()
    url=scrapy.Field()
    name=scrapy.Field()
    state=scrapy.Field()
    type=scrapy.Field()
    time=scrapy.Field()
    author=scrapy.Field()
    chapter=scrapy.Field()

"""
    章节信息
"""
class Chapter():
    id=""
    chid=""
    page=[]
"""
    页数信息
"""
class Page():
    id=""
    imageurl=""

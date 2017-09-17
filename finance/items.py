# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class EODQuote(scrapy.Item):
    ticker = scrapy.Field()
    name = scrapy.Field()
    date = scrapy.Field(serializer=str)
    exchange = scrapy.Field()
    open = scrapy.Field()
    close = scrapy.Field()
    high = scrapy.Field()
    low = scrapy.Field()
    volume = scrapy.Field()
    source = scrapy.Field()

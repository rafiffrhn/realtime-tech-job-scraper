import scrapy

class JobScraperItem(scrapy.Item):
    title = scrapy.Field()
    company = scrapy.Field()
    location = scrapy.Field()
    date_posted = scrapy.Field()
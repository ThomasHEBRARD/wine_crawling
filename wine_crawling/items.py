import scrapy

class WineItem(scrapy.Item):
    name = scrapy.Field(allow_null=True)
    color = scrapy.Field(allow_null=True)
    appellation = scrapy.Field(allow_null=True)
    domaine = scrapy.Field(allow_null=True)
    producteur = scrapy.Field(allow_null=True)
    cepage = scrapy.Field(allow_null=True)
    millesime = scrapy.Field(allow_null=True)
    pays = scrapy.Field(allow_null=True)
    note_wd = scrapy.Field(allow_null=True)
    note_wa = scrapy.Field(allow_null=True)
    note_ws = scrapy.Field(allow_null=True)
    note_jmq = scrapy.Field(allow_null=True)
    price = scrapy.Field(allow_null=True)
    classement = scrapy.Field(allow_null=True)
    url = scrapy.Field(allow_null=True)
import scrapy

class WineItem(scrapy.Item):
    name = scrapy.Field(allow_null=True)
    pays = scrapy.Field(allow_null=True)
    domaine = scrapy.Field(allow_null=True)
    producteur = scrapy.Field(allow_null=True)
    appellation = scrapy.Field(allow_null=True)
    millesime = scrapy.Field(allow_null=True)

    color = scrapy.Field(allow_null=True)
    cepage = scrapy.Field(allow_null=True)
    degre_alcool = scrapy.Field(allow_null=True)
    viticulture = scrapy.Field(allow_null=True)
    apogee = scrapy.Field(allow_null=True)
    
    price = scrapy.Field(allow_null=True)

    classement = scrapy.Field(allow_null=True)
    note_wa = scrapy.Field(allow_null=True)
    note_wd = scrapy.Field(allow_null=True)
    note_ws = scrapy.Field(allow_null=True)
    note_jmq = scrapy.Field(allow_null=True)

    website = scrapy.Field(allow_null=True)
    url = scrapy.Field(allow_null=True)

    def get_table_name(self):
        return "public.crawled_bottles"

    def get_table_creation_query(self):
        return f""" CREATE TABLE IF NOT EXISTS {self.get_table_name()}
            (
                name VARCHAR(400),
                pays VARCHAR(400),
                domaine VARCHAR(400),
                producteur VARCHAR(400),
                appellation VARCHAR(400),
                millesime VARCHAR(400),
                color VARCHAR(400), 
                cepage VARCHAR(400),
                degre_alcool VARCHAR(400),
                viticulture VARCHAR(400),
                apogee VARCHAR(400),
                price VARCHAR(400),
                classement VARCHAR(400),
                note_wa VARCHAR(400),
                note_wd VARCHAR(400),
                note_ws VARCHAR(400),
                note_jmq VARCHAR(400),
                website VARCHAR(400),
                url VARCHAR(400)
            );
        """
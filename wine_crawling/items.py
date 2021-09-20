import scrapy


class WineItem(scrapy.Item):
    id = scrapy.Field(allow_null=False)
    website_id = scrapy.Field(allow_null=True)
    name = scrapy.Field(allow_null=True)
    vintage = scrapy.Field(allow_null=True)
    domaine = scrapy.Field(allow_null=True)
    country = scrapy.Field(allow_null=True)
    region = scrapy.Field(allow_null=True)
    appellation = scrapy.Field(allow_null=True)
    soil = scrapy.Field(allow_null=True)
    color = scrapy.Field(allow_null=True)
    bottle_size = scrapy.Field(allow_null=True)
    grape = scrapy.Field(allow_null=True)
    viticulture = scrapy.Field(allow_null=True)
    apogee = scrapy.Field(allow_null=True)
    garde = scrapy.Field(allow_null=True)
    alcool = scrapy.Field(allow_null=True)
    price = scrapy.Field(allow_null=True)
    ranking = scrapy.Field(allow_null=True)
    image = scrapy.Field(allow_null=True)
    url = scrapy.Field(allow_null=True)
    website = scrapy.Field(allow_null=True)

    def get_table_name(self):
        return "public.crawled_bottles"

    def get_table_creation_query(self):
        return f""" CREATE TABLE IF NOT EXISTS {self.get_table_name()}
            (
                id serial NOT NULL PRIMARY KEY,
                website_id VARCHAR(400),
                name VARCHAR(400),
                vintage VARCHAR(400),
                domaine VARCHAR(400),
                country VARCHAR(400),
                region VARCHAR(400),
                appellation VARCHAR(400),
                soil VARCHAR(400),
                color VARCHAR(400),
                bottle_size VARCHAR(400),
                grape VARCHAR(400),
                viticulture VARCHAR(400),
                apogee VARCHAR(400),
                garde VARCHAR(400),
                alcool VARCHAR(400),
                price VARCHAR(400),
                ranking VARCHAR(400),
                image VARCHAR(400),
                url VARCHAR(400),
                website VARCHAR(400)
            );
        """

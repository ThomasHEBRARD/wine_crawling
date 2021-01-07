import scrapy
import re
from wine_crawling.items import WineItem
import json

class ComptoirDesMillesimesSpider(scrapy.Spider):
    name = "comptoirdesmillesimes"
    domain_name = "https://www.comptoirdesmillesimes.com"

    start_urls = [
        "https://www.comptoirdesmillesimes.com/toute-la-cave"
    ]

    custom_settings = {
        # "DOWNLOAD_DELAY": "3",
        # "AUTOTHROTTLE_ENABLED": "True",
        # "AUTOTHROTTLE_START_DELAY": "3",
        # "AUTOTHROTTLE_MAX_DELAY": "3",
        # "AUTOTHROTTLE_TARGET_CONCURRENCY": "1.0",
        # "ITEM_PIPELINES": {"wine_crawling.pipelines.WinePipeline": 300,}
    }

    BASE_HEADER = {
        "Cache-Control": "max-age=0",
        "origin": "https://www.comptoirdesmillesimes.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Referer": "https://www.comptoirdesmillesimes.com/toute-la-cave",
        "Sec-Fetch-Mode": "navigate",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    def errback_debug(self, failure):
        self.logger.error(repr(failure))
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error("HttpError on %s", response.url)

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error("DNSLookupError on %s", request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error("TimeoutError on %s", request.url)
            

    def parse(self, response):
        last_page = response.xpath(
            '//ul[@class="page-list pagination"]/li[14]/a/@href'
        ).extract_first()
        last_page = int(re.search(r"page=(.*)", last_page).group(1))

        current_page = int(response.xpath(
            '//li[@class="page-item active"]/a/text()[1]'
        ).extract_first())
        
        if current_page <= last_page:
            try:
                next_page = int(response.xpath(
                    '//li[@class="page-item active"]/following-sibling::li/a/text()[1]'
                ).extract_first())
                next_page_url = response.xpath(
                    '//li[@class="page-item active"]/following-sibling::li/a/@href'
                ).extract_first()

                if current_page > 1:
                    yield scrapy.Request(
                        url=next_page_url,
                        callback=self.parse,
                        errback=self.errback_debug
                    )
            except:
                pass

            for url in response.xpath("//h3[@class='product-title']/a/@href").extract():
                yield scrapy.Request(
                    url=url,
                    headers=self.BASE_HEADER,
                    callback=self.build_item,
                    errback=self.errback_debug,
                )

    def build_item(self, response):
        iterm = WineItem()
        print(json.loads(response.xpath(
            "//div[@class='tab-pane fade show active']/@data-product"
        ).extract_first()).values())
        
        # item = WineItem()
        # item["name"] = response.xpath(
        #     "//div[@class='description-1']//h1/text()"
        # ).extract_first()
        # capital = response.xpath("//div[@class='critere']/ul/li[@class='capital']")
        # info = response.xpath(
        #     "//section[@id='descriptif']//article[@class='prez-3']/ul/li"
        # )
        # item["url"] = response.url
        # for j in info:
        #     text = j.xpath("./strong/text()").extract_first()
        #     if "Pays / Region" in text:
        #         item["pays"] = j.xpath("./text()").extract_first()
        #     if "Couleur" in text:
        #         item["color"] = j.xpath("./text()").extract_first()
        #     if "Appellation" in text:
        #         item["appellation"] = j.xpath("./text()").extract_first()
        #     if "Propriétaire" in text:
        #         item["domaine"] = j.xpath("./text()").extract_first()
        #     if "Millesime" in text:
        #         item["millesime"] = j.xpath("./text()").extract_first()
        #     if "Classement" in text:
        #         item["classement"] = j.xpath("./text()").extract_first()
        #     if "Encepagement" in text:
        #         list_cepage = []
        #         if j.xpath(".//a").extract() is not None:
        #             for k in j.xpath(".//a"):
        #                 list_cepage.append(k.xpath("./text()").extract_first())
        #             item["cepage"] = '/'.join(list_cepage) if '/'.join(list_cepage) != None else ''
        #         else:
        #             item["cepage"] = ''
        # item["price"] = response.xpath(
        #     "//span[@id='input_prix']/text()"
        # ).extract_first()
        # # print(item.__dict__)
        # yield item
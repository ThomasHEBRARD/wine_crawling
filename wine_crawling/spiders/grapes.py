import scrapy
import re
from wine_crawling.items import GrapeItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError


class IdealWineSpider(scrapy.Spider):
    name = "grape_wikipedia"
    domain_name = "https://www.wikipedia.com"

    start_urls = [
        "https://fr.wikipedia.org/wiki/Liste_des_c%C3%A9pages_par_ordre_alphab%C3%A9tique",
    ]

    custom_settings = {
        "ITEM_PIPELINES": {
            "wine_crawling.pipelines.GrapePipeline": 300,
        }
    }

    BASE_HEADER = {
        "Cache-Control": "max-age=0",
        "origin": "https://www.wikipedia.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Referer": "https://fr.wikipedia.org/wiki/Liste_des_c%C3%A9pages_par_ordre_alphab%C3%A9tique",
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
        url = self.start_urls[0]
        yield scrapy.Request(
            url=url,
            headers=self.BASE_HEADER,
            callback=self.build_item,
            errback=self.errback_debug,
        )

    def build_item(self, response):
        item = GrapeItem()
        all_names = response.xpath("//tbody/tr/td[1]/a/text()").extract()
        print(all_names)
        all_variantes = response.xpath("//tbody/tr/td[1]/ul/li/text()").extract()
        print(all_variantes)
        yield item

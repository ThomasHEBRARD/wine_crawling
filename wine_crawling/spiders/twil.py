import scrapy
import re
from wine_crawling.items import WineItem


class TwilSpider(scrapy.Spider):
    name = "twil"
    domain_name = "https://www.twil.fr"

    custom_settings = {
        "ITEM_PIPELINES": {
            "wine_crawling.pipelines.WinePipeline": 300,
        }
    }

    BASE_HEADER = {
        "Cache-Control": "max-age=0",
        "origin": "https://www.twil.fr",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Referer": "https://www.twil.fr/france.html?limit=36&p=1",
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
        next_page = response.xpath(
            '//a[@class=" border-nav next_page_link next i-next"]/@href'
        ).extract_first()

        print(next_page)
        next_page = re.search(r"p=(.*)", next_page).group(1)

        next_page_url = "https://www.twil.fr/france.html?limit=36&p=%s" % next_page

        if next_page is not None:
            yield scrapy.Request(
                url=next_page_url, callback=self.parse, errback=self.errback_debug
            )
        
        for url in response.xpath("//a[@class='no-text-decoration no_focus']/@href").extract():
            yield scrapy.Request(
                url=url,
                headers=self.BASE_HEADER,
                callback=self.build_item,
                errback=self.errback_debug,
            )

    def build_item(self, response):
        item = WineItem()
        item["website"] = "twil"
        item["name"] = response.xpath("//h1[@class='wine-name']/text()").extract_first().strip()
        item["millesime"] = response.xpath("//span[@itemprop='productionDate']/text()").extract_first() # string for now
        item["appellation"] = response.xpath("//a[@class='default_color']/text()").extract_first()
        item["color"] = response.xpath("//span[@class='badge-color fs-14 gotham-book']/span/@class").get().split('-')[1]
        item["apogee"] = response.xpath("//div[@class='apogee col-xs-12 col-sm-4']/p/text()").extract()[1].strip()
        item["cepage"] = response.xpath("//div[@class='cepages col-xs-12 col-sm-4']/h2/span/text()").extract()[1].strip()
        item["terroir"] = response.xpath("//div[@class='terroir col-xs-12 col-sm-4']/h2/span/text()").extract()[1].strip()
        item["viticulture"] = response.xpath("//div[@class='culture col-xs-12 col-sm-4']/p/text()").extract()[1].strip()
        item["degree_alcool"] = response.xpath("//div[@class='alcool col-xs-12 col-sm-4']/p/text()").extract()[1].strip()

        item["url"] = response.url

        yield item

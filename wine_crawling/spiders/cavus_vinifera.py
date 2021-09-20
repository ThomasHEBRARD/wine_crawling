import scrapy
import re
from wine_crawling.items import WineItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError


class TwilSpider(scrapy.Spider):
    name = "cavusvinifera"
    domain_name = "http://cavusvinifera.com/fr/"

    start_urls = ["http://www.cavusvinifera.com/fr/liste-chateaux.php"]

    custom_settings = {
        "DOWNLOAD_DELAY": "1",
        "ITEM_PIPELINES": {
            "wine_crawling.pipelines.WinePipeline": 300,
        },
    }

    BASE_HEADER = {
        "Cache-Control": "max-age=0",
        "origin": "http://cavusvinifera.com/fr/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Referer": "http://www.cavusvinifera.com/fr/liste-chateaux.php",
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

        next_page = re.search(r"p=(.*)", next_page).group(1)

        next_page_url = "https://www.twil.fr/france.html?p=%s" % next_page
        print("next_page", next_page_url)

        all_bottles_url = response.xpath(
            "//a[@class='no-text-decoration no_focus']/@href"
        ).extract()

        for url in all_bottles_url:
            yield scrapy.Request(
                url=url,
                headers=self.BASE_HEADER,
                callback=self.build_item,
                errback=self.errback_debug,
            )

        if next_page is not None:
            yield scrapy.Request(
                url=next_page_url,
                headers=self.BASE_HEADER,
                callback=self.parse,
                errback=self.errback_debug,
            )

    def build_item(self, response):
        item = WineItem()
        item["website"] = "twil"
        item["country"] = "france"
        try:
            # Website_id
            item["website_id"] = response.xpath("//input[@name='product']/@value").get()

            # Bottle name
            item["name"] = (
                response.xpath("//h1[@class='wine-name']/text()")
                .extract_first()
                .strip()
            )
            # Vintage
            item["vintage"] = response.xpath(
                "//span[@itemprop='productionDate']/text()"
            ).extract_first()  # string for now

            print(item["name"], item["vintage"])
            # Domaine
            item["domaine"] = response.xpath("//span[@itemprop='brand']/text()").get()

            # Region
            item["region"] = response.xpath(
                "//div[@class='breadcrumbs hidden-xs']/ul/li/a/text()"
            )[2].get()

            # Bottle size
            bottle_size = response.xpath(
                "//span[@class='badge-color fs-14 gotham-book']/text()"
            ).extract()[-1]
            item["bottle_size"] = re.search(r"(.*)cl", bottle_size).group(1).strip()

            # Appellation
            item["appellation"] = response.xpath(
                "//a[@class='default_color']/text()"
            ).extract_first()

            # Ranking
            item["ranking"] = response.xpath(
                "//div[@class='col-md-6 col-xs-6 rate-stars']/div/div/div/@data-rateit-value"
            ).get()

            # Price
            item["price"] = response.xpath(
                "//div[@class='tarif']//span[@itemprop='price']/@content"
            ).get()

            # Image
            item["image"] = (
                response.xpath("//img[@id='image-main']/@src").get()
                + ","
                + response.xpath("//img[@id='image-0']/@src").get()
            )

            # Color
            item["color"] = (
                response.xpath(
                    "//span[@class='badge-color fs-14 gotham-book']/span/@class"
                )
                .get()
                .split("-")[1]
            )

            # Apogee
            item["apogee"] = (
                response.xpath("//div[@class='apogee col-xs-12 col-sm-4']/p/text()")
                .extract()[1]
                .strip()
            )

            # Grape
            item["grape"] = (
                response.xpath(
                    "//div[@class='cepages col-xs-12 col-sm-4']/h2/span/text()"
                )
                .extract()[1]
                .strip()
            )

            # Soil
            item["soil"] = (
                response.xpath(
                    "//div[@class='terroir col-xs-12 col-sm-4']/h2/span/text()"
                )
                .extract()[1]
                .strip()
            )

            # Viticulture
            item["viticulture"] = (
                response.xpath("//div[@class='culture col-xs-12 col-sm-4']/p/text()")
                .extract()[1]
                .strip()
            )

            # Alcool
            item["alcool"] = (
                response.xpath("//div[@class='alcool col-xs-12 col-sm-4']/p/text()")
                .extract()[1]
                .strip()
            )

            # Url
            item["url"] = response.url
        except:
            pass

        yield item

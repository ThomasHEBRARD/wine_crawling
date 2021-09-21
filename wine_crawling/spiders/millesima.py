import scrapy
import re
import numpy as np
from wine_crawling.items import WineItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError


class MillesimaSpider(scrapy.Spider):
    name = "millesima"
    domain_name = "https://www.millesima.fr"

    start_urls = ["https://www.millesima.fr/tous-nos-vins.html"]

    custom_settings = {
        "DOWNLOAD_DELAY": "0.7",
        "ITEM_PIPELINES": {
            "wine_crawling.pipelines.WinePipeline": 300,
        },
    }

    BASE_HEADER = {
        "Cache-Control": "max-age=0",
        "origin": "https://www.millesima.fr",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Referer": "https://www.millesima.fr/tous-nos-vins.html",
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
        all_bottles_url = [
            self.domain_name + "/" + url
            for url in response.xpath(
                "//a[@class='MillesimaLink-module__inline-link MillesimaLink-module__large']/@href"
            ).extract()
        ]

        for url in all_bottles_url:
            yield scrapy.Request(
                url=url,
                headers=self.BASE_HEADER,
                callback=self.build_item,
                errback=self.errback_debug,
            )

        current_page = int(
            response.xpath(
                "//ul[@class='PaginationV2-module__inline-list']/li[@class='PaginationV2-module__page-item PaginationV2-module__active']/text()"
            ).get()
        )

        next_page_url = "https://www.millesima.fr/tous-nos-vins.html?page=%s" % str(
            current_page + 1
        )
        print("curent page", current_page)

        if current_page is not None:
            yield scrapy.Request(
                url=next_page_url,
                headers=self.BASE_HEADER,
                callback=self.parse,
                errback=self.errback_debug,
            )

    def build_item(self, response):
        item = WineItem()
        item["website"] = "millesima"

        try:
            # Website_id

            # Bottle name
            item["name"] = response.xpath(
                "//div[@class='product-name']/h1/text()"
            ).get()
            print(item["name"])

            # Vintage
            vintage = response.xpath(
                "//ul[@class='vintage-box-content']/li[@class='active']/a/text()"
            ).get()

            if vintage:
                item["vintage"] = vintage
            else:
                item["vintage"] = item["name"].split(" ")[-1].replace('"', "")

            # Price
            if response.xpath("//div[@class='product-price']/span[@class='details']"):
                item["price"] = (
                    response.xpath(
                        "//div[@class='product-price']/span[@class='details']/span/text()"
                    )
                    .extract()[-1]
                    .split("\xa0")[0]
                    .replace(" ", "")
                )
            else:
                item["price"] = (
                    response.xpath("//span[@class='offer-price']/text()")
                    .get()
                    .split("\xa0")[0]
                    .replace(" ", "")
                )

            # Image
            image = response.xpath(
                "//div[@class='product-img product-full']/img/@src"
            ).get()
            if image:
                item["image"] = image
            else:
                item["image"] = response.xpath(
                    "//div[@class='product-img product-rotate']/div/img/@src"
                ).get()

            details_key = response.xpath(
                "//dl[@class='row list-group-flush mx-auto w-75']/dt/text()"
            ).extract()
            details_value = response.xpath(
                "//dl[@class='row list-group-flush mx-auto w-75']/dd/text()"
            ).extract()
            details = np.vstack((details_key, details_value)).T

            for key, value in details:
                # Domaine
                if key == "Producteur":
                    item["domaine"] = value

                # Region
                if key == "Région":
                    item["region"] = value

                # Country
                if key == "Pays":
                    item["country"] = value

                # Bottle size

                # Appellation
                if key == "Appellation":
                    item["appellation"] = value

                # Ranking

                # Color
                if key == "Couleur":
                    item["color"] = value

                # Apogee

                # Grape
                if key == "Encépagement":
                    item["grape"] = value

                # Soil

                # Viticulture
                if key == "Environnement":
                    item["viticulture"] = value

                # Alcool
                if key == "Alcool":
                    item["alcool"] = value

                # Garde
                if key == "Buvabilité":
                    item["garde"] = value

            # Url
            item["url"] = response.url
        except:
            pass

        yield item

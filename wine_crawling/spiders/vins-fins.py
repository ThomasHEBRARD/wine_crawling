import scrapy
import re
import numpy as np
from wine_crawling.items import WineItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError


class VinsFinsSpider(scrapy.Spider):
    name = "vinsfins"
    domain_name = "https://vins-fins.com"

    start_urls = ["https://vins-fins.com/vins.html"]

    custom_settings = {
        "DOWNLOAD_DELAY": "0.7",
        "ITEM_PIPELINES": {
            "wine_crawling.pipelines.WinePipeline": 300,
        },
    }

    BASE_HEADER = {
        "Cache-Control": "max-age=0",
        "origin": "https://www.vins-fins.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Referer": "https://vins-fins.com/vins.html",
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
        all_bottles_url = response.xpath(
            "//div[@class='product-info']/a/@href"
        ).extract()

        for url in all_bottles_url:
            yield scrapy.Request(
                url=url,
                headers=self.BASE_HEADER,
                callback=self.build_item,
                errback=self.errback_debug,
            )

        current_page = int(response.xpath("//li[@class='current']/text()").get())

        next_page_url = "https://vins-fins.com/vins.html?limit=24&p=%s" % str(
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
        item["website"] = "vinsfins"

        # URL
        item["url"] = response.url

        price_block = response.xpath(
            "//span[@itemprop='price'][@class='regular-price hidden']/text()"
        ).get()

        # Website Id
        # Price
        if price_block:
            # Website Id
            item["website_id"] = (
                response.xpath(
                    "//span[@itemprop='price'][@class='regular-price hidden']/@id"
                )
                .get()
                .split("-")[-1]
            )

            # Price
            brut_price = (
                response.xpath(
                    "//span[@itemprop='price'][@class='regular-price hidden']/text()"
                )
                .extract()[0]
                .replace(" ", "")
            )
            item["price"] = brut_price.strip("\t").strip("\r\n")

        # Color
        item["color"] = (
            response.xpath("//div[@class='product-name-1']/text()")
            .get()
            .split("l")[-1]
            .replace(" ", "")
        )

        # Image
        image = response.xpath("//img[@itemprop='image'][@id='image']/@src").get()
        if image:
            item["image"] = image

        # Grape
        if data := response.xpath("//div[@class='product-name-4']/text()"):
            grapes_not_clean = [
                r.split("% ") for r in data.get().split(" : ")[-1].split(", ")
            ]
            grapes = ["_".join(grape) for grape in grapes_not_clean]
            item["grape"] = ", ".join(grapes)

        # Bottle Size
        if bottle_size_str := response.xpath(
            "//div[@class='price-box']/span[@class='par_toto']/text()"
        ).extract():
            if search := re.search(" Bouteille \((.*)\)", bottle_size_str[0]):
                item["bottle_size"] = search.group(1)

        # Other info
        details = [
            tuple(r.split(" : "))
            for r in response.xpath("//ul[@class='colonne1']/li/text()").extract()
        ]

        for detail_key, detail_value in details:
            # Name
            if "Nom" in detail_key:
                item["name"] = detail_value

            # Vintage
            if "Millésime" in detail_key:
                item["vintage"] = detail_value

            # Country
            if "Pays" in detail_key:
                item["country"] = detail_value

            # Region
            if "Région" in detail_key:
                item["region"] = detail_value

            # Appellation
            if "Appellation" in detail_key:
                item["appellation"] = detail_value

            # Domaine
            if "Domaine" in detail_key:
                item["domaine"] = detail_value

            # Ranking
            if "Classement" in detail_key:
                item["ranking"] = detail_value

            # Garde
            if "garde" in detail_key:
                item["garde"] = detail_value

            # Viticulture
            if "viticulture" in detail_key:
                item["viticulture"] = detail_value

            # Alcool
            if "alcool" in detail_key:
                item["alcool"] = detail_value
        print(item["name"])
        yield item

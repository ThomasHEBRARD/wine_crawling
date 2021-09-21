import scrapy
import re
from wine_crawling.items import WineItem
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError


class IdealWineSpider(scrapy.Spider):
    name = "idealwine"
    domain_name = "https://www.idealwine.com"

    start_urls = [
        "https://www.idealwine.com/fr/acheter-du-vin/vente-VENTE-FLASH.jsp",
        "https://www.idealwine.com/fr/acheter-du-vin/vente-VENTE-ON-LINE.jsp",
        "https://www.idealwine.com/fr/acheter-du-vin/vins-en-vente.jsp",
    ]

    custom_settings = {
        # "DOWNLOAD_DELAY": "3",
        # "AUTOTHROTTLE_ENABLED": "True",
        # "AUTOTHROTTLE_START_DELAY": "3",
        # "AUTOTHROTTLE_MAX_DELAY": "3",
        # "AUTOTHROTTLE_TARGET_CONCURRENCY": "1.0",
        "ITEM_PIPELINES": {
            "wine_crawling.pipelines.WinePipeline": 300,
        }
    }

    BASE_HEADER = {
        "Cache-Control": "max-age=0",
        "origin": "https://www.idealwine.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
        "Sec-Fetch-User": "?1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Referer": "https://www.idealwine.com/fr/acheter-du-vin/vins-en-vente.jsp",
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
            '//ol[@class="pagingV2"]/li[11]/a/@href'
        ).extract_first()
        print(next_page)
        next_page = re.search(r"\((.*)\)", next_page).group(1)
        next_page_url = (
            "https://www.idealwine.com/fr/acheter-du-vin/page-%s.jsp" % next_page
        )

        if next_page is not None:
            yield scrapy.Request(
                url=next_page_url, callback=self.parse, errback=self.errback_debug
            )

        for link in response.xpath("//a[@class='truncatetype']/@href").extract():
            url = self.domain_name + link
            yield scrapy.Request(
                url=url,
                headers=self.BASE_HEADER,
                callback=self.build_item,
                errback=self.errback_debug,
            )

    def build_item(self, response):
        item = WineItem()
        item["website"] = "idealwine"
        item["url"] = response.url
        item["website_id"] = "-".join(response.url.split("/")[-1].split("-")[:2])

        # Name
        item["name"] = response.xpath(
            "//div[@class='description-1']//h1/text()"
        ).extract_first()

        # Image
        image_url = response.xpath("//img[@class='skip-lazy']/@src").get()
        if image_url:
            item["image"] = "idealwine.com" + image_url

        # Bottle size
        for li in response.xpath("//ul[@class='description-1-legends']/li/text()"):
            bottle_size = li.get()
            if "L" in bottle_size:
                item["bottle_size"] = bottle_size

        info = response.xpath(
            "//section[@id='descriptif']//article[@class='prez-3']/ul/li"
        )

        for j in info:
            text = j.xpath("./strong/text()").extract_first()

            # Region
            if "Région" in text:
                item["region"] = j.xpath("./text()").extract_first()

            # Country
            if "Country" in text:
                item["country"] = j.xpath("./text()").extract_first()

            # Color
            if "Couleur" in text:
                item["color"] = j.xpath("./text()").extract_first()[1:]

            # Appellation
            if "Appellation" in text:
                item["appellation"] = j.xpath("./text()").extract_first()[1:]

            # Apogee
            if "Apogée" in text:
                item["apogee"] = j.xpath("./text()").extract_first()[1:]

            # Domaine
            if "Propriétaire" in text:
                item["domaine"] = j.xpath("./text()").extract_first()[1:]

            # Vintage
            if "Millesime" in text:
                item["vintage"] = j.xpath("./text()").extract_first()[1:]

            # Ranking
            if "Classement" in text:
                item["ranking"] = j.xpath("./text()").extract_first()[1:]

            # Viticulture
            if "Viticulture" in text:
                item["viticulture"] = (
                    j.xpath("./text()").extract_first().replace("\xa0", "")
                )

            # Alcool
            if "Pourcentage" in text:
                item["alcool"] = j.xpath("./text()").extract_first().split(" %")[0][1:]

            # Grape
            if "Encepagement" in text:
                list_cepage = []
                if j.xpath(".//a").extract() is not None:
                    if "%" in j.xpath(".//strong/following-sibling::text()").get():
                        brut_percentages = j.xpath(
                            ".//strong/following-sibling::text()"
                        )
                        percentages = [
                            brut_percentages[i].get()
                            for i in range(len(brut_percentages))
                        ]
                        clean_percentages = []
                        for percentage in percentages:
                            if "%" in percentage:
                                p = re.search(r", (.*)%", percentage)
                                if p == None:
                                    p = re.search(r",(.*)%", percentage)
                                    if p == None:
                                        p = re.search(r" (.*)%", percentage)
                                        if p == None:
                                            p = re.search(r"; (.*)%", percentage)
                                            if p == None:
                                                p = re.search(r";(.*)%", percentage)
                            clean_percentages.append(p.group(1).split("%")[0])

                        for per, cepage in enumerate(j.xpath(".//a")):
                            list_cepage.append(
                                f"{clean_percentages[per]}_"
                                + cepage.xpath("./text()").extract_first()
                            )
                    else:
                        for k in j.xpath(".//a"):
                            list_cepage.append(k.xpath("./text()").extract_first())
                    item["grape"] = "/".join(list_cepage)
                else:
                    item["grape"] = j.xpath("./text()").extract_first().split(",")

            # Quantity
            if "Quantité" in text:
                quantity_text = j.xpath("./text()").extract_first()
                quantity = int(quantity_text.split("\xa0")[0])

            # Price
            price = response.xpath("//span[@id='input_prix']/text()").get()
            if price:
                item["price"] = int(price.replace(" ", "")) / quantity

        yield item

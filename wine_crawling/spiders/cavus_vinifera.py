import scrapy
import re
from wine_crawling.items import WineItem


class IdealWineSpider(scrapy.Spider):
    name = "ideal_wine"
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
        item["name"] = response.xpath(
            "//div[@class='description-1']//h1/text()"
        ).extract_first()
        capital = response.xpath("//div[@class='critere']/ul/li[@class='capital']")
        info = response.xpath(
            "//section[@id='descriptif']//article[@class='prez-3']/ul/li"
        )
        item["url"] = response.url
        item["website"] = "idealwine"
        quantity = 1

        for j in info:
            text = j.xpath("./strong/text()").extract_first()
            if "Pays / Region" in text:
                item["pays"] = j.xpath("./text()").extract_first()
            if "Couleur" in text:
                item["color"] = j.xpath("./text()").extract_first()
            if "Appellation" in text:
                item["appellation"] = j.xpath("./text()").extract_first()
            if "Apogée" in text:
                item["apogee"] = j.xpath("./text()").extract_first()
            if "Propriétaire" in text:
                item["domaine"] = j.xpath("./text()").extract_first()
            if "Millesime" in text:
                item["millesime"] = j.xpath("./text()").extract_first()
            if "Classement" in text:
                item["classement"] = j.xpath("./text()").extract_first()
            if "Viticulture" in text:
                item["viticulture"] = (
                    j.xpath("./text()").extract_first().replace("\xa0", "")
                )
            if "Pourcentage" in text:
                item["degre_alcool"] = (
                    j.xpath("./text()").extract_first().split(" %")[0]
                )
            if "Producteur" in text:
                item["producteur"] = j.xpath("./text()").extract_first()

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
                            clean_percentages.append(p.group(1).split("%")[0])

                        for per, cepage in enumerate(j.xpath(".//a")):
                            list_cepage.append(
                                f"{clean_percentages[per]}_"
                                + cepage.xpath("./text()").extract_first()
                            )
                    else:
                        for k in j.xpath(".//a"):
                            list_cepage.append(k.xpath("./text()").extract_first())
                    item["cepage"] = "/".join(list_cepage)
                else:
                    item["cepage"] = j.xpath("./text()").extract_first().split(",")

            # if "Quantité" in text:
            #     quantity_text = j.xpath("./text()").extract_first()
            #     quantity = int(quantity_text.split('B')[0])
            # if price := response.xpath("//span[@id='input_prix']/text()").extract_first() != None:
            #     item["price"] = int(price)/quantity
        # print(item.__dict__)
        yield item

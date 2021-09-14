import requests
import json
from lxml.html import fromstring
import jsonpath
from itertools import cycle
import queue
from multiprocessing import Pool
import random
import pandas as pd
import psycopg2

requests.packages.urllib3.disable_warnings()

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


def connector_db():
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="1234",
        port=5432,
    )
    return connection


def insert_postgres(item, connection):
    try:
        creation_query = f""" CREATE TABLE IF NOT EXISTS public.crawled_bottles
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
        cursor = connection.cursor()
        cursor.execute(creation_query)
        connection.commit()

        values_format = tuple([str(it).replace("'", " ") for it in item.values()])

        query = """
            INSERT INTO 
            {}({})
            VALUES{}
            """.format(
            "public.crawled_bottles",
            ",".join(item.keys()),
            values_format,
        )
        cursor.execute(query)
        connection.commit()
        print(item["name"])
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()


# return the first data
def return_one(alist):
    if alist:
        return alist[0]
    else:
        return "empty"


def detail(url, ip, proxies):
    try:
        headers = {
            # "Host": "www.vivino.com",
            # "Connection": "keep-alive",
            # "Accept": "application/json",
            # "X-Requested-With": "XMLHttpRequest",
            # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
            # "Content-Type": "application/json",
            # "Referer": "https://www.vivino.com/explore?e=eJzLLbI1VMvNzLM1VctNrLA1MjUwUEuutE3OU0u2dfaLVCsASqen2ZYlFmWmliTmqOUm26rlJwGxbUpqcTIAW4YTvg==",
            # "Accept-Encoding": "gzip, deflate, br",
            # "Accept-Language": "zh,zh-CN;q=0.9,en;q=0.8",
            # "Cookie": "_ga=GA1.2.921554165.1544198412; __auc=2600ba4b1678965d8e742ab1576; client_cache_key=VnRBRW5MYkFiV1JNeDluc2dVYzFQeEZtVk9BUmpWZ2NGajF0bFJBc09IWT0tLXlzSldOU1pxYkx3cW5KeWUrVFcwQUE9PQ%3D%3D--4b9ce67fb8cf167fe21e610512e4bc8cefa567e3; eeny_meeny_test_mobile_browsepills_v1=zsGvZXceHesuE2p9d7Y5NiOuTt66fl6rkhYWBU1HBzMWK2mQbob1uYFD0PNH7tGAgkrrV2%2Ftc2%2BEL0q0v6nP4A%3D%3D; eeny_meeny_test_crosssell_discountwine_v1=HHXMrv2%2B%2FO0AP%2Bv6p2tVG64DgPjDR1Gpp51ymRsZQD9O79ScwfLUBzgMeF2CdsdivTi4A7ATDlcS4Gapn6kFZA%3D%3D; eeny_meeny_test_takeover_offer_email_v1=oYzqbiTV20PlGVqes6muu3FSA1eKfJmDrBIhdEsCPG2rjg9R%2Ba%2BgycG%2B0lHK69CGb8EhAtqmYhv5%2FBi88NIbow%3D%3D; _gid=GA1.2.1494373843.1544536041; __asc=31e5bd511679d85a9387bc947c6; __stripe_mid=483c7ffc-739d-4270-b80e-3f98aafc094f; __stripe_sid=3687990f-857c-4cb6-8132-d1d23908cb04; _hp2_ses_props.3503103446=%7B%22r%22%3A%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DuRiyIpRm0mAPa8oS6RWRMVQiecGGQiHUkvso_aerJn8sgBB1VZ9g6GxnXeK0eFxC%26wd%3D%26eqid%3D83ede1b000006cd2000000025c0fbfe5%22%2C%22ts%22%3A1544539673035%2C%22d%22%3A%22www.vivino.com%22%2C%22h%22%3A%22%2F%22%7D; recently_viewed=ckc4N0RZeWcwdnE4bUp0eTdwNkxRWjJiNG1DdXhBemRpTkQ3cmJJSUdmMG05dFc5VHd0UENGMXRLWHM4NFAxVmZJOURLQlBEdWc1OGRBMU9waDgvdWJPdzIrWEpCVTI3WkllSUlaZEZLc1RkN1VpUFY2MGhURmRWMElYZjVLRUV5VVVVYVA5QmZjUHB5czFKNFBBZm9wempyMzdUaENHU2NPVG5SV2t5dEpLKzdnY0VtMU5UTW9XM0UwYUxabFZyOTRQeVN0QlNpbngzbUZqbEVpaXhCQ1ppYnZkbDJYTlMwT2k3UW5STGVIZz0tLWo0ZFR1N3VLcEM0bXdZVC9nQTVBNkE9PQ%3D%3D--33432b307835936fe6cd81cdbf18141077fd30cc; _gat_vivinoTracker=1; _hp2_id.3503103446=%7B%22userId%22%3A%228198599763481872%22%2C%22pageviewId%22%3A%226810762282263360%22%2C%22sessionId%22%3A%223474685745605734%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _ruby-web_session=UmNZK2VvclVmT0g2OXd4SzFacUZnSi93T21sbUdZd0lTLzVGTjVXd0Y5akdNRGVFYW16K2wrMHFPeGNWbXBvMStrUE9wSHlaSk5DVDFGRzNpMllaRVpaMkMzREVxT21IeUlGb1Y5SFdVMk1oTXNpcnpVbTlEK2R5WHN4OFRvekhhaTBDeDZXM3hoWElrYTM1RS9lMXF6Z3BFYU5pT2dTd05kZ2ZzTk1JR05GUkwyOFd6c0hiMEpqRm84dno2NFhmWjdMUStCUDAycThqaVczVTIxTTdmRXp5bTFhRkNGTzh0aXYxMGRQTEQvZ3pPYUU5ZTdCNU9GNXhuUm1BeGRpbzNyaUlqTXZaTVp0cDBWUjA3QllZdkxWR0tybzZiM2FNYkJEbXcyMUZBWUw1bld0bUlnQklpZ0p0VE9KNHV3Wld6T3N3S2kyOXVRbXFYMzdIMUdPeFo5TFpxaFh1M2dOZjl5ZHFZMkx1N2pMT1AwVGd0VTVLY3lrbld6b0hRVGFwZ0JIbTB5Ni9FWHUwWjk1eTJ0UjRGY1NLWWpIRHhUdFA5bGZHT3Z1NXN2TFNSQUVsbDJuZlMwQ1dyMUZENER1Qy0tdUpYNThHNGZlODh5Tis2TmpPZ1FhQT09--48571978c9c7904a156c17923c0f91f3b09f0f98",
            # "If-None-Match": 'W/"ab73c8aecbc35e1ac6d17806bf5a1c49"',
        }
        ip = "191.243.218.245:53281"
        proxy = {"http": ip, "https": ip}
        print("before requests", proxy)
        html = requests.get(url, headers=headers, proxies=proxy)
        js = json.loads(html.text)
        zong = jsonpath.jsonpath(js, "explore_vintage.matches.*")
        connection = connector_db()

        for index, i in enumerate(zong):
            item = WineItem()
            item["url"] = url
            item["website"] = "vivino"
            #
            item["name"] = return_one(jsonpath.jsonpath(i, "$..vintage.name"))
            item["pays"] = return_one(
                jsonpath.jsonpath(i, "$..vintage.wine.region.country.name")
            )

            #  url
            # img = return_one(
            #     jsonpath.jsonpath(i, "$..vintage.image.variations.bottle_medium")
            # )
            # if img == "empty":
            #     img = return_one(jsonpath.jsonpath(i, "$..vintage.image.location"))
            # img_url = "http://" + img.lstrip("//")
            # img_content = requests.get(img_url, proxies=ip).content
            # #
            # img_content = pymysql.Binary(img_content)

            #
            item["domaine"] = return_one(
                jsonpath.jsonpath(i, "$..vintage.wine.winery.name")
            )
            #
            region = return_one(jsonpath.jsonpath(i, "$..vintage.wine.region.name"))
            #
            item["viticulture"] = return_one(
                jsonpath.jsonpath(i, "$..vintage.wine.style.name")
            )
            #

            #   url
            url_id = return_one(jsonpath.jsonpath(i, "$..vintage.id"))
            # request url
            info_url = "https://www.vivino.com/api/vintages/{}/vintage_page_information?price_id=null".format(
                url_id
            )
            print(info_url)
            info_html = requests.get(info_url, headers=headers, proxies=ip)
            print("info_html", info_html)
            #
            # Wine category
            js2 = json.loads(info_html.text)
            grapes = jsonpath.jsonpath(js2, "$..vintage.wine.grapes.*.name")
            if grapes:
                item["cepage"] = "|".join(grapes)

                #

            # Alcohol content
            item["degre_alcool"] = return_one(
                jsonpath.jsonpath(js2, "$..vintage.wine.alcohol")
            )

            # save to data
            insert_postgres(item, connection)
            print("Use ip: ", ip)
    except Exception as e:
        print("Error: ", e)
        try:
            detail(url, proxies.get(), proxies)
        except:
            print("fail")
            pass


if __name__ == "__main__":
    proxies_csv = pd.read_csv("proxies.csv")

    proxies = queue.Queue()
    q = queue.Queue()

    for i in range(1, 30001):
        aaa = "https://www.vivino.com/api/explore/explore?min_rating=3.5&order_by=discount_percent&order=desc&page={}&price_range_max=30&price_range_min=7&wine_type_ids[]=1".format(
            "ok"
        )
        url = "https://www.vivino.com/api/explore/explore?min_rating=3.5&order_by=discount_percent&order=desc&page={}&price_range_max=30&price_range_min=7&wine_type_ids[]=1".format(
            i
        )
        q.put(url)

    for x in proxies_csv.itertuples(index=False):
        proxies.put(str(x[0]) + ":" + str(x[1]))

    while not q.empty():
        detail(q.get(), proxies.get(), proxies)

import requests
import json
import jsonpath
import pymysql
import queue
from multiprocessing import Pool
import random


requests.packages.urllib3.disable_warnings()

# Create a connection
db = pymysql.connect("127.0.0.1", "root", "cyl666.", "scrapy", charset="utf8")
# Create a cursor object
cursor = db.cursor()


# database insertion
def insert_mysql(sets):
    cursor.execute(
        "insert into red_wine(title,grade,score,country,img_url,img_content,winery,region,regional_styles,food,grapes,acidity,alcohol) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        sets,
    )
    db.commit()


#
def ip_proxy():
    # Get the proxy api 20 each time
    url = "http://webapi.http.zhimacangku.com/getip?num=20&type=2&pro=&city=0&yys=0&port=1&pack=34365&ts=1&ys=1&cs=1&lb=1&sb=0&pb=4&mr=1&regions="
    html = requests.get(url)
    ip_js = json.loads(html.text)
    all_ip = jsonpath.jsonpath(ip_js, "data.*")
    ip_lists = []
    for i in all_ip:
        proxy = {
            "http": "http://%s:%s" % (i["ip"], i["port"]),
            "https": "https://%s:%s" % (i["ip"], i["port"]),
        }
        ip_lists.append(proxy)
    return ip_lists


# return the first data
def return_one(alist):
    if alist:
        return alist[0]
    else:
        return "empty"


def detail(url, ip):
    try:
        headers = {
            "Host": "www.vivino.com",
            "Connection": "keep-alive",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36",
            "Content-Type": "application/json",
            "Referer": "https://www.vivino.com/explore?e=eJzLLbI1VMvNzLM1VctNrLA1MjUwUEuutE3OU0u2dfaLVCsASqen2ZYlFmWmliTmqOUm26rlJwGxbUpqcTIAW4YTvg==",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh,zh-CN;q=0.9,en;q=0.8",
            # 'Cookie': '_ga=GA1.2.921554165.1544198412; __auc=2600ba4b1678965d8e742ab1576; client_cache_key=VnRBRW5MYkFiV1JNeDluc2dVYzFQeEZtVk9BUmpWZ2NGajF0bFJBc09IWT0tLXlzSldOU1pxYkx3cW5KeWUrVFcwQUE9PQ%3D%3D--4b9ce67fb8cf167fe21e610512e4bc8cefa567e3; eeny_meeny_test_mobile_browsepills_v1=zsGvZXceHesuE2p9d7Y5NiOuTt66fl6rkhYWBU1HBzMWK2mQbob1uYFD0PNH7tGAgkrrV2%2Ftc2%2BEL0q0v6nP4A%3D%3D; eeny_meeny_test_crosssell_discountwine_v1=HHXMrv2%2B%2FO0AP%2Bv6p2tVG64DgPjDR1Gpp51ymRsZQD9O79ScwfLUBzgMeF2CdsdivTi4A7ATDlcS4Gapn6kFZA%3D%3D; eeny_meeny_test_takeover_offer_email_v1=oYzqbiTV20PlGVqes6muu3FSA1eKfJmDrBIhdEsCPG2rjg9R%2Ba%2BgycG%2B0lHK69CGb8EhAtqmYhv5%2FBi88NIbow%3D%3D; _gid=GA1.2.1494373843.1544536041; __asc=31e5bd511679d85a9387bc947c6; __stripe_mid=483c7ffc-739d-4270-b80e-3f98aafc094f; __stripe_sid=3687990f-857c-4cb6-8132-d1d23908cb04; _hp2_ses_props.3503103446=%7B%22r%22%3A%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DuRiyIpRm0mAPa8oS6RWRMVQiecGGQiHUkvso_aerJn8sgBB1VZ9g6GxnXeK0eFxC%26wd%3D%26eqid%3D83ede1b000006cd2000000025c0fbfe5%22%2C%22ts%22%3A1544539673035%2C%22d%22%3A%22www.vivino.com%22%2C%22h%22%3A%22%2F%22%7D; recently_viewed=ckc4N0RZeWcwdnE4bUp0eTdwNkxRWjJiNG1DdXhBemRpTkQ3cmJJSUdmMG05dFc5VHd0UENGMXRLWHM4NFAxVmZJOURLQlBEdWc1OGRBMU9waDgvdWJPdzIrWEpCVTI3WkllSUlaZEZLc1RkN1VpUFY2MGhURmRWMElYZjVLRUV5VVVVYVA5QmZjUHB5czFKNFBBZm9wempyMzdUaENHU2NPVG5SV2t5dEpLKzdnY0VtMU5UTW9XM0UwYUxabFZyOTRQeVN0QlNpbngzbUZqbEVpaXhCQ1ppYnZkbDJYTlMwT2k3UW5STGVIZz0tLWo0ZFR1N3VLcEM0bXdZVC9nQTVBNkE9PQ%3D%3D--33432b307835936fe6cd81cdbf18141077fd30cc; _gat_vivinoTracker=1; _hp2_id.3503103446=%7B%22userId%22%3A%228198599763481872%22%2C%22pageviewId%22%3A%226810762282263360%22%2C%22sessionId%22%3A%223474685745605734%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _ruby-web_session=UmNZK2VvclVmT0g2OXd4SzFacUZnSi93T21sbUdZd0lTLzVGTjVXd0Y5akdNRGVFYW16K2wrMHFPeGNWbXBvMStrUE9wSHlaSk5DVDFGRzNpMllaRVpaMkMzREVxT21IeUlGb1Y5SFdVMk1oTXNpcnpVbTlEK2R5WHN4OFRvekhhaTBDeDZXM3hoWElrYTM1RS9lMXF6Z3BFYU5pT2dTd05kZ2ZzTk1JR05GUkwyOFd6c0hiMEpqRm84dno2NFhmWjdMUStCUDAycThqaVczVTIxTTdmRXp5bTFhRkNGTzh0aXYxMGRQTEQvZ3pPYUU5ZTdCNU9GNXhuUm1BeGRpbzNyaUlqTXZaTVp0cDBWUjA3QllZdkxWR0tybzZiM2FNYkJEbXcyMUZBWUw1bld0bUlnQklpZ0p0VE9KNHV3Wld6T3N3S2kyOXVRbXFYMzdIMUdPeFo5TFpxaFh1M2dOZjl5ZHFZMkx1N2pMT1AwVGd0VTVLY3lrbld6b0hRVGFwZ0JIbTB5Ni9FWHUwWjk1eTJ0UjRGY1NLWWpIRHhUdFA5bGZHT3Z1NXN2TFNSQUVsbDJuZlMwQ1dyMUZENER1Qy0tdUpYNThHNGZlODh5Tis2TmpPZ1FhQT09--48571978c9c7904a156c17923c0f91f3b09f0f98',
            # 'If-None-Match': 'W/"ab73c8aecbc35e1ac6d17806bf5a1c49"',
        }

        html = requests.get(url, headers=headers, verify=False, proxies=ip)
        js = json.loads(html.text)
        zong = jsonpath.jsonpath(js, "explore_vintage.matches.*")

        for index, i in enumerate(zong):
            #
            title = return_one(jsonpath.jsonpath(i, "$..vintage.name"))
            # Rating
            grade = return_one(
                jsonpath.jsonpath(i, "$..vintage.statistics.ratings_average")
            )
            #
            score = return_one(
                jsonpath.jsonpath(i, "$..vintage.statistics.ratings_count")
            )
            #
            country = return_one(
                jsonpath.jsonpath(i, "$..vintage.wine.region.country.name")
            )

            #  url
            img = return_one(
                jsonpath.jsonpath(i, "$..vintage.image.variations.bottle_medium")
            )
            if img == "empty":
                img = return_one(jsonpath.jsonpath(i, "$..vintage.image.location"))
            img_url = "http://" + img.lstrip("//")
            img_content = requests.get(img_url, proxies=ip).content
            #
            img_content = pymysql.Binary(img_content)

            #
            winery = return_one(jsonpath.jsonpath(i, "$..vintage.wine.winery.name"))
            #
            region = return_one(jsonpath.jsonpath(i, "$..vintage.wine.region.name"))
            #
            regional_styles = return_one(
                jsonpath.jsonpath(i, "$..vintage.wine.style.name")
            )
            #
            food = jsonpath.jsonpath(i, "$..vintage.wine.style.food.*.name")
            if food:
                food = "|".join(food)

                #   url
            url_id = return_one(jsonpath.jsonpath(i, "$..vintage.id"))
            # request url
            info_url = "https://www.vivino.com/api/vintages/{}/vintage_page_information?price_id=null".format(
                url_id
            )
            info_html = requests.get(
                info_url, headers=headers, verify=False, proxies=ip
            )
            #
            # Wine category
            js2 = json.loads(info_html.text)
            grapes = jsonpath.jsonpath(js2, "$..vintage.wine.grapes.*.name")
            if grapes:
                grapes = "|".join(grapes)

                #
            acidity = return_one(
                jsonpath.jsonpath(js2, "$..vintage.wine.style.acidity")
            )
            # Alcohol content
            alcohol = return_one(jsonpath.jsonpath(js2, "$..vintage.wine.alcohol"))

            sets = (
                title,
                grade,
                score,
                country,
                img_url,
                img_content,
                winery,
                region,
                regional_styles,
                food,
                grapes,
                acidity,
                alcohol,
            )
            # save to data
            insert_mysql(sets)
            print("Save success: ", title, img_url)
            print("Use ip: ", ip)
    except Exception as e:
        print("Error: ", e)
        random_ip = ip_proxy()
        detail(url, random.choice(random_ip))


if __name__ == "__main__":
    # Queue
    q = queue.Queue()
    po = Pool(40)
    for i in range(1, 30001):
        url = "https://www.vivino.com/api/explore/explore?country_code=cn&currency_code=CNY&grape_filter=varietal&merchant_id=&min_rating=&order_by=&order=desc&page={}&price_range_max=&price_range_min=5".format(
            i
        )
        q.put(url)

        # ip
    random_ip = ip_proxy()
    while not q.empty():
        po.apply_async(detail, (q.get(), random.choice(random_ip)))

    po.close()
    po.join()

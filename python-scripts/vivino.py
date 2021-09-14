import requests
import pandas as pd

data = {
    "winery": [],
    "wine_name": [],
    "wine_year": [],
    "region": [],
    "country": [],
    "grapes": [],
    "rating": [],
    "price": [],
    "discounted_from": [],
    "image": [],
    "image_label": [],
    "is_natural": [],
    "top_list_rankings": [],
    "url_to_buy": [],
}
already_treated_wine = []

for i in range(200):
    print(i, len(data["wine_name"]), 'coucou')
    url = "https://www.vivino.com/api/explore/explore?min_rating=0.1&page={}".format(i)
    r = requests.get(
        url=url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
        },
    )
    matches = r.json()["explore_vintage"]["matches"]
    for match in matches:
        vintage, price = match["vintage"], match["price"]

        if vintage["id"] not in already_treated_wine:
            already_treated_wine.append(vintage["id"])

            data["winery"].append(vintage["wine"]["winery"]["name"])
            data["region"].append(vintage["wine"]["region"]["name"])
            data["country"].append(vintage["wine"]["region"]["country"]["name"])
            data["wine_name"].append(vintage["wine"]["name"])
            data["is_natural"].append(vintage["wine"]["is_natural"])
            if vintage["wine"]["style"]:
                data["grapes"].append(
                    [grape["name"] for grape in vintage["wine"]["style"]["grapes"]]
                )
            else:
                data["grapes"].append(None)
            if "bottle_medium" in vintage["image"]["variations"].keys():
                data["image"].append(vintage["image"]["variations"]["bottle_medium"])
            else:
                data["image"].append(None)
            data["image_label"].append(vintage["image"]["location"])
            data["wine_year"].append(vintage["year"])
            data["rating"].append(vintage["statistics"]["ratings_average"])
            if "top_list_rankings" in vintage.keys():
                data["top_list_rankings"].append(
                    [rank["top_list"]["name"] for rank in vintage["top_list_rankings"]]
                )
            else:
                data["top_list_rankings"].append(None)
            data["price"].append(price["amount"])
            data["discounted_from"].append(price["discounted_from"])
            data["url_to_buy"].append(price["url"])

df = pd.DataFrame(data=data)
df.to_csv("vivino3.csv", index=False)

import requests
import pandas as pd


def get_wine_data(wine_id, year, page):
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    }

    api_url = "https://www.vivino.com/api/wines/{id}/reviews?per_page=9999&year={year}&page={page}"  # <-- increased the number of reviews to 9999

    d = requests.get(
        api_url.format(id=wine_id, year=year, page=page), headers=headers
    ).json()

    return d


data = {
    "website_id": [],
    "winery": [],
    "name": [],
    "vintage": [],
    "region": [],
    "country": [],
    "grapes": [],
    "rating": [],
    "price": [],
    "discounted_from": [],
    "image": [],
    "image_label": [],
    # "is_natural": [],
    "top_list_rankings": [],
    "url": [],
}
already_treated_wine = []

for page in range(1, 10):
    r = requests.get(
        "https://www.vivino.com/api/explore/explore",
        params={
            "country_code": "fr",
            "country_codes[]": "fr",
            "currency_code": "EUR",
            # "grape_filter": "varietal",
            # "min_rating": "1",
            # "order_by": "price",
            # "order": "asc",
            "page": page,
            # "price_range_max": "500",
            "price_range_min": "0",
            "wine_type_ids[]": "1",
        },
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0"
        },
    )
    print(page)
    def append_or_none(data, the_data, key):
        if the_data:
            data[key].append(the_data)
        else:
            data[key].append(None)

    matches = r.json()["explore_vintage"]["matches"]
    for match in matches:
        vintage, price = match["vintage"], match["price"]

        if vintage["id"] not in already_treated_wine:
            already_treated_wine.append(vintage["id"])
            append_or_none(data, vintage["id"], "website_id")
            append_or_none(data, vintage["wine"]["winery"]["name"], "winery")
            append_or_none(data, vintage["wine"]["region"]["name"], "region")
            append_or_none(
                data, vintage["wine"]["region"]["country"]["name"], "country"
            )
            append_or_none(data, vintage["wine"]["name"], "name")
            append_or_none(data, vintage["image"]["location"], "image_label")
            append_or_none(data, vintage["year"], "vintage")
            append_or_none(data, vintage["statistics"]["ratings_average"], "rating")
            append_or_none(data, price["amount"], "price")
            append_or_none(data, price["url"], "url")
            append_or_none(data, price["discounted_from"], "discounted_from")
            # data["is_natural"].append(vintage["wine"]["is_natural"])
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

            if "top_list_rankings" in vintage.keys():
                data["top_list_rankings"].append(
                    [rank["top_list"]["name"] for rank in vintage["top_list_rankings"]]
                )
            else:
                data["top_list_rankings"].append(None)

dataframe = pd.DataFrame(
    data,
    columns=list(data.keys()),
)

dataframe.to_csv("data3.csv", index=False)

# ratings = []
# for _, row in dataframe.iterrows():
#     page = 1
#     while True:
#         print(
#             f'Getting info about wine {row["Wine ID"]}-{row["Year"]} Page {page}'
#         )

#         d = get_wine_data(row["Wine ID"], row["Year"], page)

#         if not d["reviews"]:
#             break

#         for r in d["reviews"]:
#             ratings.append(
#                 [
#                     row["Year"],
#                     row["Wine ID"],
#                     r["rating"],
#                     r["note"],
#                     r["created_at"],
#                 ]
#             )

#         page += 1

# ratings = pd.DataFrame(
#     ratings, columns=["Year", "Wine ID", "User Rating", "Note", "CreatedAt"]
# )

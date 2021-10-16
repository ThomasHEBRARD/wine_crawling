from app_store_scraper import AppStore
import pandas as pd
import numpy as np
import json

apps = {
    "easycave": 1094186394,
    "ma-cave-a-vins": 1547090471,
    "vivino": 414461255,
    "vinocell-cave-à-vin": 521577043,
    "mes-cave-à-vin": 1563352966,
    "twil": 1028379086,
    "avinguard": 1501867336,
    "smartcave": 442935847,
    "cellwine-gérer-la-cave-à-vin": 1229068303,
    "ploc-lapp-des-épicuvins": 1156928592,
    "vinotag": 1544449923,
}

writer = pd.ExcelWriter("reviews.xlsx")

for app_name, app_id in apps.items():
    data = {
        "title": [],
        "review": [],
        "developer_response": [],
        "rating": [],
        "date": [],
    }
    appstore_app = AppStore(country="fr", app_name=app_name, app_id=app_id)
    appstore_app.review()
    reviews = appstore_app.reviews

    for review in reviews:
        data["title"].append(review["title"])
        data["review"].append(review["review"])
        if "developerResponse" in review:
            data["developer_response"].append(review["developerResponse"]["body"])
        else:
            data["developer_response"].append(None)
        data["rating"].append(review["rating"])
        data["date"].append(review["date"].strftime("%d/%m/%Y"))

    df = pd.DataFrame(data=data)
    print(df)
    df.to_excel(writer, sheet_name=str(app_name), index=False)
    writer.save()

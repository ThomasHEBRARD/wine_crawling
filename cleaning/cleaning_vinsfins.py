import statistics
import re
import psycopg2
import pandas as pd
from cleaned_items import CleanedWineItem


def postgresql_to_dataframe(select_query, column_names):
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="1234",
        port=5432,
    )
    cursor = connection.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    tupples = cursor.fetchall()
    cursor.close()

    df = pd.DataFrame(tupples, columns=column_names)
    return df


select_query = """
    SELECT * FROM public.crawled_bottless
    WHERE website = 'vinsfinss'
    """

column_names = [
    "id",
    "website_id",
    "name",
    "vintage",
    "winery",
    "country",
    "region",
    "appellation",
    "soil",
    "color",
    "bottle_size",
    "grape",
    "viticulture",
    "apogee",
    "garde",
    "alcool",
    "price",
    "ranking",
    "image",
    "url",
    "website",
]

df = postgresql_to_dataframe(select_query, column_names)

hostname = "localhost"
username = "postgres"
password = "1234"
database = "postgres"


def process_item(item):
    try:
        connection = psycopg2.connect(
            host=hostname, user=username, password=password, dbname=database
        )
        cursor = connection.cursor()

        keys = ",".join(item.keys())
        values = tuple([str(it).replace("'", " ") for it in item.values()])

        query = """
                INSERT INTO 
                {}({})
                VALUES{}
                """.format(
            item.get_table_name(),
            keys,
            values,
        )
        cursor.execute(query)
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()

    return item


########################################################################
########################################################################


def treat_grape_idealwine(col):
    # 50 % de Cabernet Sauvignon, à 40% de Merlot, à 5 % de Cabernet Franc et à 5 % de Petit Verdot
    # 56% Cabernet Sauvignon - 36% Merlot - 6% Petit Verdot - 2% Cabernet Franc
    # 47% Cabernet Sauvignon, 47% Merlot, 5% Petit Verdot, 1% Cabernet Franc
    # Merlot, Cabernet Sauvignon, cabernet franc
    # 67% Cabernet-Sauvignon, 25% Merlot, 6% Cabernet-Franc et 2% Petit Verdot
    # 40% Merlot, 30% Cabernet Sauvignon,  30% Malbec.
    if col.grape:
        brut_grapes = col.grape.strip()
        brut_grapes = brut_grapes.replace("\xa0", "")
        brut_grapes = brut_grapes.replace("\t", "")
        grapes = re.split(r"(?<!\d),(?!\d)", brut_grapes)
        final_grape_list = []

        for grape in grapes:
            done = False
            grape = grape.replace(",", ".")
            if grape.count("%") > 1:
                # indexes = [pos for pos, char in enumerate(grape) if char == "%"]
                # if indexes[0] < 5:
                #     grape
                pass
            elif "%" in grape:
                if p := re.search(r"\((.*)%\)", grape):
                    if not done:
                        percentage = p.group(1)
                        grape_name = grape.split("(" + percentage + "%)")[0].strip()
                        if percentage.isdigit():
                            done = True
                if p := re.search(r"(.*)%", grape):
                    if not done:
                        percentage = p.group(1)
                        grape_name = grape.split(percentage + "%")[1].strip()
                        if percentage.isdigit():
                            done = True
                if p := re.search("\d à \d", grape):
                    if not done:
                        percentage = statistics.mean(
                            [int(s) for s in grape.split("%")[0].split("à")]
                        )
                        grape_name = grape.split("%")[1]
                        if percentage.isdigit():
                            done = True
                if p := re.search("\d-\d", grape):
                    if not done:
                        percentage = statistics.mean(
                            [int(s) for s in grape.split("%")[0].split("-")]
                        )
                        grape_name = grape.split("%")[1]
                        if percentage.isdigit():
                            done = True

                if grape.split("%")[-1] == "":
                    if not done:
                        percentage_match = re.search(
                            r"[-+]?\d*\.\d+|\d+", grape
                        ) or re.search(r"[-+]?\d*\.\d+|\d+ ", grape)
                        percentage = percentage_match.group(0)
                        grape_name = grape.split(percentage + "%")[0]
                        if percentage.isdigit():
                            done = True

                grape_name = grape_name.title().replace("-", " ")
                final_grape_list.append(percentage.strip() + "_" + grape_name.strip())
            else:
                final_grape_list.append(grape.strip())

        final_grape = "/".join(final_grape_list) if len(final_grape_list) else None
        return final_grape

        # print(final_grape, col.url)
        # Autre cas: 50% cabernet sauvignon, 40% merlot, 5% petit verdot 5% cabernet franc
        # 70_Cab.Sauvignon/23_Merlot/7_Cabernet Franc
        # https://www.idealwine.com/fr/acheter-vin/B2110040-34108-1-Bouteille-Chateau-Monbrison-CBO-a-partir-de-12bts-2017-Rouge.jsp
        # https://www.idealwine.com/fr/acheter-vin/B2110040-61887-1-Bouteille-Chateau-La-Prade-2014-Rouge.jsp
        # https://www.idealwine.com/fr/acheter-vin/B2110040-55125-1-Bouteille-Chateau-la-Conseillante-CBO-a-partir-de-6-bts-2018-Rouge.jsp
        # https://www.idealwine.com/fr/acheter-vin/B2110040-56250-1-Bouteille-Chateau-la-Clotte-Grand-Cru-Classe-CBO-a-partir-de-6-bts-2014-Rouge.jsp
        # 70% Merlot 20% cabernet sauvgnon 10_Petit Verdot https://www.idealwine.com/fr/acheter-vin/B2110040-68435-1-Bouteille-Chateau-Rollan-de-By-Cru-Bourgeois-2015-Rouge.jsp


def treat_name_idealwine(col):
    name = col["name"]
    name = name.split(col.ranking)[0]
    name = name.split(col.vintage)[0]
    return name


def treat_vintage_idealwine(col):
    vintage = col.vintage
    if not vintage.isdigit():
        vintage = None
    return vintage


def treat_region_country_idealwine(col):
    # Need to complete missing countries: 
    # Example: we have the right region (Bourgogne)
    # but the country is missing
    pass


def treat_bottle_size_idealwine(col):
    bottle_size = col.bottle_size.replace("cl", "")
    bottle_size = int(bottle_size / 100)
    return bottle_size

def treat_alcool_idealwine(col):
    alcool = col.alcool.replace("°", "")
    alcool = col.alcool.replace(",", ".")
    return alcool

def treat_viticulture_idealwine(col):
    if col.viticulture == '-':
        return None
    viticulture = col.viticulture.title().strip()
    return viticulture

for row, col in df.iterrows():
    grape = treat_grape_idealwine(col)
    name = treat_name_idealwine(col)
    vintage = treat_vintage_idealwine(col)
    # country, region = treat_region_country_idealwine(col)
    bottle_size = treat_bottle_size_idealwine(col)
    alcool = treat_alcool_idealwine(col)
    viticulture = treat_viticulture_idealwine(col)

    item = CleanedWineItem(col.to_dict())
    item.pop("id")
    item["grape"] = grape
    item["name"] = name
    item["vintage"] = vintage
    # item["country"] = country
    # item["region"] = region
    item["bottle_size"] = bottle_size
    item["alcool"] = alcool
    item["viticulture"] = viticulture

    # process_item(item)

# name : Château Giscours 2015 en double-magnum 
# 221 228 285
# 233 Château Giscours 2015 en double-magnum???
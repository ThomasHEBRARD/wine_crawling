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
    SELECT * FROM public.crawled_bottles
    WHERE website = 'idealwine'
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

        keys_list, values_list = [], []
        for k, v in item.items():
            if v and v != "NULL":
                values_list.append(v)
                keys_list.append(k)

        keys = ",".join(keys_list)
        values = tuple([str(it).replace("'", " ") for it in values_list])

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
    if col.grape:
        brut_grapes = col.grape.strip()
        brut_grapes = brut_grapes.replace("\xa0", "")
        brut_grapes = brut_grapes.replace("\t", "")
        grapes = re.split(r"(?<!\d),(?!\d)", brut_grapes)
        final_grape_list = []

        while not all([grape.count("%") <= 1 for grape in grapes]):
            for i in range(len(grapes)):
                if grapes[i].count("%") > 1:
                    grape = grapes.pop(i)
                    if len((s := re.split(r"(?<!\d),", grape))) > 1:
                        grapes[i:i] = s
                    elif len((s := re.split(r"(?<!net)-(?!\d)", grape))) > 1:
                        grapes[i:i] = s
                    else:
                        grapes[i:i] = re.split(r" et (?<!\d\%)", grape)

        for grape in grapes:
            done = False
            grape = grape.replace(",", ".")

            if "%" in grape:
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
                final_grape_list.append(grape.replace("_", " ").title().strip())

        final_grape = "/".join(final_grape_list) if len(final_grape_list) else None
        return final_grape

        # print(final_grape, col.url)
        #Michel Couvreur Candid (70cl) 
        # "La_folle_blanche" -> https://www.idealwine.com/fr/acheter-vin/B2110137-45394-1-Bouteille-Vin-de-France-Marguerite-LEcu-Domaine-de-2019-Blanc.jsp
        # Autre cas: 50% cabernet sauvignon, 40% merlot, 5% petit verdot 5% cabernet franc
        # 70_Cab.Sauvignon/23_Merlot/7_Cabernet Franc
        # https://www.idealwine.com/fr/acheter-vin/B2110040-34108-1-Bouteille-Chateau-Monbrison-CBO-a-partir-de-12bts-2017-Rouge.jsp
        # https://www.idealwine.com/fr/acheter-vin/B2110040-61887-1-Bouteille-Chateau-La-Prade-2014-Rouge.jsp
        # https://www.idealwine.com/fr/acheter-vin/B2110040-55125-1-Bouteille-Chateau-la-Conseillante-CBO-a-partir-de-6-bts-2018-Rouge.jsp
        # https://www.idealwine.com/fr/acheter-vin/B2110040-56250-1-Bouteille-Chateau-la-Clotte-Grand-Cru-Classe-CBO-a-partir-de-6-bts-2014-Rouge.jsp
        # 70% Merlot 20% cabernet sauvgnon 10_Petit Verdot https://www.idealwine.com/fr/acheter-vin/B2110040-68435-1-Bouteille-Chateau-Rollan-de-By-Cru-Bourgeois-2015-Rouge.jsp


def treat_name_idealwine(col):
    name = col["name"]
    name = name.replace(col.vintage, "")
    name = name.replace(str(col.ranking), "")
    if "cbo" in name.lower():
        name = re.split(r"\(cbo", name)[0]
    if "Cbo" in name:
        name = re.split(r"\(Cbo", name)[0]
    if "CBO" in name:
        name = re.split(r"\(CBO", name)[0]
    name = name.split(col.ranking)[0]
    name = name.split(col.vintage)[0]
    return name


def treat_vintage_idealwine(col):
    vintage = col.vintage
    if not vintage.isdigit():
        vintage = None
    return vintage


def treat_region_country_idealwine(col):
    region = col.region
    country = None
    # To clean
    # Divers - Divers ??
    # Japon - Honshu - Miyagi
    # Etats-Unis - Etats-Unis
    # Guyane britannique - Demerara-Mahaica
    # Découper selon les -: Si y'en a 1 qui est collés aux lettres -> Français
    # Sinon c'est une région composée
    if (
        "-" in region
        and "sud" not in region.lower()
        and "ouest" not in region.lower()
        and "rhône" not in region.lower()
        and "rhone" not in region.lower()
        and "alpes" not in region.lower()
    ):
        print(region)
        # Trier le cas Rhones-Alpes
        country = region.split(" - ")[0].strip().title()
        region = region.split(" - ")[1].strip().title()
        if region == country:
            region = None
    else:
        country = "France"
    if region == "Autres régions":
        region = None
    return country, region


def treat_bottle_size_idealwine(col):
    bottle_size = col.bottle_size.replace("L", "")
    return bottle_size


for row, col in df.iterrows():
    grape = treat_grape_idealwine(col)
    name = treat_name_idealwine(col)
    vintage = treat_vintage_idealwine(col)
    country, region = treat_region_country_idealwine(col)
    bottle_size = treat_bottle_size_idealwine(col)

    item = CleanedWineItem(col.to_dict())
    item.pop("id")
    item["grape"] = grape
    item["name"] = name
    item["vintage"] = vintage
    item["country"] = country
    item["region"] = region
    item["bottle_size"] = bottle_size

    process_item(item)

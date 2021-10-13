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


def treat_grape_vinsfins(col):
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
                final_grape_list.append(grape.title().strip())

        final_grape = "/".join(final_grape_list) if len(final_grape_list) else None
        return final_grape


def treat_name_vinsfins(col):
    name = col["name"]
    name = name.split(col.ranking)[0]
    name = name.split(col.vintage)[0]
    return name


def treat_vintage_vinsfins(col):
    vintage = col.vintage
    if vintage:
        if not vintage.isdigit():
            vintage = None
        return vintage
    return None


def treat_region_country_vinsfins(col):
    # Need to complete missing countries:
    # Example: we have the right region (Bourgogne)
    # but the country is missing
    pass


def treat_bottle_size_vinsfins(col):
    if col.bottle_size:
        bottle_size = col.bottle_size.replace("cl", "")
        bottle_size = round(int(bottle_size) / 100, 2)
        return bottle_size
    else:
        return None


def treat_alcool_vinsfins(col):
    alcool = col.alcool
    if alcool:
        alcool = alcool.replace(",", ".")
        if not alcool[-1].isdigit():
            return alcool[:-1]
    return alcool


def treat_viticulture_vinsfins(col):
    viticulture = col.viticulture
    if viticulture == "-":
        return None
    elif viticulture:
        viticulture = viticulture.title().strip()
        return viticulture
    return None


for row, col in df.iterrows():
    grape = treat_grape_vinsfins(col)
    name = treat_name_vinsfins(col)
    vintage = treat_vintage_vinsfins(col)
    # country, region = treat_region_country_vinsfins(col)
    bottle_size = treat_bottle_size_vinsfins(col)
    alcool = treat_alcool_vinsfins(col)
    viticulture = treat_viticulture_vinsfins(col)

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

    process_item(item)

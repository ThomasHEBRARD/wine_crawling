import statistics
import re
import psycopg2
import pandas as pd
from cleaned_items import CleanedWineItem
from word_recognition import rec


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
        brut_grapes = (
            brut_grapes.replace("\xa0", "").replace("ù", "%").replace("\t", "")
        )
        final_grape_list = []

        to_remove = re.search(r"\((.*)\)", brut_grapes)
        if to_remove:
            to_remove = to_remove.group(1)
            if not "%" in to_remove:
                brut_grapes = brut_grapes.replace("({})".format(to_remove), "")

        if "assemblage" in brut_grapes:
            brut_grapes = brut_grapes.split("assemblage")[0]

        if "%" in brut_grapes:
            grapes = re.split(r"%", brut_grapes)
            if grapes[0].strip().isdigit():
                current_percentage = grapes[0]
                current_grape = None
                for i in range(len(grapes)):
                    if not re.findall("[0-9]+", grapes[i + 1]):
                        current_grape = (
                            grapes[i + 1]
                            .replace(".", "")
                            .replace("-", " ")
                            .strip()
                            .title()
                        )
                        final_grape_list.append(
                            str(current_percentage) + "_" + str(current_grape)
                        )
                        break
                    next_percentage = re.findall("[0-9]+", grapes[i + 1])[0].strip()
                    current_grape = (
                        grapes[i + 1]
                        .replace(str(next_percentage), "")
                        .replace(".", "")
                        .replace("-", " ")
                        .strip()
                        .title()
                    )
                    final_grape_list.append(
                        str(current_percentage) + "_" + str(current_grape)
                    )
                    current_percentage = next_percentage
            else:
                for i in range(len(grapes)):
                    if re.findall("[0-9]+", grapes[i]):
                        current_percentage = re.findall("[0-9]+", grapes[i])[0]
                        current_grape = re.sub(
                            r"(?<!\d),(?!\d)",
                            "",
                            grapes[i]
                            .replace(str(current_percentage), "")
                            .replace(".", "")
                            .replace("-", " ")
                            .replace("(", " ")
                            .replace(")", " ")
                            .strip()
                            .title(),
                        )
                        final_grape_list.append(
                            str(current_percentage) + "_" + str(current_grape)
                        )
        print(
            [
                (grape.split("_")[1], rec(grape.split("_")[1]))
                for grape in final_grape_list
            ]
        )






        #### EN FAIT, GARER LES CHIFFRES/POURCENTAGE ON S'EN FOU, L'ALGO VA PRENDRE LE MEILLEUR MATCH QUOI QU'IL ARRIVE, ET ENSUITE IL FAUT MATCHER LES CHIFFRES

    ###
    # if col.grape:
    #     brut_grapes = col.grape.strip()
    #     brut_grapes = brut_grapes.replace("\xa0", "")
    #     brut_grapes = brut_grapes.replace("\t", "")
    #     grapes = re.split(r"(?<!\d),(?!\d)", brut_grapes)
    #     final_grape_list = []
    #     itermax = 50
    #     iter = 0

    #     while not all([g.count("%") <= 1 for g in grapes]) and iter < itermax:
    #         iter += 1
    #         for i in range(len(grapes)):
    #             if grapes[i].count("%") > 1:
    #                 grape = grapes.pop(i)
    #                 if len(s := re.split(r"(?<!\d),", grape)) > 1:
    #                     grapes[i:i] = s
    #                 elif len(s := re.split(r"(?<!net)-(?!\d)", grape)) > 1:
    #                     grapes[i:i] = s
    #                 elif len(s := re.split(r" et (?<!\d\%)", grape)) > 1:
    #                     grapes[i:i] = s
    #                 elif len(s := grape.split(";")) > 1:
    #                     grapes[i:i] = s
    #                 else:
    #                     grapes[i:i] = grape.split("?")
    #         if iter > 48:
    #             print(grapes)

    #     for grape in grapes:
    #         done = False
    #         grape = grape.replace(",", ".")

    #         if "%" in grape:
    #             if p := re.search(r"\((.*)%\)", grape):
    #                 if not done:
    #                     percentage = p.group(1)
    #                     grape_name = grape.split("(" + percentage + "%)")[0].strip()
    #                     if percentage.isdigit():
    #                         done = True
    #             if p := re.search(r"(.*)%", grape):
    #                 if not done:
    #                     percentage = p.group(1)
    #                     grape_name = grape.split(percentage + "%")[1].strip()
    #                     if percentage.isdigit():
    #                         done = True
    #             if p := re.search("\d à \d", grape):
    #                 if not done:
    #                     percentage = str(
    #                         statistics.mean(
    #                             [int(s) for s in grape.split("%")[0].split("à")]
    #                         )
    #                     )
    #                     grape_name = grape.split("%")[1]
    #                     if percentage.isdigit():
    #                         done = True
    #             if p := re.search("\d-\d", grape):
    #                 if not done:
    #                     percentage = str(
    #                         statistics.mean(
    #                             [int(s) for s in grape.split("%")[0].split("-")]
    #                         )
    #                     )
    #                     grape_name = grape.split("%")[1]
    #                     if percentage.isdigit():
    #                         done = True

    #             if grape.split("%")[-1] == "":
    #                 if not done:
    #                     percentage_match = re.search(
    #                         r"[-+]?\d*\.\d+|\d+", grape
    #                     ) or re.search(r"[-+]?\d*\.\d+|\d+ ", grape)
    #                     percentage = percentage_match.group(0)
    #                     grape_name = grape.split(percentage + "%")[0]
    #                     if percentage.isdigit():
    #                         done = True

    #             grape_name = (
    #                 grape_name.replace(".", "").replace("-", " ").strip().title()
    #             )
    #             final_grape_list.append(percentage.strip() + "_" + grape_name)
    #         else:
    #             final_grape_list.append(
    #                 grape.replace(".", "")
    #                 .replace("_", "")
    #                 .replace("-", " ")
    #                 .strip()
    #                 .title()
    #             )

    #     final_grape = "/".join(final_grape_list) if len(final_grape_list) else None
    #     return final_grape

    # virer tout ce qui est entre parenthèse si y'a pas de pourcentage
    # Michel Couvreur Candid (70cl)
    # "La_folle_blanche" -> https://www.idealwine.com/fr/acheter-vin/B2110137-45394-1-Bouteille-Vin-de-France-Marguerite-LEcu-Domaine-de-2019-Blanc.jsp
    # Autre cas: 50% cabernet sauvignon, 40% merlot, 5% petit verdot 5% cabernet franc
    # 70_Cab.Sauvignon/23_Merlot/7_Cabernet Franc
    # https://www.idealwine.com/fr/acheter-vin/B2110040-34108-1-Bouteille-Chateau-Monbrison-CBO-a-partir-de-12bts-2017-Rouge.jsp
    # https://www.idealwine.com/fr/acheter-vin/B2110040-61887-1-Bouteille-Chateau-La-Prade-2014-Rouge.jsp
    # https://www.idealwine.com/fr/acheter-vin/B2110040-55125-1-Bouteille-Chateau-la-Conseillante-CBO-a-partir-de-6-bts-2018-Rouge.jsp
    # https://www.idealwine.com/fr/acheter-vin/B2110040-56250-1-Bouteille-Chateau-la-Clotte-Grand-Cru-Classe-CBO-a-partir-de-6-bts-2014-Rouge.jsp
    # 70% Merlot 20% cabernet sauvgnon 10_Petit Verdot https://www.idealwine.com/fr/acheter-vin/B2110040-68435-1-Bouteille-Chateau-Rollan-de-By-Cru-Bourgeois-2015-Rouge.jsp
    # ['60% Cabernet Sauvignon', ' 35% Merlot 5% Petit verdot']
    # ['80% Merlot 80%', ' 20% Cabernet franc']
    # ['70% Syrah >Syrah 20%Grenache 10% Mourvèdre']
    # ['30 à 40% Savagnin 60 à 70% chardonnay']
    # ['70% Merlot 20% cabernet sauvgnon 10% Petit verdot']   sauvgnon
    # ['Cab. Sauv. 50% Merlot 40% Cab. Fc 5%', ' P. Verdot 5%']
    # ['70% Syrah >Syrah 20%Grenache 10% Mourvèdre']
    # ['Clairette 40% Grenache 30% bourboulenc', ' Roussanne']
    # ou sinon splitet par %, et faire le tri

    # -> Pour les https, prendre le chiffre (ou pas) devant et prendre le truc juste avant jsp, c'est le nom du cepage

    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "100% https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp, https://www.idealwine.com/fr/decouverte/cepage_pinot-noir.jsp"
    # "100% https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_savagnin.jsp, https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp, https://www.idealwine.com/fr/decouverte/cepage_pinot-noir.jsp"
    # "https://www.idealwine.com/fr/decouverte/cepage_chardonnay.jsp"


def treat_name_idealwine(col):
    name = col["name"]
    name = name.split(str(col.ranking))[0]
    name = name.split(str(col.vintage))[0]
    name = name.replace(
        "({}cl)".format(int(float(col.bottle_size.replace("L", "")) * 100)), ""
    )
    if "cbo" in name.lower():
        name = re.split(r"\(cbo", name)[0]
    if "Cbo" in name:
        name = re.split(r"\(Cbo", name)[0]
    if "CBO" in name:
        name = re.split(r"\(CBO", name)[0]
    # remove winery
    return name


def treat_vintage_idealwine(col):
    vintage = col.vintage
    if vintage:
        if not vintage.isdigit():
            vintage = None
        return vintage
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
    # Japan, Hso - ddzdok (c'est l'île)
    if (
        "-" in region
        and "sud" not in region.lower()
        and "ouest" not in region.lower()
        and "rhône" not in region.lower()
        and "rhone" not in region.lower()
        and "alpes" not in region.lower()
    ):
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

    # process_item(item)

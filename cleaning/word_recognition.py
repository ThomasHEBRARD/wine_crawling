import unidecode
from fuzzywuzzy import fuzz, process
import statistics
import re
import psycopg2
import pandas as pd


def postgresql_to_dataframe(select_query, column_names):
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="1234",
        port=5433,
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
    SELECT name, variants FROM public.business_grape
    where verified = True
    """

column_names = ["name", "variants"]


def treat_word(word):
    return unidecode.unidecode(
        word.lower().replace("_", " ").replace("-", " ").replace(".", "")
    )


df = postgresql_to_dataframe(select_query, column_names)

grapes = {
    treat_word(name): treat_word(variants).split(",")
    for name, variants in df.itertuples(index=False)
}


def rec(word):
    word = treat_word(word)
    max = (None, 0)

    for name, variants in grapes.items():
        # process name
        if (s := fuzz.ratio(name, word)) > max[1]:
            max = (name, s)

        # process all variants
        for variant in variants:
            if (s := fuzz.ratio(variant, word)) > max[1]:
                max = (name, s)

    return max[0]


# print(rec("cab fr"))
# print(rec("Chenin Blan"))
# print(rec("De Cabernet Franc"))
# print(rec("De Cabernet Sauvignon"))
# print(rec("De Merlot"))
# print(rec("De Petit Verdot"))
# print(rec("Gewurztra"))
# print(rec("Macabeo"))
# print(rec("Malbec."))
# print(rec("Merlot (Pas De Petit Verdot Dans Ce Millésime)"))
# print(rec("Mondeuse"))
# print(rec("Muscat blanc à petits grains"))
# print(rec("Poulsar"))
# print(rec("Uugni"))
####### >------>_>_>_>_>_ ('Pressac', 'prensal')
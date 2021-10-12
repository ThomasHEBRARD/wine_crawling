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
    WHERE website = 'millesima'
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
            if v:
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



def treat_price_millesima(col):
    price = col.price
    if price:
        return price.replace(",", ".")
    return None


for row, col in df.iterrows():
    price = treat_price_millesima(col)

    item = CleanedWineItem(col.to_dict())

    item.pop("id")
    item["price"] = price

    process_item(item)

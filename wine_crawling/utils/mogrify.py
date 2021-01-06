import psycopg2


def execute_mogrify(connection, df, table):
    """
    cursor.mogrify() to build the bulk insert query
    mogrify is one of the fastest bulk insert query for dataframes
    """
    # TODO : Multi tenant
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))
    cursor = connection.cursor()
    values = [
        cursor.mogrify("(" + ("%s," * len(tup))[:-1] + ")", tup).decode("utf8") for tup in tuples
    ]
    query = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)

    try:
        cursor.execute(query, tuples)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()
        return 1
    cursor.close()

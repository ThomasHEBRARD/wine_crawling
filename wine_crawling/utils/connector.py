import psycopg2

def connector_db():
    connection = psycopg2.connect(
        user="postgres",
        password="1234",
        host="localhost",
        port=5432,
        database="postgres",
    )
    return connection
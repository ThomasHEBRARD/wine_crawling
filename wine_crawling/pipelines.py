from itemadapter import ItemAdapter
import psycopg2


class WinePipeline:
    def open_spider(self, spider):
        hostname = "localhost"
        username = "postgres"
        password = "1234"
        database = "postgres"

        self.connection = psycopg2.connect(
            host=hostname, user=username, password=password, dbname=database
        )
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            creation_query = item.get_table_creation_query()
            self.cursor.execute(creation_query)
            self.connection.commit()

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
            self.cursor.execute(query)
            self.connection.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            self.cursor.close()

        return item


class GrapePipeline:
    def open_spider(self, spider):
        hostname = "localhost"
        username = "postgres"
        password = "1234"
        database = "postgres"
        port = 5433

        self.connection = psycopg2.connect(
            host=hostname, user=username, password=password, dbname=database, port=port
        )
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
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
            # self.cursor.execute(query)
            # self.connection.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            self.cursor.close()

        return item

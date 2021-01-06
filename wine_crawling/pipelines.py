from itemadapter import ItemAdapter
import psycopg2

class IdealWinePipeline:
    def open_spider(self, spider):
        hostname = "localhost"
        username = "postgres"
        password = "1234"
        database = "postgres"

        self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        try:
            query = f""" CREATE TABLE IF NOT EXISTS public.bottles
                    (
                        name VARCHAR(200),
                        color VARCHAR(200),
                        appellation VARCHAR(200),
                        domaine VARCHAR(200),
                        producteur VARCHAR(200),
                        cepage VARCHAR(200),
                        millesime VARCHAR(200),
                        classement VARCHAR(200),
                        pays VARCHAR(200),
                        price VARCHAR(200),
                        url VARCHAR(200)
                    );
                    """
            self.cursor.execute(query)
            self.connection.commit()
            self.cursor.execute(
                """
                INSERT INTO 
                public.bottles({})
                VALUES{}
                """.format(
                    ','.join(item.keys()),
                    tuple([str(it).replace("'", " ") for it in item.values()])
                    )
            )
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.connection.rollback()
            self.cursor.close()

        return item

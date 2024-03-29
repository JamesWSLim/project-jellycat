from scraping.scrape_aggregate import *
from medallion_bronze.bronze_jellycat import *
from medallion_bronze.bronze_size import *
from medallion_bronze.bronze_stock import *
from medallion_silver.silver import *
from medallion_gold.gold import *

from pyspark.sql import SparkSession
from delta import *

def daily_scraping():
    ### jellycat main page scraping
    try:
        print("main page scraping started ;)")
        ### scrape main page
        df = scrape_main_page()
        df = df[["jellycatname", "category", "link", "imagelink", "datecreated"]]

        ### Connect to your postgres DB
        conn = psycopg2.connect(
            host="localhost",
            database="jellycat_db",
            user="root",
            password="root")

        ### drop jellycat table if exist with cascade
        sql = """DROP TABLE IF EXISTS jellycat CASCADE"""
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        ### create jellycat table
        sql = """CREATE TABLE jellycat (
            jellycatid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            jellycatname TEXT,
            category TEXT,
            link TEXT,
            imagelink TEXT,
            datecreated timestamp
        );"""
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        ### insert jellycat data to postgresql
        engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')
        df.to_sql('jellycat', engine, if_exists='append', index=False)
        print("main page done ;)")

    except:
        print("main page failed!")

    ### sizes scraping
    try:
        print("sizes page scraping started ;)")

        ### Retrieve query results (to get primary keys)
        sql = '''select * 
            from jellycat'''
        df_jellycat = pd.read_sql_query(sql, conn)

        ### create a csv file with today's date for tracking
        date_today = date.today()
        df_jellycat.to_csv(f"csv_data/jellycat_{date_today}.csv", index=False)

        ### retrieve needed columns
        df_jellycat = df_jellycat.reset_index()[["jellycatid", "jellycatname", "link"]]

        ### scrape jellycat sizes by jellycat_id
        df_sizes = jellycat_sizes_by_id(df_jellycat)
        df_sizes = data_cleaning(df_sizes)

        ### drop size table if exist with cascade (dropping all the foreign tables)
        sql = """DROP TABLE IF EXISTS size CASCADE"""
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        ### create size table
        sql = """CREATE TABLE size (
            jellycatsizeid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            jellycatid uuid,
            jellycatname TEXT,
            size TEXT,
            height DECIMAL,
            width DECIMAL,
            price DECIMAL,
            stock TEXT,
            datecreated timestamp,
            CONSTRAINT jellycatsizeidfk FOREIGN KEY(jellycatid) REFERENCES jellycat(jellycatid)
        );"""
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        ### insert sizes data into postgresql
        df_sizes.to_sql('size', engine, if_exists='append', index=False)

        ## Retrieve query results (to get primary keys)
        sql = '''select * 
            from size'''
        df_sizes = pd.read_sql_query(sql, conn)

        ### create a csv file with today's date for tracking
        date_today = date.today()
        df_sizes.to_csv(f"csv_data/jellycat_sizes_{date_today}.csv", index=False)
        
        print("sizes page done ;)")
    
    except:
        print("sizes page failed!")

    ### stock scraping
    try:
        # conn = psycopg2.connect(
        #     host="localhost",
        #     database="jellycat_db",
        #     user="root",
        #     password="root")
        
        # engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')

        # sql = '''select * 
        #     from jellycat'''
        # df_jellycat = pd.read_sql_query(sql, conn)
        
        # sql = '''select * 
        #     from size'''
        # df_sizes = pd.read_sql_query(sql, conn)
        
        print("stock page scraping started ;)")

        df_jellycat_size = pd.merge(df_jellycat, df_sizes, on="jellycatid")
        df_jellycat_size = df_jellycat_size[df_jellycat_size["stock"]=="In Stock"]
        ### retrieve needed columns
        df_jellycat_size = df_jellycat_size.reset_index()

        ### scrape jellycat stocks by jellycat_id
        df_stocks = scrape_stock_count_by_sizes(df_jellycat_size)
        df_stocks = df_stocks[["jellycatsizeid","stockcount"]]

        ### drop stock table if exist with cascade (dropping all the foreign tables)
        sql = """DROP TABLE IF EXISTS stock CASCADE"""
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        ### create stock table
        sql = """CREATE TABLE stock (
            jellycatstockid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            jellycatsizeid uuid,
            stockcount INTEGER,
            CONSTRAINT jellycatstockidfk FOREIGN KEY(jellycatsizeid) REFERENCES size(jellycatsizeid)
        );"""
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()

        ### insert stocks data into postgresql
        df_stocks.to_sql('stock', engine, if_exists='append', index=False)

        ### Retrieve query results (to get primary keys)
        sql = '''select * 
            from stock'''
        df_stocks = pd.read_sql_query(sql, conn)

        ### create a csv file with today's date for tracking
        date_today = date.today()
        df_stocks.to_csv(f"csv_data/jellycat_stocks_{date_today}.csv", index=False)
        
        print("stock page done ;)")
    
    except:
        print("stock page failed!")

    try:
        ### start spark engine
        builder = SparkSession \
                    .builder.appName("Jellycat-ETL") \
                    .config("spark.jars", "postgresql-42.7.1.jar") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        ### ingest data from postgresql to delta lake
        bronze_jellycat(spark)
        bronze_size(spark)
        bronze_stock(spark)

        builder = SparkSession \
            .builder.appName("Jellycat-ETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        gold_aggregate(spark)

        ### silver level
        silver_all_join(spark)

        ### gold level
        gold_aggregate(spark)
        print("ETL done ;)")

    except:
        print("ETL failed!")
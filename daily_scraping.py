from scraping.jellycat_and_size_scrape import *
from medallion_bronze.bronze_jellycat import *
from medallion_bronze.bronze_size import *
from medallion_silver.all_join import *

from pyspark.sql import SparkSession
from delta import *

# ### scrape main page
# df = scrape_main_page()
# df = df[["jellycatname", "category", "link", "imagelink", "datecreated"]]

# ### Connect to your postgres DB
# conn = psycopg2.connect(
#     host="localhost",
#     database="jellycat_db",
#     user="root",
#     password="root")

# ### drop jellycat table if exist with cascade
# sql = """DROP TABLE IF EXISTS jellycat CASCADE"""
# cursor = conn.cursor()
# cursor.execute(sql)
# conn.commit()


# ### create jellycat table
# sql = """CREATE TABLE jellycat (
#     jellycatid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
#     jellycatname TEXT,
#     category TEXT,
#     link TEXT,
#     imagelink TEXT,
#     datecreated timestamp
# );"""
# cursor = conn.cursor()
# cursor.execute(sql)
# conn.commit()

# ### insert jellycat data to postgresql
# engine = create_engine('postgresql://root:root@localhost:5432/jellycat_db')
# df.to_sql('jellycat', engine, if_exists='append', index=False)

# ### Retrieve query results (to get primary keys)
# sql = '''select * 
#     from jellycat'''
# df = pd.read_sql_query(sql, conn)

# ### create a csv file with today's date for tracking
# date_today = date.today()
# df.to_csv(f"./data/jellycat_with_primary_{date_today}.csv", index=False)

# ### retrieve needed columns
# df_primary = df.reset_index()[["jellycatid", "jellycatname", "link"]]

# ### scrape jellycat sizes by jellycat_id
# df_sizes = jellycat_sizes_by_id(df_primary)
# df_sizes = data_cleaning(df_sizes)

# ### drop size table if exist with cascade (dropping all the foreign tables)
# sql = """DROP TABLE IF EXISTS size CASCADE"""
# cursor = conn.cursor()
# cursor.execute(sql)
# conn.commit()

# ### create size table
# sql = """CREATE TABLE size (
#     jellycatsizeid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
#     jellycatid uuid,
#     jellycatname TEXT,
#     size TEXT,
#     height DECIMAL,
#     width DECIMAL,
#     price DECIMAL,
#     stock TEXT,
#     datecreated timestamp,
#     CONSTRAINT jellycatsizeidfk FOREIGN KEY(jellycatid) REFERENCES jellycat(jellycatid)
# );"""
# cursor = conn.cursor()
# cursor.execute(sql)
# conn.commit()

# ### insert sizes data into postgresql
# df_sizes.to_sql('size', engine, if_exists='append', index=False)

# ### Retrieve query results (to get primary keys)
# sql = '''select * 
#     from size'''
# df_sizes = pd.read_sql_query(sql, conn)

# ### create a csv file with today's date for tracking
# date_today = date.today()
# df_sizes.to_csv(f"./data/jellycat_sizes_with_primary_{date_today}.csv", index=False)

### start spark engine
builder = SparkSession \
            .builder.appName("Jellycat-ETL") \
            .config("spark.jars", "postgresql-42.7.1.jar") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ### ingest data from postgresql to delta lake
# bronze_jellycat(spark)
# bronze_size(spark)

### silver level filters
silver_all_join(spark)
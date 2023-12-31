from pyspark.sql import SparkSession
from delta import *

builder = SparkSession \
            .builder \
            .appName("Jellycat-ETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sql(
    """CREATE TABLE IF NOT EXISTS default.bronzejellycat ( 
        jellycatid STRING, 
        jellycatname STRING,
        category STRING,
        link STRING,
        imagelink STRING,
        datecreated TIMESTAMP
        ) USING DELTA 
        TBLPROPERTIES (delta.enableChangeDataFeed = true)"""
)

# spark.sql(
#     """CREATE TABLE default.bronzesize ( 
#         jellycatsizeid STRING, 
#         jellycatid STRING, 
#         size STRING, 
#         price DECIMAL, 
#         stock STRING,
#         datecreated DATE
#         ) USING delta 
#         TBLPROPERTIES (delta.enableChangeDataFeed = true)"""
# )
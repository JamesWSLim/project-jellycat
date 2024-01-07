from pyspark.sql import SparkSession
from delta import *

builder = SparkSession \
            .builder \
            .appName("Jellycat-ETL") \
            .config("spark.jars", "postgresql-42.7.1.jar") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sql(
    """CREATE TABLE default.bronze_jellycat ( 
        jellycatid STRING, 
        jellycatname STRING,
        category STRING,
        link STRING, 
        imagelink STRING, 
        validfrom DATE, 
        validto DATE,
        current STRING 
        ) USING delta 
        TBLPROPERTIES (delta.enableChangeDataFeed = true)"""
)

spark.sql(
    """CREATE TABLE default.bronze_size ( 
        jellycatsizeid STRING, 
        jellycatid STRING, 
        size STRING, 
        price DECIMAL, 
        stock STRING
        validfrom DATE, 
        validto DATE,
        current STRING
        ) USING delta 
        TBLPROPERTIES (delta.enableChangeDataFeed = true)"""
)
from pyspark.sql import SparkSession
from delta import *

builder = SparkSession \
            .builder \
            .appName("Read from delta") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

table = DeltaTable.forPath(spark, '/Users/jameslim/Downloads/projects/jellycat-scraping/delta/jellycat_data')
table.toDF().show()
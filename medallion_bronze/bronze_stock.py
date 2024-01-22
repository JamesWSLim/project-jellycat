from pyspark.sql import SparkSession
from delta import *
import os
from pyspark.sql.types import DecimalType


def bronze_stock(spark):
    df_stock = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/jellycat_db") \
        .option("dbtable", "stock") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df_stock.write.format("delta").mode("append").save("./spark-warehouse/bronzestock")
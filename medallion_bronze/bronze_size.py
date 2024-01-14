from pyspark.sql import SparkSession
from delta import *
import os
from pyspark.sql.types import DecimalType


def bronze_size(spark):
    df_size = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/jellycat_db") \
        .option("dbtable", "size") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    df_size = df_size.withColumn("height",df_size["height"].cast(DecimalType(10,0)))
    df_size = df_size.withColumn("width",df_size["width"].cast(DecimalType(10,0)))
    df_size = df_size.withColumn("price",df_size["price"].cast(DecimalType(10,0)))
    
    df_size.write.format("delta").mode("append").save("./spark-warehouse/bronzesize")

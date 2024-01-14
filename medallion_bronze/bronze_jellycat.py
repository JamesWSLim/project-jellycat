from pyspark.sql import SparkSession
from delta import *
import os

    
def bronze_jellycat(spark):
    
    df_jellycat = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/jellycat_db") \
        .option("dbtable", "jellycat") \
        .option("user", "root") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    df_jellycat.write.format("delta").mode("append").save("./spark-warehouse/bronzejellycat")
    
from pyspark.sql import SparkSession
from delta import *

builder = SparkSession \
            .builder.appName("Jellycat-ETL") \
            .config("spark.jars", "postgresql-42.7.1.jar") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_jellycat = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/jellycat_db") \
    .option("dbtable", "jellycat") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_jellycat.write.format("delta").mode("append").save("./spark-warehouse/bronzejellycat")
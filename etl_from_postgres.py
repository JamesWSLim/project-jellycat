from pyspark.sql import SparkSession
from delta import *

builder = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.jars", "/Users/jameslim/Downloads/projects/jellycat-scraping/postgresql-42.7.1.jar") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/jellycat_db") \
    .option("dbtable", "jellycat_data") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
df.show(10)

DeltaTable.createIfNotExists(spark) \
  .tableName("default.jellycat_data") \
  .location("/Users/jameslim/Downloads/projects/jellycat-scraping/delta/jellycat_data") \
  .execute()
table = DeltaTable.forPath(spark, '/Users/jameslim/Downloads/projects/jellycat-scraping/delta/jellycat_data')
df.write\
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/Users/jameslim/Downloads/projects/jellycat-scraping/delta/jellycat_data")

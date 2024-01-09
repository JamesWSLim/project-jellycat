from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import *

builder = SparkSession \
            .builder.appName("Jellycat-ETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

jellycat_schema = StructType([
    StructField("JellycatID", StringType(), nullable=False),
    StructField("JellycatName", StringType(), nullable=False),
    StructField("Category", StringType(), nullable=True),
    StructField("Link", StringType(), nullable=True),
    StructField("ImageLink", StringType(), nullable=True),
    StructField("DateCreated", TimestampType(), nullable=False),
])


df_jellycat = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/jellycat_db") \
    .option("dbtable", "jellycat") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_jellycat.show()


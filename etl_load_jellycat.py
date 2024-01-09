import datetime
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import *

jellycat_schema = StructType([
    StructField("JellycatID", StringType(), nullable=False),
    StructField("JellycatName", StringType(), nullable=False),
    StructField("Category", StringType(), nullable=True),
    StructField("Link", StringType(), nullable=True),
    StructField("ImageLink", StringType(), nullable=True),
    StructField("DateCreated", TimestampType(), nullable=False),
])

size_schema = StructType([
    StructField("JellycatID", StringType(), nullable=False),
    StructField("JellycatName", StringType(), nullable=False),
    StructField("Size", StringType(), nullable=True),
    StructField("Price", DecimalType(), nullable=True),
    StructField("Stock", StringType(), nullable=True),
    StructField("DateCreated", TimestampType(), nullable=False),
    StructField("Height", DecimalType(), nullable=True),
    StructField("Width", DecimalType(), nullable=True),
])

### create a list of dates starting from 2024-01-05 till yesterday
startdate = '2024-01-05'
startdate = datetime.datetime.strptime(startdate, '%Y-%m-%d').date() - datetime.timedelta(days=1)
enddate = datetime.date.today() - datetime.timedelta(days=1)
total_days = enddate - startdate
date_list = [enddate - datetime.timedelta(days=x) for x in reversed(range(total_days.days))]

builder = SparkSession \
            .builder.appName("Jellycat-ETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### append data into delta table
for date in date_list:
    df_jellycat = spark.read.csv(f"./data/jellycat_with_primary_{date}.csv", header=True, schema=jellycat_schema, sep=",")
    df_jellycat.write.format("delta").mode("append").save("./spark-warehouse/bronzejellycat")
    df_sizes = spark.read.csv(f"./data/jellycat_sizes_with_primary_{date}.csv", header=True, schema=size_schema, sep=",")
    df_sizes.write.format("delta").mode("append").save("./spark-warehouse/bronzesize")

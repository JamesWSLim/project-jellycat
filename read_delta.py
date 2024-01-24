import pyspark
from delta import *
from pyspark.sql.types import *

builder = pyspark.sql.SparkSession.builder.appName("Jellycat-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# bronzejellycat = spark.read.format("delta") \
#     .load("./spark-warehouse/bronzejellycat")
# bronzejellycat.createOrReplaceTempView("jellycattemp")
# bronzejellycat.show()

# bronzesize = spark.read.format("delta") \
#     .load("./spark-warehouse/bronzesize")
# bronzesize.createOrReplaceTempView("sizetemp")
# bronzesize.show()

# bronzestock = spark.read.format("delta") \
#     .load("./spark-warehouse/bronzestock")
# bronzestock.createOrReplaceTempView("stocktemp")
# bronzestock.show()

df_revenue_day = spark.read.format("delta") \
    .load("./spark-warehouse/revenue-day")
df_revenue_day.createOrReplaceTempView("revenuedaytemp")

df_unit_sold = spark.sql(
    """
        SELECT * FROM revenuedaytemp
        ORDER BY revenue DESC;
    """
    )
df_unit_sold.show()
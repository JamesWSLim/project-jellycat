import pyspark
from delta import *
from pyspark.sql import SparkSession

def gold_aggregate(spark):

    ### daily revenue
    bronzejellycat = spark.read.format("delta") \
        .load("./spark-warehouse/revenue-day")
    bronzejellycat.createOrReplaceTempView("revenuedaytemp")

    ### aggregate by category
    df_agg_category = spark.sql(
    """
        SELECT category,
        SUM(unitsold) as totalunitsold,SUM(revenue)AS totalrevenue
        FROM revenuedaytemp
        GROUP BY category
    """
    )
    df_agg_category.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-category")

    ### aggregate by size
    df_agg_size = spark.sql(
    """
        SELECT size,
        SUM(unitsold) as totalunitsold,SUM(revenue)AS totalrevenue
        FROM revenuedaytemp
        GROUP BY size
    """
    )
    df_agg_size.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-size")

builder = SparkSession \
            .builder.appName("Jellycat-ETL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
gold_aggregate(spark)
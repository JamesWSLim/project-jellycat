import pyspark
from delta import *
from pyspark.sql import SparkSession

def gold_aggregate(spark):

    ### daily revenue
    revenue_df = spark.read.format("delta") \
        .load("./spark-warehouse/daily-revenue")
    revenue_df.createOrReplaceTempView("revenuetemp")

    ### aggregate by category
    df_agg_category = spark.sql(
        """
        SELECT category,MONTH(jellycatdatecreated) as month,YEAR(jellycatdatecreated) as year,
        SUM(unitsold) as totalunitsold, SUM(revenue) as totalrevenue
        FROM revenuetemp
        GROUP BY category, MONTH(jellycatdatecreated),YEAR(jellycatdatecreated);
    """
    )
    df_agg_category.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-category")

    ### aggregate by size
    df_agg_size = spark.sql(
        """
        SELECT size,MONTH(jellycatdatecreated) as month,YEAR(jellycatdatecreated) as year,
        SUM(unitsold) as totalunitsold, SUM(revenue) as totalrevenue
        FROM revenuetemp
        GROUP BY size, MONTH(jellycatdatecreated),YEAR(jellycatdatecreated);
    """
    )
    df_agg_size.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-size")
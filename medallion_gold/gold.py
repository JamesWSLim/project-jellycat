import pyspark
from delta import *
from pyspark.sql import SparkSession

def gold_aggregate(spark):

    ### daily revenue
    revenue_df = spark.read.format("delta") \
        .load("./spark-warehouse/daily-revenue")
    revenue_df.createOrReplaceTempView("revenuetemp")

    ### aggregate monthly by category
    df_agg_category_month = spark.sql(
        """
        SELECT category,MONTH(jellycatdatecreated) as month,YEAR(jellycatdatecreated) as year,
        SUM(unitsold) as totalunitsold, SUM(revenue) as totalrevenue
        FROM revenuetemp
        GROUP BY category, MONTH(jellycatdatecreated),YEAR(jellycatdatecreated);
    """
    )
    df_agg_category_month.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-category-month")

    ### aggregate daily by category
    df_agg_category_daily = spark.sql(
        """
        SELECT category,DAY(jellycatdatecreated) as day,MONTH(jellycatdatecreated) as month,YEAR(jellycatdatecreated) as year,
        SUM(unitsold) as totalunitsold, SUM(revenue) as totalrevenue
        FROM revenuetemp
        GROUP BY category,DAY(jellycatdatecreated), MONTH(jellycatdatecreated),YEAR(jellycatdatecreated);
    """
    )
    df_agg_category_daily.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-category-day")

    ### aggregate monthly by size
    df_agg_size_month = spark.sql(
        """
        SELECT size,MONTH(jellycatdatecreated) as month,YEAR(jellycatdatecreated) as year,
        SUM(unitsold) as totalunitsold, SUM(revenue) as totalrevenue
        FROM revenuetemp
        GROUP BY size, MONTH(jellycatdatecreated),YEAR(jellycatdatecreated);
    """
    )
    df_agg_size_month.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-size-month")

    ### aggregate daily by size
    df_agg_size_daily = spark.sql(
        """
        SELECT size,DAY(jellycatdatecreated) as day,MONTH(jellycatdatecreated) as month,YEAR(jellycatdatecreated) as year,
        SUM(unitsold) as totalunitsold, SUM(revenue) as totalrevenue
        FROM revenuetemp
        GROUP BY size,DAY(jellycatdatecreated),MONTH(jellycatdatecreated),YEAR(jellycatdatecreated);
    """
    )
    df_agg_size_daily.write.format("delta").mode("overwrite").save("./spark-warehouse/revenue-agg-size-day")
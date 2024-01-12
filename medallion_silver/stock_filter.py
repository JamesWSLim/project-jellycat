import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("Jellycat-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### load df
df_joined = spark.read.format("delta") \
    .load("./spark-warehouse/silver-all")
df_joined.createOrReplaceTempView("alltemp")

### returning this week
df_returning_this_week = spark.sql(
    """
    SELECT * from alltemp
    WHERE LOWER(stock) LIKE LOWER('%Returning to stock this week%')
    AND DATE(jellycatdatecreated) = DATE(CURRENT_TIMESTAMP)
    ORDER BY jellycatname ASC;
"""
)
df_returning_this_week.write.format("delta").mode("overwrite").save("./spark-warehouse/returning-this-week")

### returning next week
df_returning_next_week = spark.sql(
    """
    SELECT * from alltemp
    WHERE LOWER(stock) LIKE LOWER('%Returning to stock next week%')
    AND DATE(jellycatdatecreated) = DATE(CURRENT_TIMESTAMP)
    ORDER BY jellycatname ASC;
"""
)
df_returning_next_week.write.format("delta").mode("overwrite").save("./spark-warehouse/returning-next-week")

### returning 3-4 weeks
df_returning_3_4_weeks = spark.sql(
    """
    SELECT * from alltemp
    WHERE LOWER(stock) LIKE LOWER('%Returning to stock 3-4 weeks%')
    AND DATE(jellycatdatecreated) = DATE(CURRENT_TIMESTAMP)
    ORDER BY jellycatname ASC;
"""
)
df_returning_next_week.write.format("delta").mode("overwrite").save("./spark-warehouse/returning-3-4-weeks")

### more on the way
df_more = spark.sql(
    """
    SELECT * from alltemp
    WHERE LOWER(stock) LIKE LOWER('%more on the way%')
    AND DATE(jellycatdatecreated) = DATE(CURRENT_TIMESTAMP)
    ORDER BY jellycatname ASC;
"""
)
df_more.show()
df_more.write.format("delta").mode("overwrite").save("./spark-warehouse/more-on-the-way")

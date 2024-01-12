import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("Jellycat-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### load df
bronzejellycat = spark.read.format("delta") \
    .load("./spark-warehouse/bronzejellycat")
bronzejellycat.createOrReplaceTempView("jellycattemp")

bronzesize = spark.read.format("delta") \
    .load("./spark-warehouse/bronzesize")
bronzesize.createOrReplaceTempView("sizetemp")

### select all data
all_df = spark.sql(
    """SELECT t1.jellycatid,t1.jellycatname,t1.category,t1.link,t1.imagelink,t1.datecreated as jellycatdatecreated,
    t2.size,t2.height,t2.width,t2.price,t2.stock,t2.datecreated as sizedatecreated
    FROM jellycattemp t1
    LEFT JOIN sizetemp t2 on t1.jellycatid=t2.jellycatid
    """
)
all_df.createOrReplaceTempView("alltemp")
all_df.write.format("delta").mode("overwrite").save("./spark-warehouse/all")

### in stock
df_in_stock = spark.sql(
    """
    SELECT * from alltemp
    WHERE LOWER(stock) LIKE LOWER('%In Stock%')
"""
)
df_in_stock.write.format("delta").mode("overwrite").save("./spark-warehouse/in-stock")

### out of stock
df_out_of_stock = spark.sql(
    """
    SELECT * from alltemp
    WHERE LOWER(stock) NOT LIKE LOWER('%In Stock%')
"""
)
df_out_of_stock.write.format("delta").mode("overwrite").save("./spark-warehouse/out-of-stock")

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
df_more.write.format("delta").mode("overwrite").save("./spark-warehouse/more-on-the-way")
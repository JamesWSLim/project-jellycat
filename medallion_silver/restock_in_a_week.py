import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("Jellycat-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### load df
df_joined = spark.read.format("delta") \
    .load("./spark-warehouse/all")
df_joined.createOrReplaceTempView("alltemp")

df_size = spark.read.format("delta") \
    .load("./spark-warehouse/all")
df_size.createOrReplaceTempView("sizetemp")

### join table today with data 3 days ago
df_3_days_diff = spark.sql(
    """
    SELECT t1.jellycatname,t1.jellycatdatecreated,
    t2.jellycatname as t2jellycatname,t2.sizedatecreated,t2.stock as stock3daysago,
    t1.stock as stocktoday,t1.link,t1.imagelink,t1.price,t1.size,t1.height,t1.width
    FROM alltemp t1
    LEFT JOIN (
        SELECT * FROM sizetemp
        WHERE DATE(sizedatecreated)=DATEADD(day, -3, DATE(CURRENT_TIMESTAMP))
    ) t2 ON t2.jellycatname=t1.jellycatname AND t2.size=t1.size
    WHERE DATE(t1.jellycatdatecreated) = DATE(CURRENT_TIMESTAMP)
"""
)
df_3_days_diff.createOrReplaceTempView("df3daysdiff")

df_restocked_within_3_days = spark.sql(
    """
    SELECT jellycatname,size,stocktoday,stock3daysago,link,imagelink,price,height,width FROM df3daysdiff
    WHERE LOWER(stocktoday) LIKE LOWER('%In Stock%')
    AND LOWER(stock3daysago) NOT LIKE LOWER('%In Stock%')
"""
)
df_restocked_within_3_days.write.format("delta").mode("overwrite").save("./spark-warehouse/restocked-within-3-days")

df_outofstock_within_3_days = spark.sql(
    """
    SELECT jellycatname,size,stocktoday,stock3daysago,link,imagelink,price,height,width FROM df3daysdiff
    WHERE LOWER(stocktoday) NOT LIKE LOWER('%In Stock%')
    AND LOWER(stock3daysago) LIKE LOWER('%In Stock%')
"""
)
df_outofstock_within_3_days.write.format("delta").mode("overwrite").save("./spark-warehouse/outofstock-within-3-days")
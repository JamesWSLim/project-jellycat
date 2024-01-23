import pyspark
from delta import *
from pyspark.sql.types import *

builder = pyspark.sql.SparkSession.builder.appName("Jellycat-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

bronzejellycat = spark.read.format("delta") \
    .load("./spark-warehouse/bronzejellycat")
bronzejellycat.createOrReplaceTempView("jellycattemp")
bronzejellycat.show()

bronzesize = spark.read.format("delta") \
    .load("./spark-warehouse/bronzesize")
bronzesize.createOrReplaceTempView("sizetemp")
bronzesize.show()

bronzestock = spark.read.format("delta") \
    .load("./spark-warehouse/bronzestock")
bronzestock.createOrReplaceTempView("stocktemp")
bronzestock.show()

### select all data
all_df = spark.sql(
    """SELECT t1.jellycatid,t1.jellycatname,t1.category,t1.link,t1.imagelink,t1.datecreated as jellycatdatecreated,
    t2.jellycatsizeid,t2.size,t2.height,t2.width,t2.price,t2.stock,t2.datecreated as sizedatecreated, t3.stockcount
    FROM jellycattemp t1
    LEFT JOIN sizetemp t2 on t1.jellycatid=t2.jellycatid
    LEFT JOIN stocktemp t3 on t2.jellycatsizeid=t3.jellycatsizeid
    """
)
all_df.show()
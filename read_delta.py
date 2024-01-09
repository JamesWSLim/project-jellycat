import pyspark
from delta import *
from pyspark.sql.types import *

builder = pyspark.sql.SparkSession.builder.appName("Jellycat-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ### read data with Change Data Feed
# bronze_jellycat_cdf = spark.read.format("delta") \
#     .option("readChangeFeed", "true") \
#     .option("startingVersion", 0) \
#     .option("endingVersion", 10) \
#     .load("./spark-warehouse/bronzejellycat")

# bronze_size_cdf = spark.read.format("delta") \
#     .option("readChangeFeed", "true") \
#     .option("startingVersion", 0) \
#     .option("endingVersion", 10) \
#     .load("./spark-warehouse/bronzesize")

# bronze_size_cdf.show()

#### read data by version (time travel)
# bronze_jellycat_version = spark.read.format("delta") \
#     .option("versionAsOf", 1) \
#     .load("./spark-warehouse/bronze_jellycat")

# bronze_jellycat_version.show()

# bronzejellycat = spark.read.format("delta") \
#     .load("./spark-warehouse/bronzejellycat")
# bronzejellycat.createOrReplaceTempView("jellycatjoin")

# bronzesize = spark.read.format("delta") \
#     .load("./spark-warehouse/bronzesize")
# bronzesize.createOrReplaceTempView("sizejoin")

# joined_jellycat_size = spark.sql(
#     """
#     SELECT t1.jellycatid,t1.jellycatname,t1.category,t1.link,t1.imagelink,t1.datecreated as jellycatdatecreated,
#     t2.size,t2.price,t2.stock,t2.height,t2.width
#     FROM jellycatjoin t1
#     LEFT JOIN sizejoin t2 on t1.jellycatid=t2.jellycatid
#     """
# )
# joined_jellycat_size.show()

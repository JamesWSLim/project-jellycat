from pyspark.sql import SparkSession
from delta import *

def merge_to_size_table(spark, df):

    sizeTable = DeltaTable.forPath(spark, "./spark-warehouse/bronze_size")
    updates = df
    sizes = sizeTable.toDF().alias("sizes")

    # Rows to INSERT new information of existing sizes
    newAddressesToInsert = updates \
        .alias("updates") \
        .join(sizes, (updates.jellycatname == sizes.jellycatname)
            & (updates.size == sizes.size)) \
        .where("sizes.current = true AND \
            updates.category <> sizes.category OR \
            updates.link <> sizes.link OR \
            updates.imagelink <> sizes.imagelink")

    # Stage the update by unioning two sets of rows
    # 1. Rows that will be inserted in the whenNotMatched clause
    # 2. Rows that will either update the current addresses of existing agents 
    #    or insert the new addresses of new agents
    stagedUpdates = (
        newAddressesToInsert
        .selectExpr("Null as mergeKey", "updates.*") # Rows for 1
        .union(updates.selectExpr("sizeID as mergeKey", "*")) # Rows for 2
    )

    # Apply SCD Type 2 operation using merge
    sizeTable.alias("sizes").merge(
        stagedUpdates.alias("staged_updates"),
        "sizes.sizeID = mergeKey") \
    .whenMatchedUpdate(
        condition = "sizes.current = true AND \
                    sizes.category <> staged_updates.category OR \
                    sizes.link <> staged_updates.link OR \
                    sizes.imagelink <> staged_updates.imagelink",
        set = {
            "validto": "staged_updates.datecreated",
            "current": "false"
        }
    ).whenNotMatchedInsert(
        values = {
            "sizename": "staged_updates.sizeName",
            "category": "staged_updates.Category",
            "link": "staged_updates.Link",
            "imagelink": "staged_updates.ImageLink",
            "validfrom": "staged_updates.DateCreated",
            "validto": "null",
            "current": "true"
        }
    ).execute()

builder = SparkSession \
            .builder \
            .appName("Jellycat-ETL") \
            .config("spark.jars", "postgresql-42.7.1.jar") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_jellycat = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/jellycat_db") \
    .option("dbtable", "jellycat") \
    .option("user", "root") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()

merge_to_size_table(spark, df_jellycat)
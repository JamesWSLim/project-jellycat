from pyspark.sql import SparkSession
from delta import *

def merge_to_jellycat_table(spark, df):

    jellycatTable = DeltaTable.forPath(spark, "./spark-warehouse/bronze_jellycat")
    updates = df

    # Rows to INSERT new information of existing jellycats
    newAddressesToInsert = updates \
        .alias("updates") \
        .join(jellycatTable.toDF().alias("jellycats"), "jellycatid") \
        .where("jellycats.current = true AND \
            updates.category <> jellycats.category OR \
            updates.link <> jellycats.link OR \
            updates.imagelink <> jellycats.imagelink")

    # Stage the update by unioning two sets of rows
    # 1. Rows that will be inserted in the whenNotMatched clause
    # 2. Rows that will either update the current addresses of existing agents 
    #    or insert the new addresses of new agents
    stagedUpdates = (
        newAddressesToInsert
        .selectExpr("Null as mergeKey", "updates.*") # Rows for 1
        .union(updates.selectExpr("JellycatID as mergeKey", "*")) # Rows for 2
    )

    # Apply SCD Type 2 operation using merge
    jellycatTable.alias("jellycats").merge(
        stagedUpdates.alias("staged_updates"),
        "jellycats.JellycatID = mergeKey") \
    .whenMatchedUpdate(
        condition = "jellycats.current = true AND \
                    jellycats.category <> staged_updates.category OR \
                    jellycats.link <> staged_updates.link OR \
                    jellycats.imagelink <> staged_updates.imagelink",
        set = {
            "validto": "staged_updates.datecreated",
            "current": "false"
        }
    ).whenNotMatchedInsert(
        values = {
            "jellycatname": "staged_updates.JellycatName",
            "price": "staged_updates.Category",
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

merge_to_jellycat_table(spark, df_jellycat)
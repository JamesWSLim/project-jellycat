import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### load df
bronze_policy = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_policy")
bronze_policy.createOrReplaceTempView("policy_to_join")

bronze_customer = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_customer")
bronze_customer.createOrReplaceTempView("customer_to_join")

bronze_agent = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_agent")
bronze_agent.createOrReplaceTempView("agent_to_join")
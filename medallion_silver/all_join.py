import pyspark
from delta import *

def silver_all_join(spark):
    ### load df
    bronzejellycat = spark.read.format("delta") \
        .load("./spark-warehouse/bronzejellycat")
    bronzejellycat.createOrReplaceTempView("jellycattemp")

    bronzesize = spark.read.format("delta") \
        .load("./spark-warehouse/bronzesize")
    bronzesize.createOrReplaceTempView("sizetemp")

    ### select all data
    all_df = spark.sql("""
        SELECT t1.jellycatid,t1.jellycatname,t1.category,t1.link,t1.imagelink,t1.datecreated as jellycatdatecreated,
        t2.jellycatsizeid,t2.size,t2.height,t2.width,t2.price,t2.stock,t2.datecreated as sizedatecreated, t3.stockcount
        FROM jellycattemp t1
        LEFT JOIN sizetemp t2 on t1.jellycatid=t2.jellycatid
        LEFT JOIN stocktemp t3 on t2.jellycatsizeid=t3.jellycatsizeid
    """
    )
    all_df.createOrReplaceTempView("alltemp")
    all_df.write.format("delta").mode("overwrite").save("./spark-warehouse/all")

    ### out of stock
    df_out_of_stock = spark.sql(
        """
        SELECT * from alltemp
        WHERE LOWER(stock) NOT LIKE LOWER('%In Stock%')
    """
    )
    df_out_of_stock.write.format("delta").mode("overwrite").save("./spark-warehouse/out-of-stock")

    ### join table today with data 3 days ago
    df_3_days_diff = spark.sql(
        """
        SELECT t1.jellycatname,t1.jellycatdatecreated,t1.category,
        t2.jellycatname as t2jellycatname,t2.sizedatecreated,t2.stock as stock3daysago,
        t1.stock as stocktoday,t1.link,t1.imagelink,t1.price,t1.size,t1.height,t1.width
        FROM alltemp t1
        LEFT JOIN (
            SELECT * FROM alltemp
            WHERE DATE(sizedatecreated)=DATEADD(day, -3, DATE(CURRENT_TIMESTAMP))
        ) t2 ON t2.jellycatname=t1.jellycatname AND t2.size=t1.size
        WHERE DATE(t1.jellycatdatecreated) = DATE(CURRENT_TIMESTAMP)
    """
    )
    df_3_days_diff.createOrReplaceTempView("df3daysdiff")

    df_restocked_within_3_days = spark.sql(
        """
        SELECT jellycatname,size,stocktoday,category,stock3daysago,link,imagelink,price,height,width FROM df3daysdiff
        WHERE LOWER(stocktoday) LIKE LOWER('%In Stock%')
        AND LOWER(stock3daysago) NOT LIKE LOWER('%In Stock%')
        AND stock3daysago IS NOT NULL
    """
    )
    df_restocked_within_3_days.write.format("delta").mode("overwrite").save("./spark-warehouse/restocked-within-3-days")
    df_restocked_within_3_days.createOrReplaceTempView("restock")


    df_outofstock_within_3_days = spark.sql(
        """
        SELECT jellycatname,size,stocktoday,category,stock3daysago,link,imagelink,price,height,width FROM df3daysdiff
        WHERE LOWER(stocktoday) NOT LIKE LOWER('%In Stock%')
        AND LOWER(stock3daysago) LIKE LOWER('%In Stock%')
    """
    )
    df_outofstock_within_3_days.write.format("delta").mode("overwrite").save("./spark-warehouse/outofstock-within-3-days")

    
    df_new_in = spark.sql(
        """
        SELECT jellycatname,size,stocktoday,category,stock3daysago,link,imagelink,price,height,width FROM df3daysdiff
        WHERE LOWER(stocktoday) LIKE LOWER('%In Stock%')
        AND stock3daysago IS NULL
    """
    )
    df_new_in.write.format("delta").mode("overwrite").save("./spark-warehouse/new-in")
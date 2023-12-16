# Databricks notebook source
class TradeSummary():

    def __init__(self):
        self.base_dir = '/FileStore/invoices-project'

    def readFromBronzeLayer(self):
        df = spark.readStream.table("trades_bz_table")
        return df
    
    def transformBronzeLayer(self, df):
        from pyspark.sql.functions import expr
        transformed_df = df.withColumn("created_time", expr("to_timestamp(created_time, 'yyyy-MM-dd HH:mm:ss')")) \
                           .withColumn("buy_amount", expr("case when trade_type == 'Buy' then trade_amount else 0 end")) \
                           .withColumn("sell_amount", expr("case when trade_type == 'Sell' then trade_amount else 0 end")) \
                           .drop("trade_type") \
                           .drop("trade_amount")
        return transformed_df
    
    def aggregateBronzeLayer(self, df):
        from pyspark.sql.functions import window, sum
        aggregated_df = df.withWatermark("created_time","30 minutes") \
                          .groupBy(window("created_time","15 minutes","5 minutes")) \
                          .agg(
                              sum("buy_amount").alias("total_buy"),
                              sum("sell_amount").alias("total_sell")
                          )
        final_aggregated_df = aggregated_df.select('window.start','window.end','total_buy','total_sell')
        return final_aggregated_df
    
    def process(self, df):
        sQuery = df.writeStream \
          .queryName("Sliding Window") \
          .option("checkpointLocation", f"{self.base_dir}/checkpoint/trades_sliding_slvr_table") \
          .outputMode("append") \
          .toTable("trades_sliding_slvr_table")
        return sQuery
    
    # The source table needs to be created
    # spark.sql("create table trades_bz_table (trade_date string, broker_code string, created_time string, trade_type string, trade_amount decimal(12,2))")
                        

# COMMAND ----------

trade_summary = TradeSummary()
input_df = trade_summary.readFromBronzeLayer()
transformed_df = trade_summary.transformBronzeLayer(input_df)
aggregated_df = trade_summary.aggregateBronzeLayer(transformed_df)
sQuery = trade_summary.process(aggregated_df)

# COMMAND ----------

sQuery.status

# COMMAND ----------

sData_source = spark.sql("select * from trades_bz_table")

# COMMAND ----------

sData_source.count()

# COMMAND ----------

display(sData_source)

# COMMAND ----------

sData = spark.sql("select * from trades_sliding_slvr_table")

# COMMAND ----------

sData.count()

# COMMAND ----------

display(sData)

# COMMAND ----------

sQuery.stop()

# COMMAND ----------

# Do not run all these inserts together
# These inserts simulate different micro-batches
# So atleast a 30-second gap between these inserts
# Better would be, run the first insert, look at the silver layer table and observe/analyze the data
# Then, run the 2nd insert, observe the silver table data and so on

# def waitForMicroBatch():
#     import time
#     time.sleep(30)

# spark.sql("""INSERT INTO trades_bz_table VALUES
#                   ('2019-02-05', 'ABX', '2019-02-05 10:05:00', 'Buy', 500.00),
#                   ('2019-02-05', 'ACX', '2019-02-05 10:12:00', 'Buy', 300.00)
#             """)

# waitForMicroBatch()

# spark.sql("""INSERT INTO trades_bz_table VALUES
#                   ('2019-02-05', 'ABX', '2019-02-05 10:20:00', 'Buy', 600.00),
#                   ('2019-02-05', 'ACX', '2019-02-05 10:40:00', 'Buy', 900.00)
#             """)

# waitForMicroBatch()

# spark.sql("""INSERT INTO trades_bz_table VALUES
#                     ('2019-02-05', 'ACX', '2019-02-05 10:48:00', 'Sell', 500.00),
#                     ('2019-02-05', 'ABX', '2019-02-05 10:25:00', 'Sell', 400.00)
#             """)

# waitForMicroBatch()

spark.sql("""INSERT INTO trades_bz_table VALUES
                    ('2019-02-05', 'ACX', '2019-02-05 11:23:00', 'Sell', 150.00)
            """)

# COMMAND ----------

# %fs rm -r /FileStore/invoices-project/archive/
# %fs rm -r /FileStore/invoices-project/checkpoint/

# or

# dbutils.fs.rm('dbfs:/FileStore/invoices-project/archive/', True)
# dbutils.fs.rm('dbfs:/FileStore/invoices-project/dataset/', True)

# spark.sql("drop table trades_bz_table")
# spark.sql("drop table trades_tumbling_slvr_table")

dbutils.fs.rm('/FileStore/invoices-project/checkpoint/', True)
dbutils.fs.rm('/user/hive/warehouse/trades_bz_table/', True)
dbutils.fs.rm('/user/hive/warehouse/trades_tumbling_slvr_table/', True)

# COMMAND ----------



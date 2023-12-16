# Databricks notebook source
# We are assuming that this is the Silver layer
# We are assuming that some Bronze layer code has been written to populate the bronze layer table trades_bz_table
# We will be creating the bronze layer table and doing direct inserts into this table as our focus is to understand the windowing concept

# COMMAND ----------

class TradeSummary():

    def __init__(self):
        self.base_dir = "/FileStore/invoices-project"

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
    
    # watermark is 30 minutes
    # so our spark structured streaming based application can handle data that is late by upto 30 minutes
    # if a record comes in 45 minutes late, there is no guarantee that this record will be taken into account for the aggregation calculation
    # note that the column used for water-marking and the column used for grouping / windowing is the same
    def aggregateBronzeLayer(self, df):
        from pyspark.sql.functions import window, sum
        aggregated_df = df.withWatermark("created_time","30 minutes") \
                          .groupBy(window("created_time","15 minutes")) \
                          .agg(
                              sum("buy_amount").alias("total_buy"),
                              sum("sell_amount").alias("total_sell")
                          )
        final_aggregated_df = aggregated_df.select('window.start','window.end','total_buy','total_sell')
        return final_aggregated_df
    
    def process(self, df):
        sQuery = df.writeStream \
          .queryName("Tumbling Window") \
          .option("checkpointLocation", f"{self.base_dir}/checkpoint/trades_tumbling_slvr_table") \
          .outputMode("append") \
          .toTable("trades_tumbling_slvr_table")
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

display(sData_source)

# COMMAND ----------

sData = spark.sql("select * from trades_tumbling_slvr_table")

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
                    ('2019-02-05', 'ACX', '2019-02-05 10:03:00', 'Sell', 150.00)
            """)

# COMMAND ----------

# Note that, when the 3rd insert happenned, there is one "late" record
# The 10:15-10:30 created_time window is already passed.
# Even the 10:30-10:45 created_time window is gone.
# We are in the 10:45-11:00 created_time window.
# Now a record that ideally fits in the 10:15-10:30 created_time window comes in (10:25)
# Based on the watermark of 30 minutes on created_time, spark structured streaming still has the state store of the 10:15-10:30 created_time window and adjusts it
# total_sell is updated as 400
# Now if there is a 4th insert with create_time as 10:05, there is no guarantee that this window will be adjusted

# after 2nd insert
# start	                        end	                            total_buy	total_sell
# 2019-02-05T10:00:00.000+0000	2019-02-05T10:15:00.000+0000	800	        0
# 2019-02-05T10:15:00.000+0000	2019-02-05T10:30:00.000+0000	600	        0
# 2019-02-05T10:30:00.000+0000	2019-02-05T10:45:00.000+0000	900	        0

# after 3rd insert
# start	                        end	                            total_buy	total_sell
# 2019-02-05T10:00:00.000+0000	2019-02-05T10:15:00.000+0000	800	        0
# 2019-02-05T10:15:00.000+0000	2019-02-05T10:30:00.000+0000	600	        400
# 2019-02-05T10:30:00.000+0000	2019-02-05T10:45:00.000+0000	900	        0
# 2019-02-05T10:45:00.000+0000	2019-02-05T11:00:00.000+0000	0	        500


# COMMAND ----------

# %fs rm -r /FileStore/invoices-project/archive/
# %fs rm -r /FileStore/invoices-project/checkpoint/

# or

# dbutils.fs.rm('dbfs:/FileStore/invoices-project/archive/', True)
# dbutils.fs.rm('dbfs:/FileStore/invoices-project/dataset/', True)

# spark.sql("drop table trades_bz_table")
# spark.sql("drop table trades_tumbling_slvr_table")

# dbutils.fs.rm('dbfs:/FileStore/invoices-project/checkpoint/', True)
# dbutils.fs.rm('/user/hive/warehouse/invoices_bz_layer/', True)
# dbutils.fs.rm('/user/hive/warehouse/invoices_slvr_layer_transformed', True)
# dbutils.fs.rm('/user/hive/warehouse/invoices_slvr_layer_aggregated/', True)

# COMMAND ----------



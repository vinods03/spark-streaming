# Databricks notebook source
class AnotherInvoiceTransformer():

    def __init__(self):
        self.base_dir = "/FileStore/invoices-project"

    def readFromBronzeLayer(self):
        df = spark.readStream.table("invoices_bz_layer")
        return df
    
    def writeAggregatedDataIntoSilverLayer(self, df):
        print("Starting to write aggregated data from Bronze layer to Silver layer")
        sQuery = df.writeStream \
                   .queryName("Aggregate Bronze data into Silver layer") \
                   .option("checkpointLocation",f"{self.base_dir}/checkpoint/invoices_slvr_layer_aggregated") \
                   .outputMode("update") \
                   .foreachBatch(self.aggregateAndUpsert) \
                   .start()
        print("Completed writing aggregated data from Bronze layer into Silver layer")
        return sQuery
    
    def aggregateAndUpsert(self, df, batch_id):
        aggregated_df = self.aggregateBronzeData(df)
        aggregated_df.createOrReplaceTempView("incoming_data")
        merge_stmt = """
                        MERGE INTO invoices_slvr_layer_aggregated t
                        USING incoming_data s
                        ON s.CustomerCardNo == t.CustomerCardNo
                        WHEN MATCHED THEN
                        UPDATE set t.total_amount = s.total_amount + t.total_amount, t.total_rewards = s.total_rewards + t.total_rewards
                        WHEN NOT MATCHED THEN
                        INSERT *
                     """
        # spark.sql(merge_stmt)
        df._jdf.sparkSession().sql(merge_stmt)

    def aggregateBronzeData(self, df):
        from pyspark.sql.functions import sum, expr
        aggregated_df = df.groupBy("CustomerCardNo").agg(
            sum("TotalAmount").alias("total_amount"),
            sum(expr("TotalAmount * 0.02")).alias("total_rewards")
        )
        return aggregated_df

    # The main difference between this notebook and the previous notebook is that, in this notebook, aggregation is done as part of writeStream
    # The function used in the writeStream aggregates and then merges data into main table
    # Note that the aggregation function is still there,  but it is invoked in the writeStream function
    # This invokes stateless aggregation
    # In the previous notebook, aggregation is invoked separately 
    # i.e. it is a standalone function still, and it is invoked separately as an action and not as part of writeStream
    # The function used in the writeStream fuynction only merges the already aggregated data into main table
    # Because aggregation as an action is being done separately and not as part of writeStream, Spark invokes the stateful feature and starts to store the state information


# COMMAND ----------

create_tbl_stmt = "create table invoices_slvr_layer_aggregated(CustomerCardNo string, total_amount decimal(10,2), total_rewards decimal(10,2))"
spark.sql(create_tbl_stmt)

# COMMAND ----------

another_invoice_transformer = AnotherInvoiceTransformer()
input_df = another_invoice_transformer.readFromBronzeLayer()
sQuery = another_invoice_transformer.writeAggregatedDataIntoSilverLayer(input_df)

# COMMAND ----------

sQuery.status

# COMMAND ----------

aggregated_data = spark.sql("select * from invoices_slvr_layer_aggregated")

# COMMAND ----------

aggregated_data.count()

# COMMAND ----------

# total amount was 55,570 after 1st micro-batch and became 55,570*2 = 111,140 after the 2nd micro-batch
# here spark did not store any state information because aggregation is being done as part of writeStream
# however, even without having state information, we were able to do the aggregation correctly by making a simple change in the merge statement, where we make use of the value in the target table in addition to the value in the current micro-batch 
display(aggregated_data)

# COMMAND ----------

sQuery.stop()

# COMMAND ----------



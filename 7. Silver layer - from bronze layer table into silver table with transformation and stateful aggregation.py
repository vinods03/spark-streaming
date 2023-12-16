# Databricks notebook source
class InvoiceTransformer():

    def __init__(self):
        self.base_dir = "/FileStore/invoices-project"

    def readFromBronzeLayer(self):
        df = spark.readStream.table("invoices_bz_layer")
        return df
    
    def transformBronzeData(self, df):
        from pyspark.sql.functions import expr
        transformed_1_df = df.selectExpr(
                                       "InvoiceNumber","CreatedTime","StoreID","PosID","CashierID","CustomerType","CustomerCardNo","TotalAmount","PaymentMethod",           
                                       "TaxableAmount","CGST","SGST","CESS","DeliveryType","DeliveryAddress", "explode(InvoiceLineItems) as LineItem"
                                       )
        transformed_2_df = transformed_1_df.withColumn("ItemCode", expr("LineItem.ItemCode")) \
                                           .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
                                           .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
                                           .withColumn("ItemQty", expr("LineItem.ItemQty")) \
                                           .withColumn("TotalValue", expr("LineItem.TotalValue")) \
                                           .withColumn("AddressLine", expr("DeliveryAddress.AddressLine")) \
                                           .withColumn("City", expr("DeliveryAddress.City")) \
                                           .withColumn("State", expr("DeliveryAddress.State")) \
                                           .withColumn("PinCode", expr("DeliveryAddress.PinCode")) \
                                           .withColumn("ContactNumber", expr("DeliveryAddress.ContactNumber")) \
                                           .drop("Lineitem")
        return transformed_2_df
    
    def writeTransformedDataIntoSilverLayer(self, df):
        print("Starting to write transformed data from Bronze layer into Silver layer")
        sQuery_trnsfrm = df.writeStream \
                   .queryName("Transform Bronze data into Silver layer") \
                   .option("checkpointLocation", f"{self.base_dir}/checkpoint/invoices_slvr_layer_transformed") \
                   .outputMode("append") \
                   .toTable("invoices_slvr_layer_transformed")
        print("Completed writing transformed data from Bronze layer into Silver layer")
        return sQuery_trnsfrm
    
    # Aggregation is a stateful transformation
    # spark will automatically store state information because aggregation is being done and hence aggregation will be done across micro-batches, not only for this micro-batch
    # so, you will only code for the current micro-batch and spark, based on the state information it has stored automatically, will take into account all the prior micro-batches as well, for the aggregation

    def aggregateBronzeData(self, df):
        from pyspark.sql.functions import sum, expr
        aggregated_df = df.groupBy("CustomerCardNo").agg(
                            sum("TotalAmount").alias("total_amount"),
                            sum(expr("TotalAmount*0.02")).alias("total_rewards")
                        )
        return aggregated_df
    
    # When working with stateful transformations, we need to be particularly careful about the output mode
    # If the outputMode is "complete", every time the aggregation will happen over the entire record-set and target table will be deleted/overwritten completely
    # This is an issue when there is huge amount of data to process .. like say 10 million transactions per day and there will be performance impact
    
    # If the outputMode is "append", the aggregation will happen at micro-batch level and the records will keep getting added/appended into the target table
    # Suppose for CustomerCardNumber 123, totalamount is 100 in 1st micro-batch and 50 in the 2nd micro-batch, there will be 2 records in the target table for CustomerCardNumber 123 - 1 record with totalamount as 100 and another with total amount as 50. This is not of much use. We should ideally have 1 record in the target table for CustomerCardNumber 123 - with totalamount as 150
    
    # This is what outputMode "update" achieves. Note that a function is invoked for upserting into target table instead of using toTable and because toTable is not there, we should use start() as an action to run the stream

    def writeAggregatedDataIntoSilverLayer(self, df):
        print("Starting to write aggregated data from Bronze layer into Silver layer")
        sQuery_agg = df.writeStream \
                .queryName("Aggregate Bronze data into Silver layer") \
                .option("checkpointLocation",f"{self.base_dir}/checkpoint/invoices_slvr_layer_aggregated") \
                .outputMode("update") \
                .foreachBatch(self.upsert) \
                .start()
        print("Completed writing aggregated data from Bronze layer into Silver layer")
        return sQuery_agg
    

    def upsert(self, df, batch_id):
        df.createOrReplaceTempView("incoming_data")
        merge_stmt = """
                        MERGE INTO invoices_slvr_layer_aggregated t
                        USING incoming_data s
                        ON s.CustomerCardNo == t.CustomerCardNo
                        WHEN MATCHED THEN
                        UPDATE set t.total_amount = s.total_amount, t.total_rewards = s.total_rewards
                        WHEN NOT MATCHED THEN
                        INSERT *
                     """
        # spark.sql(merge_stmt)
        df._jdf.sparkSession().sql(merge_stmt)

    # select(), filter(), map(), flatMap(), explode() are all stateless transformations. 
    # These transformations do not need info of prior micro-batches. 
    # So they do not support "complete" output mode.

    # grouping, aggregation, joins, windowing are all stateful transformation
    # spark automatically stores state information when these functions are invoked, so that you code only for the current micro-batch but spark, with the help of state information extends the transformation to all micro-batches
    # need to keep in mind that too much state info can cause Out Of Memory (OOM) issues

# COMMAND ----------

invoice_transformer = InvoiceTransformer()
input_df = invoice_transformer.readFromBronzeLayer()
transformed_df = invoice_transformer.transformBronzeData(input_df)
sQuery_trnsfrm = invoice_transformer.writeTransformedDataIntoSilverLayer(transformed_df)

# COMMAND ----------

sQuery_trnsfrm.status

# COMMAND ----------

transformed_data = spark.sql("select * from invoices_slvr_layer_transformed")

# COMMAND ----------

transformed_data.count()

# COMMAND ----------

display(transformed_data)

# COMMAND ----------

sQuery_trnsfrm.stop()

# COMMAND ----------

create_tbl_stmt = "create table invoices_slvr_layer_aggregated(CustomerCardNo string, total_amount decimal(10,2), total_rewards decimal(10,2))"
spark.sql(create_tbl_stmt)

# COMMAND ----------

# drop_tbl_stmt = "drop table invoices_slvr_layer_aggregated"
# spark.sql(drop_tbl_stmt)

# COMMAND ----------

aggregated_df = invoice_transformer.aggregateBronzeData(input_df)

# COMMAND ----------

sQuery_agg = invoice_transformer.writeAggregatedDataIntoSilverLayer(aggregated_df)

# COMMAND ----------

sQuery_agg.status

# COMMAND ----------

aggregated_data = spark.sql("select * from invoices_slvr_layer_aggregated")

# COMMAND ----------

aggregated_data.count()

# COMMAND ----------

# total amount was 55,570 after 1st micro-batch and automatically became 55,570*2 = 111,140 after the 2nd micro-batch
# this was possible because spark automatically stored state information
display(aggregated_data)

# COMMAND ----------

sQuery_agg.stop()

# COMMAND ----------



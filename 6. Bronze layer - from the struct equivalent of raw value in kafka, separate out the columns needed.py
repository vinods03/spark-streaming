# Databricks notebook source
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# COMMAND ----------

class InvoiceConsumer():

    def __init__(self):
        self.base_dir = "/FileStore/invoices-project"
        self.BOOTSTRAP_SERVER = "pkc-lzvrd.us-west4.gcp.confluent.cloud:9092"
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = "XYZ"
        self.CLUSTER_API_SECRET = "ABC"
       
    def getSchema(self):
        schema = """
                    InvoiceNumber string, 
                    CreatedTime bigint, 
                    StoreID string, 
                    PosID string, 
                    CashierID string, 
                    CustomerType string, 
                    CustomerCardNo string,
                    TotalAmount double,
                    NumberOfItems bigint,
                    PaymentMethod string,
                    TaxableAmount double,
                    CGST double,
                    SGST double,
                    CESS double,
                    DeliveryType string,
                    DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, State string>,
                    InvoiceLineItems array<struct<ItemCode string, ItemDescription string, ItemPrice double, ItemQty bigint, TotalValue double>>
                """
        return schema

    def readFromKafka(self, startingTime = 1):
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER) \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';") \
            .option("kafka.group.id", "vinod_laptop") \
            .option("subscribe", "invoices") \
            .option("maxOffsetsPerTrigger", 10) \
            .option("startingTimestamp", startingTime) \
            .option("startingOffsetsByTimestampStrategy", "latest") \
            .load()
        return df
    
# startingOffsetsByTimestampStrategy = latest used to avoid errors like java.lang.AssertionError: No offset matched from request of topic-partition invoices-4 and timestamp 1
# The other option for this parameter is error. error is the default option
    
    def transformKafkaData(self, df):
        from pyspark.sql.functions import cast, from_json, expr
        transformed_1_df = df.select(from_json(df.value.cast('string'), self.getSchema()).alias('value'),'topic','timestamp') 
        transformed_2_df = transformed_1_df.withColumn("InvoiceNumber",expr("value.InvoiceNumber")) \
                                           .withColumn("CreatedTime",expr("value.CreatedTime")) \
                                           .withColumn("StoreID", expr("value.StoreID")) \
                                           .withColumn("PosID", expr("value.PosID")) \
                                           .withColumn("CashierID", expr("value.CashierID")) \
                                           .withColumn("CustomerType", expr("value.CustomerType")) \
                                           .withColumn("CustomerCardNo", expr("value.CustomerCardNo")) \
                                           .withColumn("TotalAmount", expr("value.TotalAmount")) \
                                           .withColumn("NumberOfItems", expr("value.NumberOfItems")) \
                                           .withColumn("PaymentMethod", expr("value.PaymentMethod")) \
                                           .withColumn("TaxableAmount", expr("value.TaxableAmount")) \
                                           .withColumn("CGST", expr("value.CGST")) \
                                           .withColumn("SGST", expr("value.SGST")) \
                                           .withColumn("CESS", expr("value.CESS")) \
                                           .withColumn("DeliveryType", expr("value.DeliveryType")) \
                                           .withColumn("DeliveryAddress", expr("value.DeliveryAddress")) \
                                           .withColumn("InvoiceLineItems", expr("value.InvoiceLineItems")) \
                                           .drop("value")
        return transformed_2_df
       
    def process(self, startingTime = 1):
        print("Starting Bronze Layer processing")
        print("Data will be moved from the kafka topic - invoices - into Landing / bronze layer table")
        
        input_df = self.readFromKafka(startingTime)
        transformed_df = self.transformKafkaData(input_df)
        sQuery = transformed_df.writeStream \
                   .queryName("Bronze layer processing for Kafka Source") \
                   .option("checkpointLocation", f"{self.base_dir}/checkpoint/invoices_bz_layer") \
                   .outputMode("append") \
                   .toTable("invoices_bz_layer")   

        print("Bronze layer processing completed successfully")
        print("Data has been moved from kafka topic - invoices - into Landing layer table")  

        return sQuery



# COMMAND ----------

invoiceConsumerFromKafka = InvoiceConsumer()
bzKafkaQuery = invoiceConsumerFromKafka.process()

# COMMAND ----------

bzKafkaQuery.status

# COMMAND ----------

bzData = spark.sql("select * from invoices_bz_layer")

# COMMAND ----------

bzData.count()

# COMMAND ----------

display(bzData)

# COMMAND ----------

bzKafkaQuery.stop()

# COMMAND ----------

# %fs rm -r /FileStore/invoices-project/archive/
# %fs rm -r /FileStore/invoices-project/checkpoint/

# or

# dbutils.fs.rm('dbfs:/FileStore/invoices-project/archive/', True)
# dbutils.fs.rm('dbfs:/FileStore/invoices-project/dataset/', True)

spark.sql("drop table invoices_bz_layer")
spark.sql("drop table invoices_slvr_layer_aggregated")
# spark.sql("drop table invoices_slvr_layer_transformed")

dbutils.fs.rm('dbfs:/FileStore/invoices-project/checkpoint/', True)
dbutils.fs.rm('/user/hive/warehouse/invoices_bz_layer/', True)
dbutils.fs.rm('/user/hive/warehouse/invoices_slvr_layer_aggregated/', True)
# dbutils.fs.rm('/user/hive/warehouse/invoices_slvr_layer_transformed', True)

# COMMAND ----------



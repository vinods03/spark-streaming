# spark-streaming

3. read-from-file-and-write-as-messages-into-kafka
Description: 
Has the python code and data and the steps to be executed, in order to publish messages into a kafka topic.

6. Bronze layer - from the struct equivalent of raw value in kafka, separate out the columns needed
Description:
Read from kafka topic and store the data in a bronze layer table.
Note how the "value" is converted into a string first, then the json string is assigned a schema / converted into a MapType.
Then individual columns are extracted from the MapType object and stored in the bronze layer table.

7. Silver layer - from bronze layer table into silver table with transformation and stateful aggregation
Description:
Read the data from bronze layer table and transform the data (extract individual columns from Struct data types, explode and then extract Array of Structs)
Also, Stateful aggregation is done without any kind of watermarking. 
The State information that has to be maintained is a lot here and is not an advisable approach.

8. Silver layer - from bronze layer table into silver table with transformation and stateless aggregation
Description:
Here we address the problem mentioned in the prior code - i.e. the need to maintain lot of state information, by implementing Stateless Aggregation.
Note that, in the prior code, aggregation is done separately and then the writeStream is invoked.
In this code, aggregation is done at the time of invoking writeStream thereby eliminating the automatic State information storage.
However, in the merge statement, we need to take into account the current value in the target table and then add the new value to it.

9. Tumbling Window
Description: 
Aggregation over a non-overlapping window. 
Here, groupby will be done on a window of a timestamp field and note the usage of withWatermark function.

10. Sliding Window
Description: 
Very similar to the prior code, except that the groupby done on a timestamp field will have an additional parameter.

//Save a DF as avro file 
import com.databricks.spark.avro._;
orders_df.write.avro("/user/cloudera/practice/orders_avro")

//Save a DF as avro with compression codec
import com.databricks.spark.avro._;
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
dataFrameResult.repartition(1).write.avro("/user/cloudera/problem2/products/result-df");

//Save a DF as json file 
orders_df.write.json("/user/cloudera/practice/orders_json")

//Save a DF as parquet file 
orders_df.write.parquet("/user/cloudera/practice/orders_parquet")

//set compression codec
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
ordersDF.write.parquet("/user/cloudera/problem5/parquet-snappy-compress")

//Save a DF as text file -- Single column only
prodsDF.write.text("/user/cloudera/practice/prods_text")

//Save a DF as orc file 
orders_df.write.orc("/user/cloudera/practice/orders_orc")

//Save a DF as csv file -- Since 2.0  
orders_df.write.format("csv").save("/user/cloudera/practice/orders_csv")
orders_df.write.csv("/user/cloudera/practice/orders_csv")

//Save DF to HIVE table
myDF.write.mode("append").option("path", "/loudacre/mydata").saveAsTable("my_table")


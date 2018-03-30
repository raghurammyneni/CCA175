sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--fields-terminated-by "\t" \
--lines-terminated-by "\n" \
--target-dir /user/cloudera/problem5/text \
--as-textfile



sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--target-dir /user/cloudera/problem5/avro \
--as-avrodatafile 



sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--target-dir /user/cloudera/problem5/parquet \
--as-parquetfile


import com.databricks.spark.avro._;
val ordersDF = sqlContext.read.avro("/user/cloudera/problem5/avro")
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
ordersDF.write.parquet("/user/cloudera/problem5/parquet-snappy-compress")


ordersDF.map(x=> x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3)).saveAsTextFile("/user/cloudera/problem5/text-gzip-compress", classOf[org.apache.hadoop.io.compress.GzipCodec]);


ordersDF.map(x => (x(0).toString, x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3))).take(5).foreach(println)
ordersDF.map(x => (x(0).toString, x(0)+"\t"+x(1)+"\t"+x(2)+"\t"+x(3))).saveAsSequenceFile("/user/cloudera/problem5/sequence")


sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--compress \
--compression-codec snappy \
--as-textfile \
--target-dir /user/cloudera/problem5/text-snappy-compress



val ordersDF = sqlContext.read.parquet("/user/cloudera/problem5/parquet-snappy-compress");
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed");
ordersDF.write.parquet("/user/cloudera/problem5/parquet-no-compress");

sqlContext.setConf("spark.sql.avro.compression.codec", "snappy");
ordersDF.write.avro("/user/cloudera/problem5/avro-snappy");




val avroDF = sqlContext.read.avro("/user/cloudera/problem5/avro-snappy")
avroDF.toJSON.saveAsTextFile("/user/cloudera/problem5/json-no-compress")
avroDF.toJSON.saveAsTextFile("/user/cloudera/problem5/json-gzip", classOf[org.apache.hadoop.io.compress.GzipCodec])



val jsonDF = sqlContext.read.json("/user/cloudera/problem5/json-gzip");
jsonDF.map(x => x(0) +","+ x(1) +","+ x(2) +","+ x(3)).saveAsTextFile("/user/cloudera/problem5/csv-gzip", classOf[org.apache.hadoop.io.compress.GzipCodec])


hdfs dfs -get /user/cloudera/problem5/sequence/part-00000
cut -c -300 part-00000


val seqData = sc.sequenceFile("/user/cloudera/problem5/sequence", classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text]);
seqData.map(x=>{var d = x._2.toString.split("\t"); (d(0),d(1),d(2),d(3))}).toDF().write.orc("/user/cloudera/problem5/orc");










































sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--as-avrodatafile \
--target-dir /user/cloudera/problem1/orders \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec



sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table order_items \
--as-avrodatafile \
--target-dir /user/cloudera/problem1/order_items \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec


/*************************************************************************************************************************************/

import com.databricks.spark.avro._
val ordersDF = sqlContext.read.avro("/user/cloudera/problem1/orders")

ordersDF = sqlContext.read.format("com.databricks.spark.avro").load("/user/cloudera/problem1/orders") //pyspark


ordersDF.show(5)
ordersDF.take(5)

ordersDF.schema
ordersDF.printSchema
ordersDF.dtypes.foreach(println)

/*
scala> ordersDF.schema
res5: org.apache.spark.sql.types.StructType = StructType(StructField(order_id,IntegerType,true), StructField(order_date,LongType,true), StructField(order_customer_id,IntegerType,true), StructField(order_status,StringType,true))

scala> ordersDF.printSchema
root
 |-- order_id: integer (nullable = true)
 |-- order_date: long (nullable = true)
 |-- order_customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)

scala> ordersDF.dtypes.foreach(println)
(order_id,IntegerType)
(order_date,LongType)
(order_customer_id,IntegerType)
(order_status,StringType)
*/

val orderItemsDF = sqlContext.read.avro("/user/cloudera/problem1/order_items")

orderItemsDF = sqlContext.read.format("com.databricks.spark.avro").load("/user/cloudera/problem1/order_items") //pyspark

val joinedDF = ordersDF.join(orderItemsDF, ordersDF("order_id") === orderItemsDF("order_item_order_id"))

joinedDF = ordersDF.join(orderItemsDF, ordersDF["order_id"] == orderItemsDF["order_item_order_id"]) //pyspark

/*
scala> val joinedDF = ordersDF.join(orderItemsDF, ordersDF("order_id") === orderItemsDF("order_item_order_id"))
joinedDF: org.apache.spark.sql.DataFrame = [order_id: int, order_date: bigint, order_customer_id: int, order_status: string, order_item_id: int, order_item_order_id: int, order_item_product_id: int, order_item_quantity: int, order_item_subtotal: float, order_item_product_price: float]

scala> joinedDF.printSchema
root
 |-- order_id: integer (nullable = true)
 |-- order_date: long (nullable = true)
 |-- order_customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)
 |-- order_item_id: integer (nullable = true)
 |-- order_item_order_id: integer (nullable = true)
 |-- order_item_product_id: integer (nullable = true)
 |-- order_item_quantity: integer (nullable = true)
 |-- order_item_subtotal: float (nullable = true)
 |-- order_item_product_price: float (nullable = true)
*/


joinedDF.groupBy("order_date", "order_status").agg(countDistinct(col("order_id")), sum(col("order_item_subtotal"))).show(5)

joinedDF.groupBy("order_date", "order_status").agg({"order_id":"count", "order_item_subtotal":"sum"}).show(5) //pyspark

/*
scala> joinedDF.groupBy("order_date", "order_status").agg(countDistinct(col("order_id")), sum(col("order_item_subtotal"))).show(5)
[Stage 9:=====================================================> (194 + 1) / 200]18/01/15 10:08:13 WARN memory.TaskMemoryManager: leak 16.3 MB memory from org.apache.spark.unsafe.map.BytesToBytesMap@336358b6
18/01/15 10:08:13 ERROR executor.Executor: Managed memory leak detected; size = 17039360 bytes, TID = 224
+-------------+------------+---------------+------------------------+           
|   order_date|order_status|count(order_id)|sum(order_item_subtotal)|
+-------------+------------+---------------+------------------------+
|1382598000000|     PENDING|              8|       5429.340110778809|
|1395990000000|    COMPLETE|             42|      25668.330516815186|
|1391241600000|  PROCESSING|             33|       18813.75033569336|
|1398927600000|    COMPLETE|             47|      30957.470586776733|
|1388304000000|  PROCESSING|             11|       7238.240139007568|
+-------------+------------+---------------+------------------------+
only showing top 5 rows
*/

/*************************************************************************************************************************************/



val dataframeResult = joinedDF.groupBy(to_date(from_unixtime(col("order_date")/1000)).alias("order_date"), col("order_status")).agg(countDistinct(col("order_id")).alias("orders_count"), round(sum(col("order_item_subtotal")), 2).alias("total_amount")).orderBy(col("order_date").desc, col("order_status"), col("total_amount").desc, col("orders_count"))



/*
scala> val dataframeResult = joinedDF.groupBy(to_date(from_unixtime(col("order_date")/1000)).alias("order_date"), col("order_status")).agg(countDistinct(col("order_id")).alias("orders_count"), round(sum(col("order_item_subtotal")), 2).alias("total_amount")).orderBy(col("order_date").desc, col("order_status"), col("total_amount").desc, col("orders_count"))
dataframeResult: org.apache.spark.sql.DataFrame = [order_date: date, order_status: string, orders_count: bigint, total_amount: double]

scala> dataframeResult.show()
+----------+---------------+------------+------------+                          
|order_date|   order_status|orders_count|total_amount|
+----------+---------------+------------+------------+
|2014-07-24|       CANCELED|           2|     1254.92|
|2014-07-24|         CLOSED|          26|    16333.16|
|2014-07-24|       COMPLETE|          55|    34552.03|
|2014-07-24|        ON_HOLD|           4|     1709.74|
|2014-07-24| PAYMENT_REVIEW|           1|      499.95|
|2014-07-24|        PENDING|          22|    12729.49|
|2014-07-24|PENDING_PAYMENT|          34|     17680.7|
|2014-07-24|     PROCESSING|          17|     9964.74|
|2014-07-24|SUSPECTED_FRAUD|           4|     2351.61|
|2014-07-23|       CANCELED|          10|     5777.33|
|2014-07-23|         CLOSED|          18|    13312.72|
|2014-07-23|       COMPLETE|          40|    25482.51|
|2014-07-23|        ON_HOLD|           6|     4514.46|
|2014-07-23| PAYMENT_REVIEW|           2|     1699.82|
|2014-07-23|        PENDING|          11|     6161.37|
|2014-07-23|PENDING_PAYMENT|          30|    19279.81|
|2014-07-23|     PROCESSING|          15|     7962.79|
|2014-07-23|SUSPECTED_FRAUD|           6|     3799.57|
|2014-07-22|       CANCELED|           4|     3209.73|
|2014-07-22|         CLOSED|          20|    12688.79|
+----------+---------------+------------+------------+
only showing top 20 rows
*/

/*************************************************************************************************************************************/


joinedDF.registerTempTable("ordersanditems")

val sqlResult = sqlContext.sql("SELECT to_date(from_unixtime(order_date/1000)) as order_date, order_status, count(distinct order_id) as orders_count, cast(sum(order_item_subtotal) as decimal(10,2)) as total_amount FROM ordersanditems group by order_date, order_status order by order_date desc, order_status, total_amount desc, orders_count")

/*
scala> val sqlResult = sqlContext.sql("SELECT to_date(from_unixtime(order_date/1000)) as order_date, order_status, count(distinct order_id) as orders_count, cast(sum(order_item_subtotal) as decimal(10,2)) as total_amount FROM ordersanditems group by order_date, order_status order by order_date desc, order_status, total_amount desc, orders_count")
sqlResult: org.apache.spark.sql.DataFrame = [order_date: date, order_status: string, orders_count: bigint, total_amount: decimal(10,2)]

scala> sqlResult.show()
+----------+---------------+------------+------------+                          
|order_date|   order_status|orders_count|total_amount|
+----------+---------------+------------+------------+
|2014-07-24|       CANCELED|           2|     1254.92|
|2014-07-24|         CLOSED|          26|    16333.16|
|2014-07-24|       COMPLETE|          55|    34552.03|
|2014-07-24|        ON_HOLD|           4|     1709.74|
|2014-07-24| PAYMENT_REVIEW|           1|      499.95|
|2014-07-24|        PENDING|          22|    12729.49|
|2014-07-24|PENDING_PAYMENT|          34|    17680.70|
|2014-07-24|     PROCESSING|          17|     9964.74|
|2014-07-24|SUSPECTED_FRAUD|           4|     2351.61|
|2014-07-23|       CANCELED|          10|     5777.33|
|2014-07-23|         CLOSED|          18|    13312.72|
|2014-07-23|       COMPLETE|          40|    25482.51|
|2014-07-23|        ON_HOLD|           6|     4514.46|
|2014-07-23| PAYMENT_REVIEW|           2|     1699.82|
|2014-07-23|        PENDING|          11|     6161.37|
|2014-07-23|PENDING_PAYMENT|          30|    19279.81|
|2014-07-23|     PROCESSING|          15|     7962.79|
|2014-07-23|SUSPECTED_FRAUD|           6|     3799.57|
|2014-07-22|       CANCELED|           4|     3209.73|
|2014-07-22|         CLOSED|          20|    12688.79|
+----------+---------------+------------+------------+
only showing top 20 rows
*/


/*************************************************************************************************************************************/

val rddResult = joinedDF.map(x => ((x(1).toString, x(3).toString), (x(0).toString.toInt, x(8).toString.toDouble))).combineByKey((x:(Int, Double)) => (Set(x._1), x._2), (x:(Set[Int], Double), y:(Int, Double)) => (x._1 + y._1, x._2 + y._2), (x:(Set[Int], Double), y:(Set[Int], Double)) => (x._1 ++ y._1, x._2 + y._2)).map(x => (x._1._1, x._1._2, x._2._1.size, x._2._2)).toDF.orderBy(col("_1").desc, col("_2"), col("_4").desc, col("_3"))

/*
scala> val rddResult = joinedDF.map(x => ((x(1).toString, x(3).toString), (x(0).toString.toInt, x(8).toString.toDouble))).combineByKey((x:(Int, Double)) => (Set(x._1), x._2), (x:(Set[Int], Double), y:(Int, Double)) => (x._1 + y._1, x._2 + y._2), (x:(Set[Int], Double), y:(Set[Int], Double)) => (x._1 ++ y._1, x._2 + y._2)).map(x => (x._1._1, x._1._2, x._2._1.size, x._2._2)).toDF.orderBy(col("_1").desc, col("_2"), col("_4").desc, col("_3"))
rddResult: org.apache.spark.sql.DataFrame = [_1: string, _2: string, _3: int, _4: double]

scala> rddResult.show()
+-------------+---------------+---+------------------+                          
|           _1|             _2| _3|                _4|
+-------------+---------------+---+------------------+
|1406185200000|       CANCELED|  2|           1254.92|
|1406185200000|         CLOSED| 26|16333.159999999983|
|1406185200000|       COMPLETE| 55| 34552.03000000002|
|1406185200000|        ON_HOLD|  4|1709.7400000000002|
|1406185200000| PAYMENT_REVIEW|  1|            499.95|
|1406185200000|        PENDING| 22|12729.489999999994|
|1406185200000|PENDING_PAYMENT| 34| 17680.69999999999|
|1406185200000|     PROCESSING| 17| 9964.739999999994|
|1406185200000|SUSPECTED_FRAUD|  4|           2351.61|
|1406098800000|       CANCELED| 10| 5777.329999999999|
|1406098800000|         CLOSED| 18|13312.719999999996|
|1406098800000|       COMPLETE| 40|          25482.51|
|1406098800000|        ON_HOLD|  6| 4514.459999999999|
|1406098800000| PAYMENT_REVIEW|  2|1699.8200000000002|
|1406098800000|        PENDING| 11|           6161.37|
|1406098800000|PENDING_PAYMENT| 30|19279.809999999998|
|1406098800000|     PROCESSING| 15| 7962.789999999998|
|1406098800000|SUSPECTED_FRAUD|  6|3799.5700000000006|
|1406012400000|       CANCELED|  4|           3209.73|
|1406012400000|         CLOSED| 20| 12688.78999999999|
+-------------+---------------+---+------------------+
only showing top 20 rows
*/

/*************************************************************************************************************************************/


sqlContext.setConf("spark.sql.parquet.compression.codec","gzip");
dataframeResult.write.parquet("/user/cloudera/problem1/result4a-gzip")

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");
sqlResult.write.parquet("/user/cloudera/problem1/result4b-snappy")

rddResult.map(x => x(0) +","+ x(1) +","+ x(2) +","+ x(3)).saveAsTextFile("/user/cloudera/problem1/result4c-csv")


/*************************************************************************************************************************************/

mysql -h localhost -u retail_dba -p

 create table retail_db.result(order_date varchar(255) not null,order_status varchar(255) not null, total_orders int, total_amount numeric, constraint pk_order_result primary key (order_date,order_status)); 


sqoop export \
--connect "jdbc:mysql://localhost/retail_db" \
--username retail_dba \
--password cloudera \
--table result \
--export-dir "/user/cloudera/problem1/result4c-csv"



sqoop import 
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table products \
--target-dir /user/cloudera/products \
--fields-terminated-by '|'



hdfs dfs -rm -r /user/cloudera/products
hdfs dfs -rm -r /user/cloudera/problem2
hdfs dfs -ls /user/cloudera
hdfs dfs -mkdir /user/cloudera/problem2
hdfs dfs -mkdir /user/cloudera/problem2/products
hdfs dfs -mv /user/cloudera/products/* /user/cloudera/problem2/products

/**/

val products = sc.textFile("/user/cloudera/problem2/products").map(line => line.split('|')).map(d => (d(0).toInt, d(1).toInt, d(2).toString, d(3).toString, d(4).toFloat, d(5).toString))

case class Product(productID:Integer, productCatID:Integer, productName:String, productDesc:String, productPrice:Float, productImage:String);

val productsDF = products.map(x => Product(x._1, x._2, x._3, x._4, x._5, x._6)).toDF();
productsDF.printSchema()


/*
scala> val productsDF = products.map(x => Product(x._1, x._2, x._3, x._4, x._5, x._6)).toDF();
productsDF: org.apache.spark.sql.DataFrame = [productID: int, productCatID: int, productName: string, productDesc: string, productPrice: float, productImage: string]

scala> productsDF.printSchema()
root
 |-- productID: integer (nullable = true)
 |-- productCatID: integer (nullable = true)
 |-- productName: string (nullable = true)
 |-- productDesc: string (nullable = true)
 |-- productPrice: float (nullable = false)
 |-- productImage: string (nullable = true)

*/


val filteredDF = productsDF.where(productsDF("productPrice") < 100)
val filteredDF = productsDF.filter("productPrice < 100")




val dfResult = productsDF.filter("productPrice < 100").groupBy(col("productCatID").alias("prod_category")).agg(max(col("productPrice")).alias("max_price"), countDistinct(col("productID")).alias("total_products"), round(avg(col("productPrice")), 2).alias("avg_price"), min(col("productPrice")).alias("min_price")).orderBy(col("productCatID"))


/*
scala> dfResult.show()
+-------------+---------+--------------+---------+---------+                    
|prod_category|max_price|total_products|avg_price|min_price|
+-------------+---------+--------------+---------+---------+
|            2|    99.99|            11|    66.81|    29.97|
|            3|     99.0|            19|    55.73|      0.0|
|            4|    99.95|            10|    55.89|    21.99|
|            5|    99.99|            13|    57.99|     14.0|
|            6|    99.99|            19|    43.94|     14.0|
|            7|    99.98|            18|    47.49|     14.0|
|            8|    99.98|            19|    41.67|    21.99|
|            9|    99.99|            17|    67.17|     28.0|
|           10|    99.95|             4|    78.48|    34.99|
|           11|    99.99|             5|    76.99|    34.99|
|           12|    99.99|            21|     58.7|    16.99|
|           13|    99.99|            23|    42.25|    21.99|
|           15|    81.99|            23|    39.95|      5.0|
|           16|    99.99|            13|    55.76|    27.99|
|           17|    59.99|            12|    35.82|    19.99|
|           18|    89.99|            10|    58.49|      0.0|
|           19|    99.99|            11|     79.9|      0.0|
|           20|    99.99|            17|    81.22|     22.0|
|           21|    99.99|            23|    59.38|      8.0|
|           22|    99.99|            20|    69.29|    21.99|
+-------------+---------+--------------+---------+---------+
only showing top 20 rows
*/





productsDF.registerTempTable("products")

val sqlResult = sqlContext.sql("SELECT productCatID as prod_category, MAX(productPrice) as max_price, COUNT(DISTINCT(productID)) as total_products, CAST(AVG(productPrice) as decimal(10,2)) as avg_price, MIN(productPrice) as min_price FROM products WHERE productPrice < 100 GROUP BY (productCatID) ORDER BY (productCatID)")



val rddResult = products.map(x => (x._2, x._5)).filter(x => x._2 < 100).aggregateByKey((0.0, 0, 0.0, 9999999999999999.0))((x, y) => (math.max(x._1, y), x._2 + 1, x._3 + y, math.min(x._4, y)), ((x, y) => (math.max(x._1, y._1), x._2 + y._2, x._3 + y._3, math.min(x._4, y._4)))).sortBy(x=> x._1).map(x => (x._1, x._2._1, x._2._2, (x._2._3 / x._2._2), x._2._4))







import com.databricks.spark.avro._;
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")
dataFrameResult.repartition(1).write.avro("/user/cloudera/problem2/products/result-df");




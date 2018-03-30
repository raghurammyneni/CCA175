scala> import com.databricks.spark.avro._
import com.databricks.spark.avro._



scala> val categoriesDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/categories")
categoriesDF: org.apache.spark.sql.DataFrame = [category_id: int, category_department_id: int, category_name: string]

scala> categoriesDF.printSchema
root
 |-- category_id: integer (nullable = true)
 |-- category_department_id: integer (nullable = true)
 |-- category_name: string (nullable = true)



scala> val productsDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/products")
productsDF: org.apache.spark.sql.DataFrame = [product_id: int, product_category_id: int, product_name: string, product_description: string, product_price: float, product_image: string]

scala> productsDF.printSchema
root
 |-- product_id: integer (nullable = true)
 |-- product_category_id: integer (nullable = true)
 |-- product_name: string (nullable = true)
 |-- product_description: string (nullable = true)
 |-- product_price: float (nullable = true)
 |-- product_image: string (nullable = true)



scala> val orderItemsDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/order_items")
orderItemsDF: org.apache.spark.sql.DataFrame = [order_item_id: int, order_item_order_id: int, order_item_product_id: int, order_item_quantity: int, order_item_subtotal: float, order_item_product_price: float]

scala> orderItemsDF.printSchema
root
 |-- order_item_id: integer (nullable = true)
 |-- order_item_order_id: integer (nullable = true)
 |-- order_item_product_id: integer (nullable = true)
 |-- order_item_quantity: integer (nullable = true)
 |-- order_item_subtotal: float (nullable = true)
 |-- order_item_product_price: float (nullable = true)



scala> categoriesDF.show(5)
+-----------+----------------------+-------------------+
|category_id|category_department_id|      category_name|
+-----------+----------------------+-------------------+
|          1|                     2|           Football|
|          2|                     2|             Soccer|
|          3|                     2|Baseball & Softball|
|          4|                     2|         Basketball|
|          5|                     2|           Lacrosse|
+-----------+----------------------+-------------------+
only showing top 5 rows


scala> productsDF.show(5)
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|product_id|product_category_id|        product_name|product_description|product_price|       product_image|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
|         1|                  2|Quest Q64 10 FT. ...|                   |        59.98|http://images.acm...|
|         2|                  2|Under Armour Men'...|                   |       129.99|http://images.acm...|
|         3|                  2|Under Armour Men'...|                   |        89.99|http://images.acm...|
|         4|                  2|Under Armour Men'...|                   |        89.99|http://images.acm...|
|         5|                  2|Riddell Youth Rev...|                   |       199.99|http://images.acm...|
+----------+-------------------+--------------------+-------------------+-------------+--------------------+
only showing top 5 rows



scala> orderItemsDF.show(5)
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|            1|                  1|                  957|                  1|             299.98|                  299.98|
|            2|                  2|                 1073|                  1|             199.99|                  199.99|
|            3|                  2|                  502|                  5|              250.0|                    50.0|
|            4|                  2|                  403|                  1|             129.99|                  129.99|
|            5|                  4|                  897|                  2|              49.98|                   24.99|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 5 rows



scala> categoriesDF.select(categoriesDF("category_name")).show(5)
+-------------------+
|      category_name|
+-------------------+
|           Football|
|             Soccer|
|Baseball & Softball|
|         Basketball|
|           Lacrosse|
+-------------------+
only showing top 5 rows



scala> categoriesDF.join(productsDF, categoriesDF("category_id") === productsDF("product_category_id"), "inner").join(orderItemsDF, productsDF("product_id") === orderItemsDF("order_item_product_id"), "inner").groupBy(categoriesDF("category_name")).agg(count(orderItemsDF("order_item_id")).alias("total_orders")).orderBy(col("total_orders").desc).show(10)
+--------------------+------------+                                             
|       category_name|total_orders|
+--------------------+------------+
|              Cleats|       24551|
|      Men's Footwear|       22246|
|     Women's Apparel|       21035|
|Indoor/Outdoor Games|       19298|
|             Fishing|       17325|
|        Water Sports|       15540|
|    Camping & Hiking|       13729|
|    Cardio Equipment|       12487|
|       Shop By Sport|       10984|
|         Electronics|        3156|
+--------------------+------------+
only showing top 10 rows





impala-shell
invalidate metadata;
show databases;
use retail_stage_avro;
show tables;


select c.category_name, count(order_item_id) as count
from order_items oi, products p, categories c
where oi.order_item_product_id = p.product_id
and c.category_id = p.product_category_id
group by c.category_name
order by count desc
limit 10;

Query submitted at: 2018-03-20 10:06:09 (Coordinator: http://quickstart.cloudera:25000)
Query progress can be monitored at: http://quickstart.cloudera:25000/query_plan?query_id=294729312e69d96f:1059540400000000
+----------------------+-------+
| category_name        | count |
+----------------------+-------+
| Cleats               | 24551 |
| Men's Footwear       | 22246 |
| Women's Apparel      | 21035 |
| Indoor/Outdoor Games | 19298 |
| Fishing              | 17325 |
| Water Sports         | 15540 |
| Camping & Hiking     | 13729 |
| Cardio Equipment     | 12487 |
| Shop By Sport        | 10984 |
| Electronics          | 3156  |
+----------------------+-------+
Fetched 10 row(s) in 0.92s



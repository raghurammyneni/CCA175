scala> import com.databricks.spark.avro._
import com.databricks.spark.avro._

scala> val productsDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/products");
productsDF: org.apache.spark.sql.DataFrame = [product_id: int, product_category_id: int, product_name: string, product_description: string, product_price: float, product_image: string]

scala> val orderItemsDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/order_items");
orderItemsDF: org.apache.spark.sql.DataFrame = [order_item_id: int, order_item_order_id: int, order_item_product_id: int, order_item_quantity: int, order_item_subtotal: float, order_item_product_price: float]

scala> val ordersDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/orders");
ordersDF: org.apache.spark.sql.DataFrame = [order_id: int, order_date: bigint, order_customer_id: int, order_status: string]


scala> ordersDF.select(col("order_status")).distinct.show()
+---------------+                                                               
|   order_status|
+---------------+
|        PENDING|
|        ON_HOLD|
| PAYMENT_REVIEW|
|PENDING_PAYMENT|
|     PROCESSING|
|         CLOSED|
|       COMPLETE|
|       CANCELED|
|SUSPECTED_FRAUD|
+---------------+

scala> productsDF.join(orderItemsDF, productsDF("product_id") === orderItemsDF("order_item_product_id")).join(ordersDF, orderItemsDF("order_item_order_id") === ordersDF("order_id")).where("order_status <> 'CANCELED' AND order_status <> 'SUSPECTED_FRAUD'").groupBy(productsDF("product_id"), productsDF("product_name")).agg(sum(orderItemsDF("order_item_subtotal")).alias("total_revenue")).orderBy(col("total_revenue").desc).show(10)
+----------+--------------------+------------------+                            
|product_id|        product_name|     total_revenue|
+----------+--------------------+------------------+
|      1004|Field & Stream Sp...| 6637668.282318115|
|       365|Perfect Fitness P...|4233794.3682899475|
|       957|Diamondback Women...| 3946837.004547119|
|       191|Nike Men's Free 5...|3507549.2067337036|
|       502|Nike Men's Dri-FI...|         3011600.0|
|      1073|Pelican Sunstream...|2967851.6815185547|
|      1014|O'Brien Men's Neo...| 2765543.314743042|
|       403|Nike Men's CJ Eli...|2763977.4868011475|
|       627|Under Armour Girl...| 1214896.220287323|
|       565|adidas Youth Germ...|           63490.0|
+----------+--------------------+------------------+
only showing top 10 rows





[quickstart.cloudera:21000] > select p.product_id, p.product_name, r.revenue
                            > from products p inner join
                            > (select oi.order_item_product_id, sum(cast(oi.order_item_subtotal as float)) as revenue
                            > from order_items oi inner join orders o
                            > on oi.order_item_order_id = o.order_id
                            > where o.order_status <> 'CANCELED'
                            > and o.order_status <> 'SUSPECTED_FRAUD'
                            > group by order_item_product_id) r
                            > on p.product_id = r.order_item_product_id
                            > order by r.revenue desc
                            > limit 10;
Query: select p.product_id, p.product_name, r.revenue
from products p inner join
(select oi.order_item_product_id, sum(cast(oi.order_item_subtotal as float)) as revenue
from order_items oi inner join orders o
on oi.order_item_order_id = o.order_id
where o.order_status <> 'CANCELED'
and o.order_status <> 'SUSPECTED_FRAUD'
group by order_item_product_id) r
on p.product_id = r.order_item_product_id
order by r.revenue desc
limit 10
Query submitted at: 2018-03-20 11:05:08 (Coordinator: http://quickstart.cloudera:25000)
Query progress can be monitored at: http://quickstart.cloudera:25000/query_plan?query_id=2a45441232a9b176:6ef910400000000
+------------+-----------------------------------------------+-------------------+
| product_id | product_name                                  | revenue           |
+------------+-----------------------------------------------+-------------------+
| 1004       | Field & Stream Sportsman 16 Gun Fire Safe     | 6637668.282318115 |
| 365        | Perfect Fitness Perfect Rip Deck              | 4233794.368289948 |
| 957        | Diamondback Women's Serene Classic Comfort Bi | 3946837.004547119 |
| 191        | Nike Men's Free 5.0+ Running Shoe             | 3507549.206733704 |
| 502        | Nike Men's Dri-FIT Victory Golf Polo          | 3011600           |
| 1073       | Pelican Sunstream 100 Kayak                   | 2967851.681518555 |
| 1014       | O'Brien Men's Neoprene Life Vest              | 2765543.314743042 |
| 403        | Nike Men's CJ Elite 2 TD Football Cleat       | 2763977.486801147 |
| 627        | Under Armour Girls' Toddler Spine Surge Runni | 1214896.220287323 |
| 565        | adidas Youth Germany Black/Red Away Match Soc | 63490             |
+------------+-----------------------------------------------+-------------------+
Fetched 10 row(s) in 5.62s

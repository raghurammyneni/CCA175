scala> import com.databricks.spark.avro._
import com.databricks.spark.avro._

scala> val productsDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/products");
productsDF: org.apache.spark.sql.DataFrame = [product_id: int, product_category_id: int, product_name: string, product_description: string, product_price: float, product_image: string]

scala> val orderItemsDF = sqlContext.read.avro("/user/hive/warehouse/retail_stage_avro.db/order_items");
orderItemsDF: org.apache.spark.sql.DataFrame = [order_item_id: int, order_item_order_id: int, order_item_product_id: int, order_item_quantity: int, order_item_subtotal: float, order_item_product_price: float]


scala> val resultDF = productsDF.join(orderItemsDF, productsDF("product_id") === orderItemsDF("order_item_product_id")).select(productsDF("product_name"), orderItemsDF("order_item_order_id"));
resultDF: org.apache.spark.sql.DataFrame = [product_name: string, order_item_order_id: int]



scala> resultDF.where("order_item_order_id = 2").show()
+--------------------+-------------------+
|        product_name|order_item_order_id|
+--------------------+-------------------+
|Nike Men's CJ Eli...|                  2|
|Nike Men's Dri-FI...|                  2|
|Pelican Sunstream...|                  2|
+--------------------+-------------------+


scala> val resultRdd = resultDF.map(x => x.toString()).map(line => line.replaceAll("\\[", "").replaceAll("\\]", "").split(","));
resultRdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[373] at map at <console>:34


scala> resultRdd.take(5)
res49: Array[Array[String]] = Array(Array(Nike Men's Fingertrap Max Training Shoe, 57864), Array(Nike Men's Fingertrap Max Training Shoe, 58018), Array(Nike Men's Fingertrap Max Training Shoe, 58491), Array(Nike Men's Fingertrap Max Training Shoe, 58522), Array(Nike Men's Fingertrap Max Training Shoe, 58707))


scala> resultRdd.map(x => (x(1).toInt, x(0)))
res50: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[374] at map at <console>:37

scala> resultRdd.map(x => (x(1).toInt, x(0))).take(5)
res51: Array[(Int, String)] = Array((57864,Nike Men's Fingertrap Max Training Shoe), (58018,Nike Men's Fingertrap Max Training Shoe), (58491,Nike Men's Fingertrap Max Training Shoe), (58522,Nike Men's Fingertrap Max Training Shoe), (58707,Nike Men's Fingertrap Max Training Shoe))


scala> resultRdd.map(x => (x(1).toInt, x(0))).combineByKey((x) => (Set(x)), (x:Set[String], y) => (x + y), (x:Set[String], y:Set[String]) => (x ++ y)).take(5)
res55: Array[(Int, scala.collection.immutable.Set[String])] = Array((57436,Set(Nike Men's Dri-FIT Victory Golf Polo, Glove It Women's Mod Oval 3-Zip Carry All Gol, Field & Stream Sportsman 16 Gun Fire Safe, O'Brien Men's Neoprene Life Vest)), (18624,Set(Pelican Sunstream 100 Kayak)), (54040,Set(Nike Men's Free 5.0+ Running Shoe, Diamondback Women's Serene Classic Comfort Bi, O'Brien Men's Neoprene Life Vest)), (7608,Set(Under Armour Girls' Toddler Spine Surge Runni)), (18500,Set(Nike Men's Dri-FIT Victory Golf Polo, Field & Stream Sportsman 16 Gun Fire Safe)))


scala> resultRdd.map(x => (x(1).toInt, x(0))).combineByKey((x) => (Set(x)), (x:Set[String], y) => (x + y), (x:Set[String], y:Set[String]) => (x ++ y)).map{case (x:Int, y:Set[String]) => (x, y.toList)}.flatMapValues(x => x.combinations(2)).map{case (x:Int, y:List[String]) => (y, 1)}.take(5)

res58: Array[(List[String], Int)] = Array((List(Nike Men's Dri-FIT Victory Golf Polo, Glove It Women's Mod Oval 3-Zip Carry All Gol),1), (List(Nike Men's Dri-FIT Victory Golf Polo, Field & Stream Sportsman 16 Gun Fire Safe),1), (List(Nike Men's Dri-FIT Victory Golf Polo, O'Brien Men's Neoprene Life Vest),1), (List(Glove It Women's Mod Oval 3-Zip Carry All Gol, Field & Stream Sportsman 16 Gun Fire Safe),1), (List(Glove It Women's Mod Oval 3-Zip Carry All Gol, O'Brien Men's Neoprene Life Vest),1))


scala> resultRdd.map(x => (x(1).toInt, x(0))).combineByKey((x) => (Set(x)), (x:Set[String], y) => (x + y), (x:Set[String], y:Set[String]) => (x ++ y)).map{case (x:Int, y:Set[String]) => (x, y.toList)}.flatMapValues(x => x.combinations(2)).map{case (x:Int, y:List[String]) => (y, 1)}.reduceByKey((x,y) => x+y).map{case (x:List[String], y:Int) => (y, x)}.sortByKey(false).take(10).foreach(println)

(6169,List(Perfect Fitness Perfect Rip Deck, Nike Men's CJ Elite 2 TD Football Cleat))
(5556,List(Perfect Fitness Perfect Rip Deck, O'Brien Men's Neoprene Life Vest))
(4991,List(Nike Men's CJ Elite 2 TD Football Cleat, O'Brien Men's Neoprene Life Vest))
(4984,List(Perfect Fitness Perfect Rip Deck, Field & Stream Sportsman 16 Gun Fire Safe))
(4865,List(Perfect Fitness Perfect Rip Deck, Nike Men's Dri-FIT Victory Golf Polo))
(4795,List(Nike Men's Dri-FIT Victory Golf Polo, O'Brien Men's Neoprene Life Vest))
(4420,List(Nike Men's CJ Elite 2 TD Football Cleat, Nike Men's Dri-FIT Victory Golf Polo))
(4371,List(Nike Men's Dri-FIT Victory Golf Polo, Field & Stream Sportsman 16 Gun Fire Safe))
(4100,List(Perfect Fitness Perfect Rip Deck, Diamondback Women's Serene Classic Comfort Bi))
(3990,List(Nike Men's Dri-FIT Victory Golf Polo, Pelican Sunstream 100 Kayak))



/*


orderItemsDF.map(x => x.toString()).map(x => x.split(",")).map(x => (x(1).toInt, x(2).toInt)).combineByKey((x:Int) => (Set(x)), (x:Set[Int], y:Int) => (x + y), (x:Set[Int], y:Set[Int]) => (x ++ y)).map{case (x:Int, y:Set[Int]) => (x, y.toList)}.flatMapValues(v => v.combinations(2)).map{case (x:Int, y:List[Int]) => ((y(0), y(1)), 1)}.reduceByKey((x,y) => x+y).map{case (x,y) => (y,x)}.sortByKey(false).take(10)


res57: Array[(Int, (Int, Int))] = Array((3691,(365,403)), (3482,(365,502)), (3332,(365,1014)), (3197,(403,502)), (3005,(365,1004)), (2986,(403,1014)), (2858,(502,1014)), (2828,(403,1004)), (2682,(365,1073)), (2648,(502,1004)))

*/
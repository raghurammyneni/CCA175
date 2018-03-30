//helper functions
ordersDF.printSchema

//create a DF dataframe reader option, infer schema
val myDF = sqlContext.read. \
    format("csv"). \
    option("inferSchema", "true"). \
    option("header", "true"). \
    load("/loudacre/myFile.csv") 

//Schema

StructType([StructFields])
StructField(ColName, ColType, Nullable)

//python
from pyspark.sql.types import *

columnsList = [StructField("pcode", StringType()),
               StructField("lastName", StringType()),
               StructField("firstName", StringType()),
               StrunctField("age", IntegerType())]

peopleSchema = StructType(columnsList)

//scala
import org.apache.spark.sql.types._

val columnsList = [StructField("pcode", StringType,
               StructField("lastName", StringType,
               StructField("firstName", StringType,
               StrunctField("age", IntegerType]

val peopleSchema = StructType(columnsList)


//create a DF dataframe reader option, defined schema
val myDF = sqlContext.read. \
    format("csv"). \
    schema(peopleSchema). \
    option("header", "true"). \
    load("/loudacre/myFile.csv") 



//DF actions         
count
first
take(n)
show(n)
collect
write

//DF Trasformations
select
where
orderBy
join
limit(n)


orders_df.select("order_customer_id", "order_date", "order_status").show()
orders_df.select(to_date(from_unixtime(col("order_date")/1000)) as "order_dt").show()

orders_df.select("order_customer_id", "order_date", "order_status").where("order_status = 'PROCESSING'").show()

//to perform operations on the values of the columns use column references or cast them to "col"
//both operations are same
orders_df.select(orders_df("order_id") * 2).show()
orders_df.select(col("order_id") * 2).show()

orders_df.where(orders_df("order_status").startsWith("C")).show()

//this would through an error -
orders_df.select("order_id" * 2).show()

//assigning alias
orders_df.select((orders_df("order_id") * 2).alias("new_col_id")).show()
orders_df.select((col("order_id") * 2).alias("new_col_id")).show()

//aggregation - 
orders_df.groupBy("order_status").count().show()

//Joining DF
peopleDF.join(pcodeDF, peopleDF("pcode") === pcodeDF("pcode"), "left_outer").show()


























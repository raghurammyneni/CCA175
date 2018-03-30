//Create a DF from avro data file
import com.databricks.spark.avro._;
val ordersDF = sqlContext.read.avro("/user/cloudera/problem1/orders");

//create a DF from json data file
val orders_df = sqlContext.read.json("/user/cloudera/problem5/json-no-compress")

//create a DF from parquet data file
val parquetDF = sqlContext.read.parquet("/user/cloudera/problem5/parquet")

//use parquet-tools to read parquet files
parquet-tools head mydatafile.parquet
parquet-tools schema mydatafile.parquet
parquet-tools schema hdfs://master-1/loudacre/devices_parquet/

//create a DF from text file 
val prodsDF = sqlContext.read.text("/user/cloudera/problem5/products-text")

//create a DF from csv file 
val prodsDF = sqlContext.read.csv("/user/cloudera/problem5/products-text")

val caersDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/user/cloudera/FDA/CAERS_ASCII_2004_2017Q2.csv")

val peopleDF = sqlContext.read. 
    option("inferSchema", "true"). \
    option("header", "true"). \
    .csv("people.csv")

//create a DF from ORC data file
val orcDF = sqlContext.read.orc("/user/cloudera/problem5/orc")

//create a DF dataframe reader option
val myDF = sqlContext.read. \
    format("csv").
    option("header", "true"). \
    load("/loudacre/myFile.csv") 

//create DF from list 
val myList = List(("Raghu", "Myneni"), ("Sailaja", "Kasaraneni"))
val myDF = sqlContext.createDataFrame(myList)

//create a DF from hive table 
val myDF = sqlContext.read.table("my_table")



//helper functions
ordersDF.printSchema



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








val diamonds = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
sqoop import-all-tables 
--connect jdbc:mysql://localhost/retail_db 
--username retail_dba 
--password cloudera 
--warehouse-dir /user/hive/warehouse/retail_stage.db 
--compress 
--compression-codec snappy 
--as-avrodatafile 
-m 1





mkdir avro-test
cd avro-test

hdfs dfs -get /user/hive/warehouse/retail_stage.db/orders/part-m-00000.avro
ls
avro-tools getschema part-m-00000.avro > orders.avsc
ls
cat orders.avsc

/*
[cloudera@quickstart avro-test]$ cat orders.avsc
{
  "type" : "record",
  "name" : "orders",
  "doc" : "Sqoop import of orders",
  "fields" : [ {
    "name" : "order_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_id",
    "sqlType" : "4"
  }, {
    "name" : "order_date",
    "type" : [ "null", "long" ],
    "default" : null,
    "columnName" : "order_date",
    "sqlType" : "93"
  }, {
    "name" : "order_customer_id",
    "type" : [ "null", "int" ],
    "default" : null,
    "columnName" : "order_customer_id",
    "sqlType" : "4"
  }, {
    "name" : "order_status",
    "type" : [ "null", "string" ],
    "default" : null,
    "columnName" : "order_status",
    "sqlType" : "12"
  } ],
  "tableName" : "orders"
}

*/



hdfs dfs -mkdir /user/hive/schemas
hdfs dfs -mkdir /user/hive/schemas/orders
hdfs dfs -put avro-test/orders.avsc /user/hive/schemas/orders

hdfs dfs -ls /user/hive/schemas/orders


CREATE EXTERNAL TABLE orders_sqoop
STORED AS AVRO
LOCATION '/user/hive/warehouse/retail_stage.db/orders'
TBLPROPERTIES ('avro.schema.url' = '/user/hive/schemas/orders/orders.avsc');

DESCRIBE FORMATTED orders_sqoop;

SELECT * FROM orders_sqoop as c WHERE c.order_date IN
(SELECT b.order_date FROM
(SELECT a.order_date, COUNT(1) as total_orders 
FROM orders_sqoop as a 
GROUP BY a.order_date 
ORDER BY total_orders DESC LIMIT 1) as b)


create database retail;
show databases;
show tables;
use retail;


CREATE EXTERNAL TABLE orders_avro
 (order_id INT,
  order_date DATE,
  order_customer_id INT,
  order_status STRING)
 PARTITIONED BY (order_month STRING)
 STORED AS AVRO;



SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 300;



insert overwrite table orders_avro 
partition (order_month)
select order_id, 
to_date(from_unixtime(cast(order_date/1000 as int))), 
order_customer_id, 
order_status, 
substr(from_unixtime(cast(order_date/1000 as int)),1,7) as order_month 
from default.orders_sqoop;



SELECT * FROM orders_avro as c WHERE c.order_date IN
(SELECT b.order_date FROM
(SELECT a.order_date, COUNT(1) as total_orders 
FROM orders_avro as a 
GROUP BY a.order_date 
ORDER BY total_orders DESC LIMIT 1) as b)


hdfs dfs -get /user/hive/schemas/orders/orders.avsc
gedit orders.avsc
hdfs dfs -put -f orders.avsc /user/hive/schemas/orders/orders.avsc
hdfs dfs -cat orders.avsc /user/hive/schemas/orders/orders.avsc



describe default.orders_sqoop;
select * from default.orders_sqoop limit 5;


use default;
insert into orders_sqoop VALUES (999999, 1374735600000, 256, 'New Style', 3, 'CLOSED');


































































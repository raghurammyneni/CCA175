sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table products_replica \
--fields-terminated-by "|" \
--lines-terminated-by "\n" \
--where "product_id >= 1 AND product_id <= 1000" \
--null-non-string -1 \
--null-string "NOT-AVAILABLE" \
--target-dir /user/cloudera/problem5/products-text \
-m 3
--outdir /home/cloudera/sqoop1 \
--boundary-query "select min(product_id), max(product_id) from products_replica where product_id between 1 and 1000";

/**************************************************************************************************************************************/


sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table products_replica \
--fields-terminated-by "*" \
--lines-terminated-by "\n" \
--null-non-string -1000 \
--null-string "NA" \
--where "product_id <= 1111" \
--target-dir /user/cloudera/problem5/products-text-part1 \
-m 2 \
--outdir /home/cloudera/sqoop2 \
--boundary-query "SELECT MIN(product_id), MAX(product_id) FROM products_replica WHERE product_id <= 1111" 


/**************************************************************************************************************************************/



sqoop import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table products_replica \
--fields-terminated-by "*" \
--lines-terminated-by "\n" \
--null-non-string -1000 \
--null-string "NA" \
--where "product_id > 1111" \
--target-dir /user/cloudera/problem5/products-text-part2 \
-m 5 \
--outdir /home/cloudera/sqoop3 \
--boundary-query "SELECT MIN(product_id), MAX(product_id) FROM products_replica WHERE product_id > 1111" 


/**************************************************************************************************************************************/




sqoop merge \
--class-name products_replica \
--jar-file /tmp/sqoop-cloudera/compile/bb11ea7e587bcfe7819b566ede2b0135/products_replica.jar \
--merge-key product_id \
--new-data /user/cloudera/problem5/products-text-part2 \
--onto /user/cloudera/problem5/products-text-part1  \
--target-dir /user/cloudera/problem5/products-text-both-parts



/**************************************************************************************************************************************/


sqoop job \
--create first_sqoop_job \
-- import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table products_replica \
--target-dir /user/cloudera/problem5/products-incremental \
--check-column product_id \
--incremental append
--last-value 0


sqoop job --exec first_sqoop_job

insert into products_replica values (1346,2,'something 1','something 2',300.00,'not avaialble',3,'STRONG');
insert into products_replica values (1347,5,'something 787','something 2',356.00,'not avaialble',3,'STRONG');

sqoop job --exec first_sqoop_job

insert into products_replica values (1376,4,'something 1376','something 2',1.00,'not avaialble',3,'WEAK');
insert into products_replica values (1365,4,'something 1376','something 2',10.00,'not avaialble',null,'NOT APPLICABLE');


/**************************************************************************************************************************************/



create table products_hive  (product_id int, product_category_id int, product_name string, product_description string, product_price float, product_imaage string,product_grade int,  product_sentiment string);

sqoop job \
--create hive_sqoop_job \
-- import \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table products_replica \
--check-column product_id \
--incremental append \
--last-value 0 \
--hive-import \
--hive-database problem5 \
--hive-table products_hive

sqoop job --exec hive_sqoop_job


insert into products_replica values (1378,4,'something 1376','something 2',10.00,'not avaialble',null,'NOT APPLICABLE');
insert into products_replica values (1379,4,'something 1376','something 2',10.00,'not avaialble',null,'NOT APPLICABLE');


/**************************************************************************************************************************************/


sqoop export \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--table products_external \
--update-key product_id \
--update-mode allowinsert \
--export-dir /user/hive/warehouse/problem5.db/products_hive \
--input-fields-terminated-by '\001' \
--input-null-non-string "null" \
--input-null-string "null" \
--columns "product_id, product_category_id, product_name, product_description, product_price, product_impage, product_grade, product_sentiment"






































































sqoop import-all-tables \
--connect jdbc:mysql://localhost/retail_db \
--username retail_dba \
--password cloudera \
--warehouse-dir /user/hive/warehouse/problem6.db \
--hive-import \
--hive-database problem6 \
--create-hive-table \
--as-textfile



sudo ln -s /usr/hive/conf/hive-site.xml /usr/spark/conf/hive-site.xml





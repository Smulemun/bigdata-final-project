DROP DATABASE IF EXISTS team16_projectdb CASCADE;

CREATE DATABASE team16_projectdb LOCATION "project/hive/warehouse";
USE team16_projectdb;

CREATE EXTERNAL TABLE ecom STORED AS AVRO LOCATION 'project/warehouse/ecom' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/ecom.avsc');

SELECT * FROM ecom LIMIT 100;
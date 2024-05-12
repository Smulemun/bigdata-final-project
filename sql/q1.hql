USE team16_projectdb;

DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results(
    brand VARCHAR(50),
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q1'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT brand, COUNT(*) as count
FROM ecom_part_buck
GROUP BY brand;

INSERT OVERWRITE DIRECTORY 'project/output/q1'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q1_results;

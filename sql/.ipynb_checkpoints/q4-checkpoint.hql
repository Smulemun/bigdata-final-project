USE team16_projectdb;

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results(
    category_code VARCHAR(50),
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q4'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q4_results
SELECT category_code, COUNT(*) as count
FROM ecom_part_buck
GROUP BY category_code
HAVING count < 300000;

INSERT OVERWRITE DIRECTORY 'project/output/q4'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q4_results;

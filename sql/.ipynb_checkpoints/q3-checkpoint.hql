USE team16_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(
    category_code VARCHAR(50),
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q3'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q3_results
SELECT category_code, COUNT(*) as count
    FROM ecom_part_buck
GROUP BY category_code;

INSERT OVERWRITE DIRECTORY 'project/output/q3'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q3_results;

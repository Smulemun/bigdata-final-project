USE team16_projectdb;

DROP TABLE IF EXISTS q2_results;
CREATE EXTERNAL TABLE q2_results(
    brand VARCHAR(50),
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q2'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q2_results
SELECT brand, COUNT(*) as count
FROM ecom_part_buck
WHERE brand != ''
GROUP BY brand
HAVING count >= 100000;

INSERT OVERWRITE DIRECTORY 'project/output/q2'
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ','
SELECT * FROM q2_results;

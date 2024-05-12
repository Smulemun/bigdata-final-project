USE team16_projectdb;

DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results(
    event_types VARCHAR(50),
    count BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q5'; 

INSERT INTO q5_results
SELECT event_types, COUNT(*) as count
FROM ecom_part_buck
GROUP BY event_types;

INSERT OVERWRITE DIRECTORY 'project/output/q5'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q5_results;

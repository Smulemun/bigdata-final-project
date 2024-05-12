USE team16_projectdb;

DROP TABLE IF EXISTS q6_results;
CREATE EXTERNAL TABLE q6_results(
    user_id INT,
    avg_money_spent FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q6';

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q6_results
SELECT user_id, AVG(price) as avg_money_spent
FROM ecom_part_buck
WHERE event_types = 'purchase'
GROUP BY user_id;

INSERT OVERWRITE DIRECTORY 'project/output/q6'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q6_results;
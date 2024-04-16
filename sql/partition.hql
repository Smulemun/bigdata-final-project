USE team16_projectdb;

-- tmp
DROP TABLE ecom_part_buck;

CREATE EXTERNAL TABLE ecom_part_buck(
    event_id int,
    event_time date,
    product_id int,
    category_id bigint,
    category_code varchar(100),
    brand varchar(100),
    price float,
    user_id int,
    user_session varchar(100)
) 
    PARTITIONED BY (event_types varchar(50)) 
    CLUSTERED BY (event_id) into 10 buckets
    STORED AS AVRO LOCATION 'project/hive/warehouse/employees_part_buck' 
    TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

INSERT INTO ecom_part_buck
SELECT event_id,
    from_unixtime(CAST(event_time/1000 AS bigint)) AS event_time, 
    product_id, category_id, category_code, brand, price, user_id, user_session, event_type FROM ecom;

SELECT * FROM ecom_part_buck WHERE event_type='"view"' AND event_id<100;
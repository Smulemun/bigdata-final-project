COPY ecom_large(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) FROM STDIN WITH CSV HEADER DELIMITER ',' NULL AS 'null';

#!/bin/bash

password=$(head -n 1 bigdata-final-project/secrets/.psql.pass)

pip install kaggle 

kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store/ -p bigdata-final-project/data/

unzip bigdata-final-project/data/ecommerce-behavior-data-from-multi-category-store.zip -d bigdata-final-project/data/

rm bigdata-final-project/data/ecommerce-behavior-data-from-multi-category-store.zip

python3 bigdata-final-project/scripts/build_db.py

hdfs dfs -rm -r /user/team16/project/warehouse/*

sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team16_projectdb --username team16 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1

# ?
mv *.avsc bigdata-final-project/output/
mv *.java bigdata-final-project/output/
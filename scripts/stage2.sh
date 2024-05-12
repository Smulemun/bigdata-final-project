#!/bin/bash

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put output/*.avsc project/warehouse/avsc

password=$(head -n 1 secrets/.psql.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/db.hql > output/hive_results.txt 2> /dev/null

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/partition.hql

echo "brand,count" > output/q1.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/q1.hql 
hdfs dfs -cat project/output/q1/* >> output/q1.csv

echo "brand,count" > output/q2.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/q2.hql 
hdfs dfs -cat project/output/q2/* >> output/q2.csv

echo "category_code,count" > output/q3.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/q3.hql 
hdfs dfs -cat project/output/q3/* >> output/q3.csv

echo "category_code,count" > output/q4.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/q4.hql 
hdfs dfs -cat project/output/q4/* >> output/q4.csv

echo "event_types,count" > output/q5.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/q5.hql 
hdfs dfs -cat project/output/q5/* >> output/q5.csv

echo "user_id,avg_money_spent" > output/q6.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/q6.hql
hdfs dfs -cat project/output/q6/* >> output/q6.csv


hdfs dfs -rm -R .Trash


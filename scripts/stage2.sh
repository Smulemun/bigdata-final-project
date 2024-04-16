#!/bin/bash

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put bigdata-final-project/output/*.avsc project/warehouse/avsc

password=$(head -n 1 bigdata-final-project/secrets/.psql.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f bigdata-final-project/sql/db.hql > bigdata-final-project/output/hive_results.txt 2> /dev/null

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f bigdata-final-project/sql/partition.hql
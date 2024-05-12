#!/bin/bash

password=$(head -n 1 secrets/.psql.pass)

hdfs dfs -mkdir project/output/realeval

hdfs dfs -put output/evaluation.csv project/output/realeval

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team16 -p $password -f sql/create_results.hql
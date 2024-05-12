#!/bin/bash
set -e # EXIT ON ERROR

rm -f bigdata-final-project/data/test.json > /dev/null
rm -f bigdata-final-project/data/train.json > /dev/null
rm -f bigdata-final-project/data/train_folded.json > /dev/null
rm -f bigdata-final-project/data/test_folded.json > /dev/null
hdfs dfs -rm -f -r project/data/train_folded > /dev/null
hdfs dfs -rm -f -r project/data/test_folded > /dev/null
hdfs dfs -rm -f -r project/models/model1/ > /dev/null
rm -r -f models/model1 > /dev/null
hdfs dfs -rm -f -r project/models/model2/ > /dev/null
rm -r -f models/model2 > /dev/null
hdfs dfs -rm -f -R .Trash > /dev/null

echo "Starting Preprocessing the data"
spark-submit --master yarn scripts/preprocessing.py 2> /dev/null
echo "Successfully executed scripts/preprocessing.py"

echo "Grid Search Model 1"
spark-submit --master yarn scripts/als.py 2> /dev/null
echo "Successfully executed scripts/als.py"

echo "Grid Search Model 2"
spark-submit --master yarn scripts/kfold_grid_search.py 2> /dev/null
echo "Successfully executed scripts/kfold_grid_search.py" 

echo "Evaluation model1, model2"
spark-submit --master yarn scripts/inference.py 2> /dev/null
echo "Successfully executed scripts/inference.py"
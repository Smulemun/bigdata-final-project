"""
    DOCSTRING
"""

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from data_utils import load_data, run
import numpy as np

TEAM = 16
NWORKERS = 3
CORES = 1
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName(f"{TEAM} - spark ML (ALS model)")\
        .master("yarn")\
        .config("hive.metastore.uris",
                "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", WAREHOUSE)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .config('spark.executor.instances', NWORKERS)\
        .config("spark.executor.cores", CORES)\
        .config("spark.executor.cpus", CORES)\
        .config("spark.executor.memory", "3g")\
        .enableHiveSupport()\
        .getOrCreate()

train_df = load_data(path='project/data', spark=spark, split='train',
                     feature='category_code')
test_df = load_data(path='project/data', spark=spark, split='test',
                    feature='category_code')

print('loaded data')

als = ALS(maxIter=5,
          userCol="user_id",
          itemCol="product_id",
          ratingCol="rating",
          coldStartStrategy="drop")

als_model = als.fit(train_df)

print('trained model')

predictions = als_model.transform(test_df)

evaluator = RegressionEvaluator(labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
print("Root-mean-square error = " + str(rmse) + '\n' + 'R2 = ' + str(r2))

grid = ParamGridBuilder()
grid = grid.addGrid(als.blockSize, [1024, 2048])\
           .addGrid(als.regParam, np.logspace(1e-3, 1e-1, num=2))\
                    .build()

evaluator1 = RegressionEvaluator(labelCol="rating",
                                 predictionCol="prediction",
                                 metricName="rmse")

cv = CrossValidator(estimator=als,
                    estimatorParamMaps=grid,
                    evaluator=evaluator1,
                    parallelism=5,
                    numFolds=3)

cv_model_als = cv.fit(train_df)

print('performed grid search')

model1 = cv_model_als.bestModel

model1.write().overwrite().save("project/models/model1")

run("hdfs dfs -get project/models/model1 models/model1")

print('saved model')

predictions = model1.transform(test_df)

predictions.select("rating", "prediction")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .save("project/output/model1_predictions")

run("hdfs dfs -cat project/output/model1_predictions/*.csv > output/model1_predictions.csv")

print('saved predictions')

rmse1 = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
r21 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
print("Best model root-mean-square error = " + str(rmse1) +
      '\n' + 'R2 = ' + str(r21))

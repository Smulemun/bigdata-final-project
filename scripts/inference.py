'''
INFERENCE MODELS
'''
import os
import warnings
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from data_utils import load_data
from model_utils import load_model, eval_model
warnings.filterwarnings("ignore")

TEAM = 16
NWORKERS = 3
CORES = 1
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName(f"{TEAM} - spark ML (ALS model)")\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", WAREHOUSE)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .config('spark.executor.instances', NWORKERS)\
        .config("spark.executor.cores", CORES)\
        .config("spark.executor.cpus", CORES)\
        .config("spark.executor.memory", "3g")\
        .enableHiveSupport()\
        .getOrCreate()

sc = spark.sparkContext
sc.addPyFile('scripts/data_utils.py')
sc.addPyFile('scripts/model_utils.py')
sc.addPyFile('scripts/net.py')

print(f'{"_"*100}INFERENCE MODELS{"_"*100}')

test_df = load_data(path='project/data', spark=spark, split='test', feature='category_code')

model1 = load_model(model_name='model1')
model2 = load_model(model_name='model2')

print('Models loaded')

evaluator = RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="rmse")

results1 = eval_model(evaluator=evaluator, model=model1, test_df=test_df,
                      save=True, model_name='model1', ranking_metrics=True)
results2 = eval_model(evaluator=evaluator, model=model2, test_df=test_df,
                      save=True, model_name='model2', ranking_metrics=True)

results1['model'] = str(model1)
results2['model'] = str(model2)
df = spark.createDataFrame([results1, results2])
df.show(truncate=False)

df.coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .save("project/output/evaluation/")

os.system("hdfs dfs -cat project/output/evaluation/*.csv > output/evaluation.csv")

spark.stop()

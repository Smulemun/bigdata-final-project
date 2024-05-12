"""
    Model training
"""
import warnings
import time
import argparse
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from net import Content_based_filtering
from sparktorch import serialize_torch_obj, SparkTorch
from sparktorch import PysparkPipelineWrapper
import torch
import torch.nn as nn
from model_utils import eval_model, save_model
from data_utils import load_data
warnings.filterwarnings("ignore")


# Initialize parser
parser = argparse.ArgumentParser()

# Adding optional argument
parser.add_argument("--split_train", type=str, required=True)
parser.add_argument("--split_test", type=str, required=True)
parser.add_argument("--brand_dim", type=int, required=True)
parser.add_argument("--dim", type=int, required=True)
parser.add_argument("--save", action='store_true', default=False)

# Read arguments from command line
args = parser.parse_args()

brand_dim = args.brand_dim
dim = args.dim
split_train = args.split_train
split_test = args.split_test
save = args.save

user_features = ['user_id']
item_features = ['product_id', 'category_code', 'brand', 'price']
session_features = ['year', 'month_sin', 'month_cos', 'day_sin', 'day_cos',
                    'hour_sin', 'hour_cos', 'minute_sin', 'minute_cos',
                    'second_sin', 'second_cos']
TARGET = 'rating'

TEAM = 16
NWORKERS = 5
CORES = 1
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName(f"{TEAM} - spark ML")\
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
sc.addPyFile('scripts/net.py')
sc.addPyFile('scripts/data_utils.py')
sc.addPyFile('scripts/model_utils.py')
vector_assembler = VectorAssembler(inputCols=user_features + item_features +
                                   session_features, outputCol='features')
evaluator = RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="rmse")

print(f'{"_"*100}TRAINING THE MODEL{"_"*100}')

model = Content_based_filtering(
    n_brands=34,
    n_items=39699,
    n_users=97917,
    brand_dim=brand_dim,
    dim=dim,
)
print('created model')
torch_obj = serialize_torch_obj(
    model=model,
    criterion=nn.L1Loss(),
    optimizer=torch.optim.Adam,
    lr=0.0001,
)
print('created torch obj')
spark_model = SparkTorch(
    inputCol='features',
    labelCol=TARGET,
    predictionCol='prediction',
    torchObj=torch_obj,
    iters=100,
    miniBatch=64,
    verbose=1,
)
print('created spark model')


train_data = load_data(split=split_train, spark=spark)
test_data = load_data(split=split_test, spark=spark)

# train_data.select('features').show(1, truncate=False)

print('loaded folded data')

start = time.time()
cur_model = Pipeline(stages=[vector_assembler, spark_model]).fit(train_data)
end = time.time()
print('trained model')
print(f'Time elapsed: {end - start}')
if save:
    save_model(cur_model, model_name='model2')

# cur_model.write().overwrite().save("project/models/model2")
# print('saved to hdfs')
# run("hdfs dfs -get project/models/model2 models/model2")
# save_model(pipeline=cur_model, model_name='model2')
# print('saved model')

model = PysparkPipelineWrapper.unwrap(cur_model)

results = eval_model(evaluator=evaluator, model=model, test_df=test_data, save=False)
with open('output/nn_rmse.txt', 'w', encoding="utf-8") as f:
    f.write(str(results['RMSE']))

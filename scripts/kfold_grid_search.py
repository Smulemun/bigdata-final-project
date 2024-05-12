'''
Grid-Search K-fold CV for model2
'''
import os
import warnings
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, row_number
from pyspark.sql import Window
from data_utils import load_data, save_data
from pyspark.ml.evaluation import RegressionEvaluator


warnings.filterwarnings("ignore")

TEAM = 16
NWORKERS = 1
CORES = 1
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName(f"{TEAM} - spark ML")\
        .master("yarn")\
        .config("hive.metastore.uris",
                "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", WAREHOUSE)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .config('spark.executor.instances', NWORKERS)\
        .config("spark.executor.cores", CORES)\
        .config("spark.executor.cpus", CORES)\
        .config("spark.executor.memory", "2g")\
        .enableHiveSupport()\
        .getOrCreate()


sc = spark.sparkContext
sc.addPyFile('scripts/net.py')
sc.addPyFile('scripts/data_utils.py')
sc.addPyFile('scripts/model_utils.py')

user_features = ['user_id']
item_features = ['product_id', 'category_code', 'brand', 'price']
session_features = ['year', 'month_sin', 'month_cos', 'day_sin', 'day_cos',
                    'hour_sin', 'hour_cos', 'minute_sin', 'minute_cos',
                    'second_sin', 'second_cos']

train_df = load_data(path='project/data', spark=spark, split='train',
                     feature='category_code')


def k_fold_split(df, k=3):
    """
        K-Fold split
    """
    df_with_id = df.withColumn("id", monotonically_increasing_id())
    w = Window.orderBy(df_with_id.id.desc())
    df_with_id = df_with_id.withColumn("id", row_number().over(w))
    total_rows = df_with_id.count()
    fold_size = total_rows // k
    for n in range(k):
        test_fold_start = n * fold_size
        test_fold_end = (n + 1) * fold_size if n != k - 1 else total_rows
        test_fold = df_with_id.filter((col("id") >= test_fold_start) & (col("id") < test_fold_end))

        training_fold = df_with_id.filter((col("id") < test_fold_start) |
                                          (col("id") >= test_fold_end))
        test_fold = test_fold.drop("id")
        training_fold = training_fold.drop("id")

        yield (test_fold, training_fold)


brand_dims = [8, 16]
dims = [16, 32]

BEST_PARAMS = None
best_rmse = float('inf')

evaluator = RegressionEvaluator(labelCol="rating", predictionCol="prediction",
                                metricName="rmse")
split_train = 'train_folded'
split_test = 'test_folded'

print(f'Grid search params: brand_dim:{brand_dims}, dim:{dims}')

for brand_dim in brand_dims:
    for dim in dims:
        print(f'Params: brand_dim={brand_dim}, dim={dim}')
        rmses = []
        for ind, data in enumerate(k_fold_split(train_df)):
            test_data, train_data = data
            test_data = test_data.repartition(CORES)
            train_data = train_data.repartition(CORES)

            save_data(train_data, split=split_train)
            save_data(test_data, split=split_test)

            RETURN_CODE = subprocess.call(["spark-submit", "--master", "yarn",
                                           "scripts/train.py", "--split_train",
                                           f"{split_train}", "--split_test",
                                           f"{split_test}", "--brand_dim",
                                           f"{brand_dim}", "--dim", f"{dim}"],
                                          stderr=subprocess.DEVNULL)

            if RETURN_CODE == 0:
                print("Process completed successfully")
            else:
                print(f"Process failed with return code {RETURN_CODE}")

            os.system(f'hdfs dfs -rm -r project/data/{split_train} 2> /dev/null')
            os.system(f'hdfs dfs -rm -r project/data/{split_test} 2> /dev/null')
            os.system(f'rm -f data/{split_train}.json 2> /dev/null')
            os.system(f'rm -f data/{split_test}.json 2> /dev/null')
            os.system('hdfs dfs -rm -R .Trash 2> /dev/null')

            # model = load_model(model_name='model2')
            # model = PysparkPipelineWrapper.unwrap(PipelineModel.load('project/models/model2'))
            # results = eval_model(evaluator=evaluator, model=model, test_df=test_data, save=False)

            with open('output/nn_rmse.txt', 'r', encoding='utf-8') as f:
                cur_rmse = float(f.readline())

            print(f"FOLD {ind}, RMSE: {cur_rmse}")
            rmses.append(cur_rmse)

        print('#' * 70)
        mean_rmse = sum(rmses) / len(rmses)
        print(f'MEAN RMSE: {mean_rmse}')
        if mean_rmse < best_rmse:
            # best_model = model
            best_rmse = mean_rmse
            BEST_PARAMS = (brand_dim, dim)
        # del [model, results, test_data, train_data]
        # gc.collect()

print(f'Best RMSE: {best_rmse}')
print(f'Best params: {BEST_PARAMS}')

print('Training the best model')
RETURN_CODE = subprocess.call(["spark-submit", "--master", "yarn",
                               "scripts/train.py", "--split_train", "train",
                               "--split_test", "test", "--brand_dim",
                               f"{BEST_PARAMS[0]}", "--dim",
                               f"{BEST_PARAMS[1]}", "--save"],
                              stderr=subprocess.DEVNULL)

if RETURN_CODE == 0:
    print("Process completed successfully")
else:
    print(f"Process failed with return code {RETURN_CODE}")
spark.stop()

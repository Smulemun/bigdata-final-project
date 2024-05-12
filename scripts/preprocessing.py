"""
    Preprocess Data
"""
import math
from pyspark.ml import Transformer
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, IntegerType
from pyspark.ml.feature import VectorAssembler, Word2Vec, RegexTokenizer
from pyspark.ml.feature import MinMaxScaler
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from data_utils import save_data


TEAM = 16
NWORKERS = 1
CORES = 1
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName(f"{TEAM} - spark ML")\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", WAREHOUSE)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()

sc = spark.sparkContext
sc.addPyFile('scripts/net.py')
sc.addPyFile('scripts/data_utils.py')

print('started preprocessing')

data = spark.read.format("avro").table('team16_projectdb.ecom_part_buck')


class CyclicTransformer(Transformer):
    """ Custom Transformer for Date """

    def __init__(self, input_col):
        super().__init__()
        self.input_col = input_col

    def _transform(self, dataset):
        extract_year = F.udf(lambda x: x.year)
        extract_month = F.udf(lambda x: x.month)
        extract_day = F.udf(lambda x: x.day)
        extract_hour = F.udf(lambda x: x.hour)
        extract_minute = F.udf(lambda x: x.minute)
        extract_second = F.udf(lambda x: x.second)

        return dataset.withColumn('year', extract_year(self.input_col))\
                 .withColumn('month', extract_month(self.input_col))\
                 .withColumn('day', extract_day(self.input_col))\
                 .withColumn('hour', extract_hour(self.input_col))\
                 .withColumn('minute', extract_minute(self.input_col))\
                 .withColumn('second', extract_second(self.input_col))\
                 .withColumn('month_sin', F.sin(F.col('month') * 2 * math.pi / 12))\
                 .withColumn('month_cos', F.cos(F.col('month') * 2 * math.pi / 12))\
                 .withColumn('day_sin', F.sin(F.col('day') * 2 * math.pi / 31))\
                 .withColumn('day_cos', F.cos(F.col('day') * 2 * math.pi / 31))\
                 .withColumn('hour_sin', F.sin(F.col('hour') * 2 * math.pi / 24))\
                 .withColumn('hour_cos', F.cos(F.col('hour') * 2 * math.pi / 24))\
                 .withColumn('minute_sin', F.sin(F.col('minute') * 2 * math.pi / 60))\
                 .withColumn('minute_cos', F.cos(F.col('minute') * 2 * math.pi / 60))\
                 .withColumn('second_sin', F.sin(F.col('second') * 2 * math.pi / 60))\
                 .withColumn('second_cos', F.cos(F.col('second') * 2 * math.pi / 60))\
                 .drop('month').drop('day')\
                 .drop('hour').drop('minute').drop('second')


cyclic_trans = CyclicTransformer('event_time')
data = cyclic_trans.transform(data)
data = data.na.drop(subset=data.columns)
data = data.filter(data.brand != '')
data = data.filter(data.category_code != '')

event_type_to_rating = F.udf(lambda x: 1 if x == 'purchase' else
                             0 if x == 'cart' else -1, IntegerType())
data = data.withColumn('rating', event_type_to_rating('event_types'))\
            .drop('event_types')
brand_counts = data.groupBy("brand").count()
rare_brands = brand_counts.filter(F.col("count") < 10000).select("brand").rdd\
                          .flatMap(lambda x: x).collect()
data = data.withColumn("brand", F.when(F.col("brand").isin(rare_brands), "other")\
           .otherwise(F.col("brand")))

user_features = ['user_id']
item_features = ['product_id', 'category_code', 'brand', 'price']
session_features = ['year', 'month_sin', 'month_cos', 'day_sin', 'day_cos',
                    'hour_sin', 'hour_cos', 'minute_sin',
                    'minute_cos', 'second_sin', 'second_cos']
TARGET = 'rating'
user_interaction_counts = data.groupBy('user_id').count()
active_users = user_interaction_counts.filter(F.col('count') > 5)
data_filtered = data.join(active_users, 'user_id', 'inner')
data_filtered = data_filtered.withColumn('year', F.col('year').cast('int'))
columns_to_scale = ['year', 'price']

assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec")
              for col in columns_to_scale]
scalers = [MinMaxScaler(inputCol=col + "_vec", outputCol=col + "_scaled")
           for col in columns_to_scale]
pipeline = Pipeline(stages=assemblers + scalers)
scalerModel = pipeline.fit(data_filtered)
scaled_data = scalerModel.transform(data_filtered)

scaled_data = scaled_data.drop(*['year', 'price', 'year_vec', 'price_vec'])

udf_extract_double = udf(lambda vector: vector.tolist()[0], FloatType())
scaled_data = scaled_data.withColumn("year", udf_extract_double("year_scaled"))\
                         .withColumn("price", udf_extract_double("price_scaled"))
scaled_data = scaled_data.drop(*['year_scaled','price_scaled','count', 'category_id'])
unique_user_ids = scaled_data.select('user_id').distinct().rdd.flatMap(lambda x: x).collect()
user_id_mapping = {_id: idx for idx, _id in enumerate(unique_user_ids)}
user_id_mapper = F.udf(lambda x: user_id_mapping[x], IntegerType())
mapped_data = scaled_data.withColumn('user_id', user_id_mapper('user_id'))

unique_product_ids = mapped_data.select('product_id').distinct().rdd.flatMap(lambda x: x).collect()
product_id_mapping = {_id: idx for idx, _id in enumerate(unique_product_ids)}
product_id_mapper = F.udf(lambda x: product_id_mapping[x], IntegerType())
mapped_data = mapped_data.withColumn('product_id', product_id_mapper('product_id'))

tokenizer = RegexTokenizer(inputCol='category_code',
                           outputCol='tokenized_category', pattern=r"\.")
word2Vec = Word2Vec(vectorSize=16, seed=42, minCount=1, inputCol='tokenized_category',
                    outputCol='category_embedding')
embedding_pipeline = Pipeline(stages=[tokenizer, word2Vec]).fit(mapped_data)
mapped_data = embedding_pipeline.transform(mapped_data)
mapped_data = mapped_data.drop('category_code')\
                         .withColumnRenamed("category_embedding", "category_code")

unique_brand_ids = mapped_data.select('brand').distinct().rdd.flatMap(lambda x: x).collect()
brand_id_mapping = {_id: idx for idx, _id in enumerate(unique_brand_ids)}
brand_id_mapper = F.udf(lambda x: brand_id_mapping[x], IntegerType())
mapped_data = mapped_data.withColumn('brand', brand_id_mapper('brand'))

N_users = mapped_data.select('user_id').distinct().count()
N_products = mapped_data.select('product_id').distinct().count()
N_brands = mapped_data.select('brand').distinct().count()

window_spec = Window.partitionBy('user_id').orderBy('event_time')

df_with_row_number = mapped_data.withColumn('row_number', F.row_number().over(window_spec))

user_count_window = Window.partitionBy('user_id')
total_user_count = F.count('user_id').over(user_count_window)

# Calculate the 80% threshold for each user group
TRAIN_TEST_RATIO = 0.8
split_threshold = (total_user_count * TRAIN_TEST_RATIO).cast('int')

df_labeled = df_with_row_number.withColumn('split', F.when(F.col('row_number') <= split_threshold,
                                                           'train').otherwise('test'))

# Split the DataFrame into train and test sets based on the label
train_df = df_labeled.filter(F.col('split') == 'train').drop('row_number', 'split')
test_df = df_labeled.filter(F.col('split') == 'test').drop('row_number', 'split')


train_df = train_df.select(user_features + item_features + session_features + [TARGET])
test_df = test_df.select(user_features + item_features + session_features + [TARGET])

print('data preprocessed')

save_data(train_df, split='train')
save_data(test_df, split='test')

train_df.show(1, truncate=False)

spark.stop()

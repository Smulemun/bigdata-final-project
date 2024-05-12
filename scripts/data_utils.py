"""
    Data Utils
"""
import os
from pyspark.ml.linalg import Vectors, VectorUDT
import pyspark.sql.functions as F

array_to_vector = F.udf(lambda x: Vectors.dense(x), VectorUDT())


def run(command):
    """Run bash commands"""
    return os.popen(command).read()


def save_data(data_frame, split='train'):
    """Save pyspark dataframes"""
    data_frame.coalesce(1)\
            .write\
            .mode("overwrite")\
            .format("json")\
            .save(f"project/data/{split}")

    run(f"hdfs dfs -cat project/data/{split}/*.json > data/{split}.json")
    print(f'Data saved to project/data/{split}')


def load_data(split, spark, path='project/data', feature='category_code'):
    """Load pyspark dataframes"""
    data_frame = spark.read.json(os.path.join(path, split))
    data_frame = data_frame.withColumn(feature, F.col(feature).getField("values"))\
                            .withColumn(feature, array_to_vector(F.col(feature)))
    print(f'Loaded {os.path.join(path, split)}')
    return data_frame

import os
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.ml.pipeline import PipelineModel, Pipeline
from sparktorch import PysparkPipelineWrapper
import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.window import Window


def save_model(pipeline, path: str = 'project/models/',
               model_name: str = 'model1'):
    """
    Saves a trained pipeline model to a specified path in HDFS and retrieves it
    locally under the models directory.

    Parameters:
    - pipeline (Pipeline or PipelineModel): The Spark ML pipeline or pipeline
        model to be saved.
    - path (str): The directory path in HDFS where the model will be saved.
        Default is 'project/models/'.
    - model_name (str): The name to save the model file as. Default is
    'model1'.
    """

    pipeline.write().overwrite().save(os.path.join(path, model_name))
    print('saved to hdfs')
    os.popen(
        f"hdfs dfs -get {os.path.join(path, model_name)} models/{model_name}")
    print(f'Model saved at models/{model_name}')


def load_model(path: str = 'project/models/', model_name: str = 'model1'):
    """
    Load a saved PySpark ML Pipeline or PyTorch model from the specified path in HDFS.

    :param path: The directory path in HDFS where the model is stored.
    :param model_name: The name of the model file.
    :return: The loaded PySpark ML Pipeline or PyTorch model.
    """

    if model_name == 'model2':
        pipeline = PipelineModel.load(os.path.join(path, model_name))
        return PysparkPipelineWrapper.unwrap(pipeline)

    model = ALSModel.load(os.path.join(path, model_name))
    return model


def eval_model(evaluator: RegressionEvaluator, model: Pipeline,
               test_df, model_name: str = 'model1', save: bool = True,
               ranking_metrics: bool = False):
    """
    Evaluate a PySpark ML Pipeline model on a test dataset.

    :param evaluator: The PySpark ML RegressionEvaluator object.
    :param model: The PySpark ML Pipeline or PyTorch model to evaluate.
    :param test_df: The test dataset.
    :param model_name: The name of the model file.
    :param save: Whether to save the model predictions.
    :param ranking_metrics: Whether to calculate ranking metrics.
    :return: A dictionary containing the evaluation metrics.
    """

    predictions = model.transform(test_df)

    if save:
        save_predictions(predictions=predictions, model_name=model_name)

    res = {}

    res['RMSE'] = evaluator.evaluate(predictions,
                                     {evaluator.metricName: "rmse"})
    res['R2'] = evaluator.evaluate(predictions,
                                   {evaluator.metricName: "r2"})

    if ranking_metrics:
        rounded_prediction = predictions.withColumn("rounded_prediction",
                                                    F.round("prediction"))

        window_spec = Window.partitionBy("user_id").orderBy(
            F.col("rounded_prediction").desc())

        predictions_and_labels = rounded_prediction.withColumn(
            "rating_list",
            F.collect_list("rating").over(window_spec)).withColumn(
            "prediction_list",
            F.collect_list("rounded_prediction") .over(window_spec)).select(
            "user_id",
            "rating_list",
            "prediction_list")
        predictions_and_labels_rdd = predictions_and_labels.rdd.map(lambda row:\
                                    (row.prediction_list, row.rating_list))

        metrics = RankingMetrics(predictions_and_labels_rdd)
        res['MAP'] = metrics.meanAveragePrecision
        res['NDCG'] = metrics.ndcgAt(3)

    return res


def save_predictions(predictions, path: str = 'project/output/',
                     model_name: str = 'model1'):
    """
    Saves the predictions DataFrame as a CSV file to a specified path.

    Parameters:
    - predictions (DataFrame): The DataFrame containing the model's
    predictions.
    - path (str): The directory path where the predictions will be saved in
    HDFS.
    Default is 'project/output/'.
    - model_name (str): The name used to label the saved predictions file.
    Default is 'model1'.

    """
    predictions.select("rating", "prediction")\
        .coalesce(1)\
        .write\
        .mode("overwrite")\
        .format("csv")\
        .option("sep", ",")\
        .option("header", "true")\
        .save(os.path.join(path, f'{model_name}_predictions'))

    out_path = os.path.join(path, f'{model_name}_predictions', '*.csv ')
    os.system(
        f"hdfs dfs -cat {out_path} > output/{model_name}_predictions.csv")

    print(f'Predictions saved at output/{model_name}_predictions.csv')

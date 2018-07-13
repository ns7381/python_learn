import json

import numpy
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import StringIndexer, IndexToString, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, NumericType

from resource import Training, Source


class ML:
    sparkSession = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    def __init__(self):
        pass

    def read_source(self, source):
        """
        get spark dataframe from source
        :param source: a source that contains file_path, source_type, options and schema_json
        :return: spark dataframe
        """
        if source.schema_json:
            if isinstance(source.schema_json, str):
                schema = StructType.fromJson(json.loads(source.schema_json))
            elif isinstance(source.schema_json, dict):
                schema = StructType.fromJson(source.schema_json)
            else:
                raise TypeError("source schema should be str")
            return self.sparkSession.read.format(source.source_type) \
                .options(**json.loads(source.options)).schema(schema).load(source.file_path).cache()
        else:
            return self.sparkSession.read.format(source.source_type) \
                .options(**json.loads(source.options)).load(source.file_path).cache()

    def get_schema_json(self, source):
        """
        get spark dataframe schema json
        :param source: a source that contains file_path, source_type, options and schema_json
        :return: dataframe schema json
        """
        df = self.read_source(source)
        # df = df.withColumn("sepallength", df["sepallength"].cast(DoubleType()))
        # df = df.withColumn("sepalwidth", df["sepalwidth"].cast(DoubleType()))
        # df = df.withColumn("petallength", df["petallength"].cast(DoubleType()))
        # df = df.withColumn("petalwidth", df["petalwidth"].cast(DoubleType()))
        # df = df.withColumn("class", df["class"].alias("class", metadata={"label": True}))
        return df.schema.json()

    def compute_statistics(self, source, column_name):
        # TODO when field type is not number
        df = self.read_source(source)
        summary = df.describe([column_name]).toPandas()
        summary_dict = {k: v.decode().replace('"', "") for k, v in summary.values}
        min_val = float(summary_dict['min'])
        max_val = float(summary_dict['max'])
        interval = (max_val - min_val) / 10
        thresholds = numpy.arange(min_val, max_val + interval, interval)
        histogram_tuple = df.select(column_name).rdd.flatMap(lambda x: x).histogram(thresholds.tolist())
        return summary_dict, histogram_tuple

    def train_model(self, training):
        df = self.read_source(training.source)
        feature_names, label_converter, string_indexers = _feature_transform(df)
        assembler = VectorAssembler(inputCols=feature_names, outputCol="features")
        (trainingData, testData) = df.randomSplit([float(training.training_rate), (1 - float(training.training_rate))])

        import importlib
        algorithm_class = getattr(importlib.import_module("pyspark.ml" + training.algorithm_type), training.algorithm)()
        algorithm = algorithm_class.setParams(**training.params)
        stages = []
        stages.extend(string_indexers)
        stages.extend([assembler, algorithm, label_converter]) if label_converter else stages.extend(
            [assembler, algorithm])
        pipeline = Pipeline(stages=stages)

        pipeline_model = pipeline.fit(trainingData)
        pipeline_model.save(training.model_path)
        prediction = pipeline_model.transform(testData)
        evaluate_model(prediction, training.algorithm_type)

    def model_predict(self, training, predict_source):
        predict_df = self.read_source(predict_source)
        pipeline_model = PipelineModel.load(training.model_path)
        predict_result_df = pipeline_model.transform(predict_df)
        print(predict_result_df)


def evaluate_regression(prediction):
    rmse = RegressionEvaluator(metricName="rmse").evaluate(prediction)
    mse = RegressionEvaluator(metricName="mse").evaluate(prediction)
    r2 = RegressionEvaluator(metricName="r2").evaluate(prediction)
    mae = RegressionEvaluator(metricName="mae").evaluate(prediction)
    return {"rmse": rmse, "mse": mse, "r2": r2, "mae": mae}


def evaluate_classification(prediction):
    f1 = MulticlassClassificationEvaluator(metricName="f1").evaluate(prediction)
    weightedPrecision = MulticlassClassificationEvaluator(metricName="weightedPrecision").evaluate(prediction)
    weightedRecall = MulticlassClassificationEvaluator(metricName="weightedRecall").evaluate(prediction)
    accuracy = MulticlassClassificationEvaluator(metricName="accuracy").evaluate(prediction)
    areaUnderROC = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(prediction)
    areaUnderPR = BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(prediction)
    return {"f1": f1, "weightedPrecision": weightedPrecision, "weightedRecall": weightedRecall,
            "accuracy": accuracy, "areaUnderROC": areaUnderROC, "areaUnderPR": areaUnderPR}


def evaluate_model(prediction, algorithm_type):
    if algorithm_type == "classification":
        return evaluate_classification(prediction)
    else:
        return evaluate_regression(prediction)


def _feature_transform(df):
    string_indexers = list()
    label_converter = None
    feature_names = []
    for field in df.schema.fields:
        is_label = field.metadata and field.metadata['label']
        if is_label:
            if not isinstance(field.dataType, NumericType):
                string_indexer = StringIndexer(inputCol=field.name, outputCol="label").fit(df)
                label_converter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                                labels=string_indexer.labels)
                string_indexers.append(string_indexer)
            else:
                df.withColumnRenamed(field.name, "label")
        elif not isinstance(field.dataType, NumericType):
            feature_name = field.name + "_idx"
            string_indexer = StringIndexer(inputCol=field.name, outputCol=feature_name).fit(df)
            string_indexers.append(string_indexer)
            feature_names.append(feature_name)
        else:
            feature_names.append(field.name)
    return feature_names, label_converter, string_indexers


if __name__ == "__main__":
    s = Source("E:\\tmp\\MachineLearning\\iris.csv", "csv", json.dumps({"header": True, "delimiter": " "}), {
        "fields": [{"metadata": {}, "name": "sepallength", "nullable": True, "type": "double"},
                   {"metadata": {}, "name": "sepalwidth", "nullable": True, "type": "double"},
                   {"metadata": {}, "name": "petallength", "nullable": True, "type": "double"},
                   {"metadata": {}, "name": "petalwidth", "nullable": True, "type": "double"},
                   {"metadata": {"label": True}, "name": "class", "nullable": True, "type": "string"}],
        "type": "struct"}
               )
    # {"fields":[{"metadata":{},"name":"sepallength","nullable":true,"type":"double"},{"metadata":{},"name":"sepalwidth","nullable":true,"type":"double"},{"metadata":{},"name":"petallength","nullable":true,"type":"double"},{"metadata":{},"name":"petalwidth","nullable":true,"type":"double"},{"metadata":{"label":true},"name":"class","nullable":true,"type":"string"}],"type":"struct"}

    ml = ML()
    # ml.read_source(s)
    # schema_json = ml.get_schema_json(s)
    # print(schema_json)
    # s.schema_json = schema_json
    t = Training("classification", "LogisticRegression",
                 {"maxIter": 10, "regParam": 0.3, "elasticNetParam": 0.8, "family": "multinomial"}, 0.7, s, "./test")
    # ml.train_model(t)
    ml.model_predict(t, s)
    # ml.compute_statistics(s, "sepallength")

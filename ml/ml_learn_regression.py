# coding:utf-8
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import Row, functions
from pyspark.ml.linalg import Vector, Vectors
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, HashingTF, Tokenizer
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, BinaryLogisticRegressionSummary, \
    LogisticRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Word_Count").getOrCreate()


# spark = SparkSession.builder.master("spark://10.200.84.59:7077").appName("Word_Count").getOrCreate()


def f(x):
    rel = {}
    rel['features'] = Vectors.dense(float(x[0]), float(x[1]), float(x[2]), float(x[3]))
    rel['label'] = str(x[4])
    return rel


data = spark.sparkContext.textFile("E:/iris.txt").map(lambda line: line.split(',')).map(lambda p: Row(**f(p))).toDF()
data.createOrReplaceTempView("iris")
df = spark.sql("select * from iris where label != 'Iris-setosa'")

labelIndexer = StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
featureIndexer = VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(df)
trainingData, testData = df.randomSplit([0.7, 0.3])
lr = LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10).setRegParam(
    0.3).setElasticNetParam(0.8)
labelConverter = IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
lrPipeline = Pipeline().setStages([labelIndexer, featureIndexer, lr, labelConverter])

# lrPipelineModel = lrPipeline.fit(trainingData)
# lrPredictions = lrPipelineModel.transform(testData)
# preRel = lrPredictions.select("predictedLabel", "label", "features", "probability").collect()
# for item in preRel:
#     print(
#         str(item['label']) + ',' + str(item['features']) + '-->prob=' + str(
#             item['probability']) + ',predictedLabel' + str(
#             item['predictedLabel']))
# evaluator = MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
# lrAccuracy = evaluator.evaluate(lrPredictions)
# print("Test Error = " + str(1.0 - lrAccuracy))
# lrModel = lrPipelineModel.stages[2]
# print("Coefficients: " + str(lrModel.coefficients) + "Intercept: " + str(lrModel.intercept) + "numClasses: " + str(
#     lrModel.numClasses) + "numFeatures: " + str(lrModel.numFeatures))
#



paramGrid = ParamGridBuilder().addGrid(lr.elasticNetParam, [0.2, 0.8]).addGrid(lr.regParam, [0.01, 0.1, 0.5]).build()
cv = CrossValidator().setEstimator(lrPipeline).setEvaluator(
    MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol(
        "prediction")).setEstimatorParamMaps(paramGrid).setNumFolds(3)
cvModel = cv.fit(trainingData)
lrPredictions = cvModel.transform(testData)
lrPreRel = lrPredictions.select("predictedLabel", "label", "features", "probability").collect()
for item in lrPreRel:
    print(
        str(item['label']) + ',' + str(item['features']) + '-->prob=' + str(
            item['probability']) + ',predictedLabel' + str(
            item['predictedLabel']))
evaluator = MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
lrAccuracy = evaluator.evaluate(lrPredictions)
bestModel = cvModel.bestModel
lrModel = bestModel.stages[2]
print(
"Coefficients: " + str(lrModel.coefficientMatrix) + "Intercept: " + str(lrModel.interceptVector) + "numClasses: " + str(
    lrModel.numClasses) + "numFeatures: " + str(lrModel.numFeatures))
lr.explainParam(lr.regParam)
lr.explainParam(lr.elasticNetParam)

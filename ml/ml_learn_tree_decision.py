# coding:utf-8
from pyspark.ml.linalg import Vector, Vectors
from pyspark.sql import Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassificationModel, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.master("local").appName("Word_Count").getOrCreate()


# spark = SparkSession.builder.master("spark://10.200.84.59:7077").appName("Word_Count").getOrCreate()


def f(x):
    rel = {}
    rel['features'] = Vectors.dense(float(x[0]), float(x[1]), float(x[2]), float(x[3]))
    rel['label'] = str(x[4])
    return rel


data = spark.sparkContext.textFile("E:/iris.txt").map(lambda line: line.split(',')).map(lambda p: Row(**f(p))).toDF()
data.createOrReplaceTempView("iris")
df = spark.sql("select * from iris")
# rel = df.rdd.map(lambda t : str(t[1])+":"+str(t[0])).collect()
# for item in rel:
#     print(item)
labelIndexer = StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
featureIndexer = VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(df)
trainingData, testData = df.randomSplit([0.7, 0.3])
dtClassifier = DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
labelConverter = IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
pipelinedClassifier = Pipeline().setStages([labelIndexer, featureIndexer, dtClassifier, labelConverter])

modelClassifier = pipelinedClassifier.fit(trainingData)
predictionsClassifier = modelClassifier.transform(testData)
predictionsClassifier.select("predictedLabel", "label", "features").show(20)

evaluator = MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol(
    "prediction").setMetricName("accuracy")
lrAccuracy = evaluator.evaluate(predictionsClassifier)
print("Test Error = " + str(1.0 - lrAccuracy))
treeModelClassifier = modelClassifier.stages[2]
print("Learned classification tree model:\n" + str(treeModelClassifier.toDebugString))


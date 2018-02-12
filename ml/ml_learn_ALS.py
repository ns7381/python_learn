# coding:utf-8
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

spark = SparkSession.builder.master("local").appName("Word_Count").getOrCreate()


# spark = SparkSession.builder.master("spark://10.200.84.59:7077").appName("Word_Count").getOrCreate()


def f(x):
    rel = {}
    rel['userId'] = int(x[0])
    rel['movieId'] = int(x[1])
    rel['rating'] = float(x[2])
    rel['timestamp'] = float(x[3])
    return rel


ratings = spark.sparkContext.textFile("E:/learn/bigdata/spark/data/mllib/als/sample_movielens_ratings.txt").map(
    lambda line: line.split('::')).map(lambda p: Row(**f(p))).toDF()
ratings.show()

training, test = ratings.randomSplit([0.8, 0.2])
alsExplicit = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
alsImplicit = ALS(maxIter=5, regParam=0.01, implicitPrefs=True, userCol="userId", itemCol="movieId", ratingCol="rating")
modelExplicit = alsExplicit.fit(training)
modelImplicit = alsImplicit.fit(training)
predictionsExplicit = modelExplicit.transform(test)
predictionsImplicit = modelImplicit.transform(test)
predictionsExplicit.show()

evaluator = RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
rmseExplicit = evaluator.evaluate(predictionsExplicit)
print("Explicit:Root-mean-square error = "+str(rmseExplicit))
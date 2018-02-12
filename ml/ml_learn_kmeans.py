# coding:utf-8
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors

spark = SparkSession.builder.master("local").appName("Word_Count").getOrCreate()


# spark = SparkSession.builder.master("spark://10.200.84.59:7077").appName("Word_Count").getOrCreate()


def f(x):
    rel = {}
    rel['features'] = Vectors.dense(float(x[0]), float(x[1]), float(x[2]), float(x[3]))
    return rel


df = spark.sparkContext.textFile("E:/iris.txt").map(lambda line: line.split(',')).map(lambda p: Row(**f(p))).toDF()
kmeansmodel = KMeans().setK(3).setFeaturesCol('features').setPredictionCol('prediction').fit(df)
results = kmeansmodel.transform(df).collect()
for item in results:
    print(str(item[0]) + ' is predcted as cluster' + str(item[1]))

results2 = kmeansmodel.clusterCenters()
for item in results2:
    print(item)

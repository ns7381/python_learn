# coding:utf-8
from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
spark = SparkSession.builder.master("spark://10.200.84.59:7077").appName("Word_Count").getOrCreate()
documentDF = spark.createDataFrame([
    ("Hi I heard about Spark".split(" "), ),
    ("I wish Java could use case classes".split(" "), ),
    ("Logistic regression models are neat".split(" "), )
], ["text"])
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")
model = word2Vec.fit(documentDF)
result = model.transform(documentDF)
for row in result.collect():
    text, vector = row
    print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))


# coding:utf-8
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
spark = SparkSession.builder.master("spark://10.200.84.59:7077").appName("Word_Count").getOrCreate()
sentenceData = spark.createDataFrame(
    [(0, "I heard about Spark and I love Spark"), (0, "I wish Java could use case classes"),
     (1, "Logistic regression models are neat")]).toDF("label", "sentence")
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
rescaledData.select("label", "features").show()

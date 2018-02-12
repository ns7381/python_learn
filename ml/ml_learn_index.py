# coding:utf-8
from pyspark.ml.feature import StringIndexer, IndexToString, OneHotEncoder
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Word_Count").getOrCreate()
# spark = SparkSession.builder.master("spark://10.200.84.59:7077").appName("Word_Count").getOrCreate()
df = spark.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")],
    ["id", "category"])
indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
model = indexer.fit(df)
indexed = model.transform(df)
indexed.show()

converter = IndexToString(inputCol="categoryIndex", outputCol="originalCategory")
converted = converter.transform(indexed)
converted.show()


encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec")
encoded = encoder.transform(indexed)
encoded.show()
import numpy as np
import pandas as pd
import pyspark
import os
import urllib
import sys

from pyspark.sql.functions import *
from pyspark.ml.classification import *
from pyspark.ml.evaluation import *
from pyspark.ml.feature import *


# start Spark session
spark = pyspark.sql.SparkSession.builder.appName('Iris').getOrCreate()

# load iris.csv into Spark dataframe
dataWCN = spark.read.csv('/FileStore/tables/iris.csv', header=None)
columns = ["sepal-length", "sepal-width", "petal-length", "petal-width", "class"]
dataString = dataWCN.toDF(*columns)
data = dataString.select([col('sepal-length').cast("double"),col('sepal-width').cast("double"),col('petal-length').cast("double"),col('petal-width').cast("double"),col('class')])


# vectorize all numerical columns into a single feature column
feature_cols = data.columns[:-1]
assembler = pyspark.ml.feature.VectorAssembler(inputCols=feature_cols, outputCol='features')
data = assembler.transform(data)


# convert text labels into indices
data = data.select(['features', 'class'])
label_indexer = pyspark.ml.feature.StringIndexer(inputCol='class', outputCol='label').fit(data)
data = label_indexer.transform(data)


# Indenfy relation between class and label
labeltypes = data.select([col('class'),col('label')]).distinct()


# only select the features and label column
data = data.select(['features', 'label'])
print("Reading for machine learning")


# train the model
reg = 0.01

lr = pyspark.ml.classification.LogisticRegression(regParam=reg)
model = lr.fit(data)


# Preparing predictions dataframe
pred_data = spark.createDataFrame(
[(5.1, 3.5, 1.4, 0.2),
(6.2, 3.4, 5.4, 2.3)],
["sepal-length", "sepal-width", "petal-length", "petal-width"])

# Transfrom the data
data = assembler.transform(pred_data)

# Performs the predictions
predictions = model.transform(data)

#Identify the class predicted
dataJoined = predictions.join(labeltypes,predictions.prediction ==  labeltypes.label,"left").select([col("class")])

# Write dataframe top disk
dataJoined.coalesce(1).write.mode('overwrite').csv("/FileStore/tables/task3_2.csv")








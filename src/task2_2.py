
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max


# SparkSession is created
spark = SparkSession \
    .builder \
    .appName("Task2_2") \
    .getOrCreate()

# Load a dataframe
parDF = spark.read.parquet("/data/src/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")

# Select certain columns and rename them
resultDF = parDF.select( [ max("price").alias("max_price"), min("price").alias("min_price"), count("price").alias("row_count") ])

# Save data to disk
resultDF.write.mode('overwrite').csv("/data/out/task2_2.csv")







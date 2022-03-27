
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# SparkSession is created
spark = SparkSession \
    .builder \
    .appName("Task2_3") \
    .getOrCreate()

# Creates dataframe with data
parDF = spark.read.parquet("/data/src/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")

# Filter the data according to certain conditions and calculates average
# if I choose review_scores_rating==10 no rows are returned, so I choose >=10 to get any rows
filteredDF = parDF.  \
            filter( (parDF.price > 5000) & (parDF.review_scores_rating.cast("integer") >= 10)).  \
            select( [ avg("bathrooms").alias("avg_bathrooms"), avg("bedrooms").alias("avg_bedrooms") ])


# Write data to disk
filteredDF.write.mode('overwrite').csv("/data/out/task2_3.csv")







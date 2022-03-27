
from pyspark.sql import SparkSession
from pyspark.sql.functions import count,min

# SparkSession is created
spark = SparkSession \
    .builder \
    .appName("Task2_3") \
    .getOrCreate()

# Datafrae is created fron file
parDF = spark.read.parquet("/data/src/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet")


#Creates a new column with the mininu price of dataframe
modifiedDF = parDF.join(parDF.agg(min('price').alias('Minimun_price')))

# Filters according to the conditions using the column created previosly
filteredDF = modifiedDF.filter( (modifiedDF.price.cast("integer") == modifiedDF.Minimun_price.cast("integer"))
                                & \
                                (modifiedDF.review_scores_rating.cast("integer") >= 10) \
                               ). \
                        select( [ count("beds").alias("People") ])

# Write data to disk
filteredDF.write.mode('overwrite').csv("/data/out/task2_4.csv")







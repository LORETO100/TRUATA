
import pyspark
from operator import add


# SparkContext is created
sc = pyspark.SparkContext(appName="task2_1b")

# Reads the data
lines = sc.textFile("/data/src/groceries.csv")

# Get a list of products
products = lines.flatMap(lambda line: line.split(","))

# Adds the ocurrences by poroduct and select the 5 higher
ocurrences = products.map(lambda product: (product,1)).reduceByKey(add).sortBy(lambda x: x[1], False).take(5)

# White data to disk
output_rdd=sc.parallelize(ocurrences)
output_rdd.coalesce(1).saveAsTextFile("/data/out/out.txt")







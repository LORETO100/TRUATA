
import pyspark

# SparkContext is created
sc = pyspark.SparkContext(appName="task2_1a")

# Read the file
lines = sc.textFile("/data/src/groceries.csv")

# Creates a list of products
products = lines.flatMap(lambda line: line.split(","))

# Saves the list to disk
products.coalesce(1).saveAsTextFile("/data/out/out.txt")






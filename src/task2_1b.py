
import pyspark

# SparkContext is created
sc = pyspark.SparkContext(appName="task2_1b")

# Read the file
lines = sc.textFile("/data/src/groceries.csv")

# List of products is created
products = lines.flatMap(lambda line: line.split(","))

# Count the number of products
nr_products = products.count()

# Write the list to disk
output_rdd=sc.parallelize(["Count:",str(nr_products)])
output_rdd.coalesce(1).saveAsTextFile("/data/out/out.txt")







# In Python

# Import method
from pyspark.context import SparkContext

# Create SparkContext object
sc = SparkContext('local', 'test')

# Create an RDD of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])
# Use map and reduceByKey transformations with their lambda
# expressions to aggregate and then compute average
print("Prueba")
print(dataRDD.glom().collect())
print("Prueba")
agesRDD = (dataRDD
	.map(lambda x: (x[0], (x[1], 1)))
	.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
	.map(lambda x: (x[0], x[1][0]/x[1][1])))
print(agesRDD.glom().collect())
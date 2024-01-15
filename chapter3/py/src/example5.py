# In Python, define a schema

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a DataFrame using SparkSession
spark = (SparkSession
	.builder
	.appName("example5")
	.getOrCreate())

# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
	StructField('UnitID', StringType(), True),
	StructField('IncidentNumber', IntegerType(), True),
	StructField('CallType', StringType(), True),
	StructField('CallDate', StringType(), True),
	StructField('WatchDate', StringType(), True),
	StructField('CallFinalDisposition', StringType(), True),
	StructField('AvailableDtTm', StringType(), True),
	StructField('Address', StringType(), True),
	StructField('City', StringType(), True),
	StructField('Zipcode', IntegerType(), True),
	StructField('Battalion', StringType(), True),
	StructField('StationArea', StringType(), True),
	StructField('Box', StringType(), True),
	StructField('OriginalPriority', StringType(), True),
	StructField('Priority', StringType(), True),
	StructField('FinalPriority', IntegerType(), True),
	StructField('ALSUnit', BooleanType(), True),
	StructField('CallTypeGroup', StringType(), True),
	StructField('NumAlarms', IntegerType(), True),
	StructField('UnitType', StringType(), True),
	StructField('UnitSequenceInCallDispatch', IntegerType(), True),
	StructField('FirePreventionDistrict', StringType(), True),
	StructField('SupervisorDistrict', StringType(), True),
	StructField('Neighborhood', StringType(), True),
	StructField('Location', StringType(), True),
	StructField('RowID', StringType(), True),
	StructField('Delay', FloatType(), True)])

# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "../../../databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
#fire_df.show(5)

# Return the rows where "CallType" is different from "Medical Incident"
few_fire_df = (fire_df
	.select("IncidentNumber", "AvailableDtTm", "CallType")
	.where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)

# In Python, return number of distinct types of calls using countDistinct()
from pyspark.sql.functions import *
(fire_df
	.select("CallType")
	.where(col("CallType").isNotNull())
	.agg(countDistinct("CallType").alias("DistinctCallTypes"))
	.show())

# Filter for only distinct non-null CallTypes from all the rows
(fire_df
	.select("CallType")
	.where(col("CallType").isNotNull())
	.distinct()
	.show(10, False))

# Rename column "Delay""
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
.select("ResponseDelayedinMins")
.where(col("ResponseDelayedinMins") > 5)
.show(5, False))

# Convert column format to timestamp type
fire_ts_df = (new_fire_df
.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
.drop("CallDate")
.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
.drop("WatchDate")
.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
"MM/dd/yyyy hh:mm:ss a"))
.drop("AvailableDtTm"))
# Select the converted columns
(fire_ts_df
.select("IncidentDate", "OnWatchDate", "AvailableDtTS")
.show(5, False))
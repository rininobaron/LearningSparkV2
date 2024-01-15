// In Scala, define a schema

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Create a DataFrame using SparkSession
val spark = SparkSession.builder.appName("example4").getOrCreate()

// Programmatic way to define a schema
val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
	StructField("UnitID", StringType, true),
	StructField("IncidentNumber", IntegerType, true),
	StructField("CallType", StringType, true),
	StructField("CallDate", StringType, true),
	StructField("WatchDate", StringType, true),
	StructField("CallFinalDisposition", StringType, true),
	StructField("AvailableDtTm", StringType, true),
	StructField("Address", StringType, true),
	StructField("City", StringType, true),
	StructField("Zipcode", IntegerType, true),
	StructField("Battalion", StringType, true),
	StructField("StationArea", StringType, true),
	StructField("Box", StringType, true),
	StructField("OriginalPriority", StringType, true),
	StructField("Priority", StringType, true),
	StructField("FinalPriority", IntegerType, true),
	StructField("ALSUnit", BooleanType, true),
	StructField("CallTypeGroup", StringType, true),
	StructField("NumAlarms", IntegerType, true),
	StructField("UnitType", StringType, true),
	StructField("UnitSequenceInCallDispatch", IntegerType, true),
	StructField("FirePreventionDistrict", StringType, true),
	StructField("SupervisorDistrict", StringType, true),
	StructField("Neighborhood", StringType, true),
	StructField("Location", StringType, true),
	StructField("RowID", StringType, true),
	StructField("Delay", FloatType, true)))

// Read the file using the CSV DataFrameReader
val sfFireFile="../../../databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
val fireDF = spark.read.schema(fireSchema).option("header", "true").csv(sfFireFile)
//fireDF.show(5)

// Return the rows where "CallType" is different from "Medical Incident"
val fewFireDF = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where($"CallType" =!= "Medical Incident")
fewFireDF.show(5, false)

// Return number of distinct types of calls using countDistinct()
fireDF.select("CallType").where(col("CallType").isNotNull).agg(countDistinct('CallType) as 'DistinctCallTypes).show()

// Filter for only distinct non-null CallTypes from all the rows
fireDF.select("CallType").where(col("CallType").isNotNull).distinct().show(10, false)

// Rename column "Delay"
val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF.select("ResponseDelayedinMins").where($"ResponseDelayedinMins" > 5).show(5, false)
// In Scala
import org.apache.spark.sql.Row
// Create a Row
val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
	Array("twitter", "LinkedIn"))
// Access using index for individual items
blogRow(1)

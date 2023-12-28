# In Python
from pyspark.sql import Row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
	["twitter", "LinkedIn"])
# access using index for individual items
print(blog_row[1])
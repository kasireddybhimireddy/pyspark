from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

# Create SparkSession
spark = SparkSession.builder \
    .appName("ThirdMaxSalary") \
    .getOrCreate()

# Sample DataFrame (Replace this with your DataFrame)
data = [(1, "John", 60000),
        (2, "Alice", 60000),
        (3, "Bob", 55000),
        (4, "Jane", 70000),
        (5, "Doe", 45000)]
columns = ["emp_id", "emp_name", "salary"]
df = spark.createDataFrame(data, columns)

# Create a window specification
windowSpec = Window.orderBy(df["salary"].desc())

# Add row number column
df = df.withColumn("denrank", dense_rank().over(windowSpec))

# Filter for the row with row_number 3
third_max_salary_df = df.filter(df["denrank"] == 3)

# Select only the necessary columns
result = third_max_salary_df.select("emp_id", "emp_name", "salary")

# Show the result
result.show()

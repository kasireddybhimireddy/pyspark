from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Example") \
    .getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("country_code", StringType(), True)
])

# Provided data and columns
data = [("IN",), ("SG",), ("US",)]
columns = ["country_code"]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
final_df = df.withColumn("country_name",
                         when(df["country_code"] == "IN", "India")
                         .when(df["country_code"] == "SG", "Singapore")
                         .when(df["country_code"] == "US", "United States"))
                       
                       
final_df.display()

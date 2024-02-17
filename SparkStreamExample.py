from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,array_contains
from pyspark.sql.types import DataType,StringType,IntegerType,FloatType,StructType,StructField,TimestampType


spark = SparkSession.builder.appName('test_dbs').getOrCreate()
userSchema = StructType().add("name", "string").add("salary", "integer")
df = spark.readStream.option("sep",",").schema(userSchema).csv('file:///C:/Users/kasir/PycharmProjects/python_spark/com/kasi/definative_guide/csv/*')
total_salary = df.groupBy('name').sum('salary')
total_salary.writeStream.outputMode("complete").format("console").start()

spark.sparkContext.broadcast()


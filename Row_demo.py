from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

list = [Row(name='kasi',sdal=123)]

df = spark.createDataFrame(list)

print(df.printSchema())
df.show()
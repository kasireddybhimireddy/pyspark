from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
list = [('kasi',123),('reddy',456)]

df = spark.createDataFrame(list,['name','sal'])
pd = df.toPandas()

print(pd)



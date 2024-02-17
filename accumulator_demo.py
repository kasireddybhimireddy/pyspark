from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
accumulator  = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1,2,3,4,5,6])
rdd.foreach(lambda x: accumulator.add(x))

print(accumulator.value)

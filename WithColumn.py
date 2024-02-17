from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.getOrCreate()

list=[('kasi',"123"),("reddy","345")]

df = spark.createDataFrame(list, ['name',"sal"])
df.printSchema()
df.withColumn('sal_int',col('sal').cast('integer')).withColumnRenamed('sal','saraly').show()
df.drop('sal').show()
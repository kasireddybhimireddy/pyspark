from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import explode, col

spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.option("multiline","true").json(r'C:\Users\kasir\PycharmProjects\python_spark\modified.json')
df.printSchema()
df.show()
df.select('detail-record').show()

tempDf = df.select("HEADER-RECORD", explode("DETAIL-RECORD").alias("parameters_exploded"),
                   "TRAILER")
explodedColsDf = tempDf.select("HEADER-RECORD", "parameters_exploded.*","TRAILER")
new_explode = explodedColsDf.select("HEADER-RECORD",explode("DETAILS").alias("detailsexplode"),"number","type","TRAILER")
new_explode.select(col("HEADER-RECORD.*"),"detailsexplode.*","number","type",col("TRAILER.*")).show()
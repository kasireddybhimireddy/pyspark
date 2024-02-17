from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test_my_spark_submit").getOrCreate()
list =[('kasi',2000),('bhimi',5000)]
emp_df = spark.createDataFrame(list,['name','sal'])
emp_df.write.parquet('emps')
test = spark.read.parquet('emps')
test.show()
list =[('kasi',2000,1001),('bhimi',5000,20002)]
emp_df = spark.createDataFrame(list,['name','sal','dept'])
emp_df.write.option('append').parquet('test_parquet')
spark.read.option('mergeschema',True).parquet('test_parquet').show()
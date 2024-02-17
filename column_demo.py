from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,concat,concat_ws


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data=[(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data).toDF("col1","col2","col3")
df.select((df.col1 + df.col2),lit('kasireddy')).show()


data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')]
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)
df.select(col('fname').alias('first_name')).show()
df.select(concat(df.fname, df.lname)).show()
df.select(concat_ws(" ",df.fname, df.lname).alias('full_name')).show()
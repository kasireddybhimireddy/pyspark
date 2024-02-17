from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("test_columns").getOrCreate()
list = [\
   ('kasi','apple'),\
    ('kasi','banana'),\
    ('kasi','oranges'),\
    ('lakshmi','apple'),\
    ('lakshmi','banana'), \
    ('harshith','oranges'),\
   ('harshith','carrot')

]
df = spark.createDataFrame(list,['name','item'])
df.filter(col('item') == 2).show()
#df.filter(col('item').isin(['apple','banana']) ).select(col('name')).distinct().show()



windowSpecAgg  = Window.partitionBy("name")
df.withColumn('count', count('item').over(windowSpecAgg)).filter(col('count') == 2).filter(
 col('item').isin(['apple', 'banana'])).select(col('name')).distinct().show()
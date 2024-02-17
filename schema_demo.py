from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import *


spark = SparkSession.builder.getOrCreate()
list = [('kasi',123),('laksmi',67)]
#without schema
with_out_schema_df = spark.createDataFrame(list)

with_out_schema_df.printSchema()

#with list as Schema
df = spark.createDataFrame(list,['name','sal'])

df.printSchema()

#using schema
schema = StructType([StructField('name',StringType(),True),
            StructField('sal',LongType(),True)])

schema_demo = spark.createDataFrame(list,schema)

schema_demo.show()

#add struct add type
schema1 = StructType().add('name',StringType(),True).add('sal',LongType(),True)

add_schema_demo = spark.createDataFrame(list,schema1)
add_schema_demo.printSchema()
add_schema_demo.show()
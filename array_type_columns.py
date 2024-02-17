from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,split,exp,array,array_contains


spark = SparkSession.builder.getOrCreate()

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtWork",ArrayType(StringType()),True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data, schema)
#Use explode() function to create a new row for each element in the given array column.
df.select(df.name, explode(df.languagesAtSchool)).show()

df.select(split(col('name'),',')).show()

df.select(array(df.currentState,df.previousState)).show()

df.select(array_contains(df.languagesAtSchool, 'Java')).show()

df.filter(array_contains(df.languagesAtSchool, 'Java')).show()
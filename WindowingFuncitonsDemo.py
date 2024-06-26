from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, rank, max, dense_rank, avg, cume_dist, lag, lead, percent_rank, ntile

spark = SparkSession.builder.appName('widows_demo').getOrCreate()

simpleData = [("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              ]

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)
df.select(max(df.salary)).show()

windowSpec = Window.partitionBy(df.department).orderBy(df.salary)

df.withColumn('row_number', row_number().over(windowSpec)).show()
df.withColumn('rank',rank().over(windowSpec)).show()
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number,rank,max


spark = SparkSession.builder.appName('widows_demo').getOrCreate()

simpleData = [("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              ]

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)
df.select(max(df.salary)).show()
#Window Function
windowSpec = Window.partitionBy(df.department).orderBy(df.salary)

df.withColumn('row_number', row_number().over(windowSpec)).show()
df.withColumn('rank',rank().over(windowSpec)).show()
df.withColumn('dense_rank', dense_rank().over(windowSpec)).show()
df.withColumn('avg_salary', avg(df.salary).over(windowSpec)).show()
df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()
df.withColumn("ntile",ntile(2).over(windowSpec)).show()

#Analytic functions
df.withColumn("cume_dist",cume_dist().over(windowSpec)) .show()
df.withColumn("lag",lag("salary",2).over(windowSpec)).show()
#df.withColumn("lead",lead("salary",2)).over(windowSpec).show()

#Aggrigate function
windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()
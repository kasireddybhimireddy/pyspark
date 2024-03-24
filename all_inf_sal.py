from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank,max,col,aggregate
from pyspark.sql.window import Window,WindowSpec


spark=SparkSession.builder.appName('').getOrCreate()

data = [(1, "John", 60000),
        (2, "Alice", 60000),
        (3, "Bob", 55000),
        (4, "Jane", 80000),
        (5, "Doe", 45000)]
columns = ["emp_id", "emp_name", "salary"]

df=spark.createDataFrame(data,columns)

df.createOrReplaceTempView("emp")


spark.sql(""" select max(salary) as ms from emp """).show()

print(df.agg(max("salary").alias("max_salary")).collect()[0]["max_salary"])
#print(df.agg(max("salary")))




spark.sql((""" select s.emp_name,s.salary from (select emp_name,salary,dense_rank() over (order by salary desc)as rank from emp)s
where rank=2""")).show()


windowSpec = Window.orderBy(col("salary").desc())
df2 = df.withColumn("rank", dense_rank().over(windowSpec))
second_highest_salary = df2.filter(col("rank") == 2).drop("emp_id")
second_highest_salary.show()

spark.stop()
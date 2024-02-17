from pyspark.sql import SparkSession
from pyspark.sql.functions import *



spark = SparkSession.builder.getOrCreate()
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

df.select(col('department')).distinct().show()

df.select(avg(df.salary).alias('average_sal')).show()

df.select(collect_list("salary")).show(truncate=False)

df.select(collect_set("salary").alias('salary_set')).show(truncate=False)

df.createOrReplaceTempView('emp')
spark.sql("select distinct department from emp ").show()
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

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
deptData = [("Sales", "SalesDept"), \
              ("Finance", "FinanceDept"),\
              ("Marketing", "MarketingDept")\
              ]
columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
columns_dept = ["department", "department_name"]
df_dept = spark.createDataFrame(data=deptData, schema=columns_dept)
df_dept.printSchema()


windowSpec = Window.partitionBy(col('department')).orderBy(col('salary').desc())
df_join = df.join(df_dept, df.department == df_dept.department, 'inner').drop(df_dept.department)
df_join.withColumn('dense_rank', dense_rank().over(windowSpec)).filter('dense_rank == 2').show()


'''
df.groupBy('department').max('salary').show(truncate=False)
df.groupBy('department').count().show()

windowSpec = Window.partitionBy(col('department')).orderBy(col('salary').desc())

df.withColumn('dense_rank', dense_rank().over(windowSpec)).filter('dense_rank == 2').show()

'''


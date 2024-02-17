from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('joins_demo').getOrCreate()
emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

#inner join
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id ,'inner').show()

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id ,'left').filter(col('dept_id').isNull()).show()

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id ,'left').filter("dept_id is null").show()

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id ,'left').filter(col('gender') == '').show()

#right and rignt0uter both are same
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id ,'right').show()

#Right a.k.a Rightouter
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id ,'rightouter').show()

#leftsemi join is similar to inner join difference being leftsemi join returns all
# columns from the left dataset and ignores all columns from the right dataset.
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id ,'leftsemi').show()

#leftanti join does the exact opposite of the leftsemi,
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'leftanti').show()

empDF.alias('emp1')\
    .join(empDF.alias('emp2'), col('emp1.superior_emp_id') == col('emp2.emp_id'), 'inner')\
    .select(col('emp1.emp_id'),col('emp1.name'),\
            col('emp2.emp_id').alias('manager_id'),col('emp2.name').alias('manager_name')).show()

#using sql
empDF.createOrReplaceTempView('emp')
deptDF.createOrReplaceTempView('dept')
spark.sql('select * from emp').show()
spark.sql('select * from emp e , dept d where e.emp_dept_id == d.dept_id').show()
#using join key word
spark.sql('select * from emp e inner join  dept d on e.emp_dept_id == d.dept_id').show()



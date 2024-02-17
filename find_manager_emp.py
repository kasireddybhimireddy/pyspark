from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('joins_demo').getOrCreate()


emp_list =[
    ("Emp00001",   "Ram",             "Emp00005"),
    ("Emp00002",     "Sharath",         "Emp00003"),
    ("Emp00003" ,    "Nivas",           "Emp00005"),
    ("Emp00004" ,    "Praveen",         "Emp00002"),
    ("Emp00005",     "Maharaj",         "Emp00002"),
("Emp00006",     "Maharaj1",      None   )
]

df = spark.createDataFrame(emp_list,["emp_id","emp_name","mgr_id"])
df.createTempView("emp")
spark.sql(""" SELECT e.emp_name, m.emp_name AS manager_name
FROM   emp e
 left 
 JOIN   emp m on e.mgr_id = m.emp_id """).show()
spark.sql(""" SELECT e.emp_name, m.emp_name AS manager_name
FROM   emp e left join emp m on e.mgr_id = m.emp_id """).show()




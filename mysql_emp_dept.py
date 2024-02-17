from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.\
    config("spark.jars", r"C:\Users\kasir\PycharmProjects\python_spark\com\kasi\saprksql\mysql-connector-j-8.0.31.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()

emp_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sample") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp") \
    .option("user", "root").option("password", "12345678").load()

dept_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sample") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "dept") \
    .option("user", "root").option("password", "12345678").load()
emp_df.printSchema()
dept_df.printSchema()
emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")

spark.sql(""" SELECT e.ename, m.ename AS manager_name
FROM   emp e left join emp m on e.mgr = m.empno """).show()

spark.sql(""" select * from (
select ename, sal, dense_rank() 
over(order by sal desc)r from emp) 
where r=3 """).show()

spark.sql(""" SELECT e.ename,d.dname,t.salary 
from 
dept d inner join emp e 
on d.deptno=e.deptno
 inner join 
(Select deptno,max(sal) salary 
from emp
 group by deptno)t 
on e.deptno= t.deptno and e.sal=t.salary""").show()

spark.sql(""" WITH salaries_ranks AS (
SELECT e.ename,
 d.dname,
 sal,
 ROW_NUMBER() OVER (PARTITION BY d.deptno ORDER BY sal DESC
 ) AS salary_rank
FROM dept d JOIN emp e ON d.deptno = e.deptno
)
 
SELECT *
FROM salaries_ranks
WHERE salary_rank=1""").show()

spark.sql(""" SELECT e.ename,d.dname,t.salary 
from 
dept d inner join emp e 
on d.deptno=e.deptno
 inner join 
(Select deptno,max(sal) salary 
from emp
 group by deptno)t 
on e.deptno= t.deptno and e.sal=t.salary""").show()

spark.sql(""" WITH salaries_ranks AS (
SELECT e.ename,
 d.dname,
 sal,
 ROW_NUMBER() OVER (PARTITION BY d.deptno ORDER BY sal DESC
 ) AS salary_rank
FROM dept d JOIN emp e ON d.deptno = e.deptno
)

SELECT *
FROM salaries_ranks
WHERE salary_rank=1""").show()

spark.sql(""" select * from (
SELECT e.ename,
 d.dname,
 sal,
Dense_rank() OVER (PARTITION BY d.deptno ORDER BY sal DESC
 )r
FROM dept d JOIN emp e ON d.deptno = e.deptno) where r=2""").show()




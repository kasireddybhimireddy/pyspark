from pyspark.sql import SparkSession


employee = [
(1, "Peter", "A", ["C#", "Biztalk", "Sql"]),
(2, "Paul", "A", ["C#", "Java", "Sharepoint", "Oracle"]),
(3, "Mary", "A", ["Spark", "Scala", "Java"]),
(4, "David", "B",["C#", "biztalk", "Sql", "Java"]),
(5, "Alan", "B", ["react", "angula", "javascript", "HTML"]),
(6, "John", "B",   ["javascript", "Scala", "Spark", "angular"])
]

spark = SparkSession.builder.appName("test_my_spark_submit").getOrCreate()
sc = spark.sparkContext

emps = sc.parallelize(employee)
empsDF = emps.toDF(["emp_no","emp_name","team","languages"])
empsDF.show()

empsDF.createTempView("emp")
spark.sql("select * from emp").show()
empsDF.write.parquet('test_csv')
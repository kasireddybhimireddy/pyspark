from pyspark.sql.functions import count, when, col, udf, lit
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


employee = [
(1, "Peter", "A", ["C#", "Biztalk", "Sql"]),
(2, "Paul", "A", ["C#", "Java", "Sharepoint", "Oracle"]),
(3, "Mary", "A", ["Spark", "Scala", "Java"]),
(4, "David", "B",["C#", "biztalk", "Sql", "Java"]),
(5, "Alan", "B", ["react", "angula", "javascript", "HTML"]),
(6, "John", "B",   ["javascript", "Scala", "Spark", "angular"])
]

spark = SparkSession.builder.appName('test_dbs').getOrCreate()
sc = spark.sparkContext

emps = sc.parallelize(employee)
empsDF = emps.toDF(["emp_no","emp_name","team","languages"])
team_count = empsDF.filter(F.col('team') == 'A').count()
print(team_count)
empsDF.show()
empsDF.printSchema()
#team_count = empsDF.filter(F.col('team') == 'A').count()

team_a_DF = empsDF.filter(F.col('team') == 'A')

result = empsDF.join(F.broadcast(team_a_DF),empsDF.emp_no == team_a_DF.emp_no, 'inner')

result.show()


empsDF.filter(F.array_contains(F.col('languages'),'Java')).show()


list = ["my name is vishnu"]
listRdd = spark.sparkContext.parallelize(list)
flatMapRdd=listRdd.flatMap(lambda x: x.split(" "))
for element in flatMapRdd.collect():
    print(element)
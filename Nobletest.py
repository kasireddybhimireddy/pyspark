from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,array_contains
from pyspark.sql.types import DataType,StringType,IntegerType,FloatType,StructType,StructField,TimestampType


spark = SparkSession.builder.appName('test').getOrCreate()
employee = [
(1, "Peter", "A", ["C#", "Biztalk", "Sql"]),
(2, "Paul", "A", ["C#", "Java", "Sharepoint", "Oracle"]),
(3, "Mary", "A", ["Spark", "Scala", "Java"]),
(4, "David", "B",["C#", "biztalk", "Sql", "Java"]),
(5, "Alan", "B", ["react", "angula", "javascript", "HTML"]),
(6, "John", "B",   ["javascript", "Scala", "Spark", "angular"])
]

employee_df = spark.createDataFrame(employee,["emp_no","emp_name","team","languages"])
employee_df.printSchema()

#Q1 Get the number of employee in Team A
print(employee_df.filter(col('team') == 'A').count())

#Q2 Get the Name of the employees which has biztalk skills
employee_df.filter(array_contains(col('languages'),'Biztalk')).show()

list =['My name is vishnu']

list_rdd = spark.sparkContext.parallelize(list)

list_rdd.flatMap(lambda x : x.split(' ')).foreach(lambda x : print(x))
flat_list_rdd = list_rdd.flatMap(lambda x : x.split(' '))
flat_list_rdd.map(lambda x: (x,1)).foreach(lambda x : print(x))

contract = [
    (16667,"COKE 65",None),
    (18933,"GC7",None),
    (15565,"COKE 65",1.2),
    (13548,None,55.6)
]


pricing = [
("GC7",datetime.strptime('2020-10-01', '%Y-%m-%d'),35.8),
("COKE 65",datetime.strptime('2020-10-01', '%Y-%m-%d'),22.4),
("GC7",datetime.strptime('2020-10-02', '%Y-%m-%d'),35.9),
("COKE 65",datetime.strptime('2020-10-02', '%Y-%m-%d'),22.5)
]

pricing_schema= StructType(
    [
        StructField("price_index",StringType(),False),
        StructField("price_date",TimestampType(),False),
        StructField("price",FloatType(),False)

    ]
)
contract_df = spark.createDataFrame(contract,['position','price_index','price'])
contract_df.printSchema()

pricing_df = spark.createDataFrame(pricing,pricing_schema)
pricing_df.printSchema()

contract_df.join(pricing_df,contract_df.price_index == pricing_df.price_index, 'inner').show()
#filter data based on date
contract_df.join(pricing_df,contract_df.price_index == pricing_df.price_index, 'inner').filter(pricing_df.price_date == '2020-10-02').show()

spark.readStream.csv()
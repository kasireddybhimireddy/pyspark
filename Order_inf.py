import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
                    .master('local[1]') \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

data=[(1,'rahul'),(2,'virat'),(3,'ranveer')]
schema="CustId int,CustName String"
CustDf=spark.createDataFrame(data,schema)

CustDf.show()

# 2 tables customer table and order table


data=[(1,1),(2,3)]
schema="OrderId int,CustId int"
OrderDf=spark.createDataFrame(data,schema)
OrderDf.show()

#Result dataframe

NonOrderDf=CustDf.join(OrderDf,CustDf.CustId==OrderDf.CustId,'left').filter(OrderDf.CustId.isNull()).select(CustDf.CustName)
NonOrderDf.show()


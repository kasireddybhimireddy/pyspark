#find the customer ids who brought all products
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct,col

spark=SparkSession.builder\
                  .master('local[1]')\
                  .appName('AllProduct')\
                  .getOrCreate()

data=[(1,2),(1,1),(2,1),(3,1),(3,2)]
schema="CustId int,ProductKey int"

OrderDf=spark.createDataFrame(data,schema)

data=[(1,),(2,)]
schema="ProductKey int"
ProductDf=spark.createDataFrame(data,schema)
ProductDf.show()

NoProductsDf=ProductDf.agg(countDistinct(col("ProductKey")).alias("CountProducts"))
NoProductsDf.show()

NoOrdersDf=OrderDf.groupBy(col("CustId")).agg(countDistinct(col("ProductKey")).alias(("CountProducts")))
NoOrdersDf.show()



ResultDf=NoProductsDf.join(NoOrdersDf,NoProductsDf.CountProducts==NoOrdersDf.CountProducts,'inner').select(col("CustId"))
ResultDf.show()


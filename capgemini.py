from pyspark.sql.functions import col, when
from com.kasi.util import SessionFactory



from pyspark.sql import SparkSession
spark = SessionFactory.getSparkSession()
data = ["IN","SG",  "PK"]

df = spark.createDataFrame(data, "string").toDF("country_code")
df.withColumn("country_name",when(df.country_code == "IN","INDIA").\
            when(df.country_code == "SG","SINAGAPORE").\
            when(df.country_code == "PK","PAKISTAN")).\
            show()




from pyspark.sql import SparkSession
from pyspark.sql.functions import col,max,dense_rank,row_number
from pyspark.sql.window import Window,WindowSpec


spark=SparkSession.builder.appName('')\
           .getOrCreate()

data=[(1,"s",88),(1,"s",88),(2,"U",80),
      (4,"e",12),(5,"yy",23),(6,"we",84),
      (7,"gg",123),(8,"il",23),(5,"yy",23),
      (10,"ee",11),(11,"kl",20),(12,"ik",88)]
columns=["emp_id","emp_name","sal"]
df=spark.createDataFrame(data,columns)
df.show()
print(df.count())
df.createOrReplaceTempView("temp")
distinct_df=spark.sql(""" select distinct emp_id,emp_name,sal from temp""")
distinct_df.show()
print(distinct_df.count())
df_row_number=spark.sql("""select *,row_number() over (partition by emp_id order by emp_id)as rownumber from temp """)
df_row_number2=df_row_number.filter(col("rownumber") ==1).drop("rownumber")
df_row_number2.show()
print(df_row_number2.count())

#df=spark.createDataFrame(data,columns)
#df.distinct().display()
#if use select along with distinct lost one coloumn
#df.select("emp_id","emp_name").distinct().display()
#df.dropDuplicates().display()
#df.dropDuplicates(['emp_id','emp_name']).display()

spark.stop()



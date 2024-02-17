from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.\
    config("spark.jars", r"C:\Users\kasir\OneDrive\Desktop\thales\Lib\mysql-connector-j-8.0.31.jar") \
    .master("local").appName("PySpark_MySQL_test").getOrCreate()

wine_df = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/thales") \
    .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "tickers") \
    .option("user", "root").option("password", "12345678").load()
wine_df = wine_df.drop_duplicates()
wine_df.printSchema()
wine_df.createOrReplaceTempView('tickers')

spark.sql('select * from tickers').show()
spark.sql("""select ticker, count(*) from tickers group by ticker """).show()
spark.sql("""  describe tickers """).show()
spark.sql("""  select max(open) as max_open from tickers     """).show()

json_df = spark.read.json(r'file:///C:\Users\kasir\PycharmProjects\python_spark\com\kasi\definative_guide\resources\2015-summary.json')

json_df.createOrReplaceTempView('2015_summary')

spark.sql(""" select * from 2015_summary """).show()

spark.sql(""" select dest_country_name, count(*) from 2015_summary group by dest_country_name """).show()
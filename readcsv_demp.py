from pyspark.sql import SparkSession



spark = SparkSession.builder.appName('readcsv').getOrCreate()

df = spark.read.option('header',True).option('inferschema',True).csv('annual-enterprise-survey-2021-financial-year-provisional-csv.csv')
df.show()
df.show(10,truncate=True)
df.show(10,truncate=False)
df.show(10,truncate=0,vertical=False)
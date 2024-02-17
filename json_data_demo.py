from pyspark.sql import SparkSession,Row
from pyspark.sql.types import MapType, StringType, StructType, StructField
from pyspark.sql.functions import from_json, to_json, col, json_tuple, get_json_object, schema_of_json, lit, explode

spark = SparkSession.builder.appName("json_data_demo").getOrCreate()

jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df=spark.createDataFrame([(1, jsonString)],["id","value"])
df.printSchema()
df.show(truncate=False)

print("from_json")

df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
df2.printSchema()
df2.show(truncate=False)

print("to_json")
df2.withColumn("value",to_json(col("value"))) \
   .show(truncate=False)

df2.printSchema()
print("tuple demo")
df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
    .toDF("id","Zipcode","ZipCodeType","City") \
    .show(truncate=False)
df.printSchema()

print("get_json_object")
df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
    .show(truncate=False)
print("schema_of_json")
schemaStr=spark.range(1) \
    .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
    .collect()[0][0]
print(schemaStr)


dfFromTxt=spark.read.text("resources/simple_zipcodes_json.txt")
dfFromTxt.printSchema()

schema = StructType([
    StructField("Zipcode",StringType(),True),
    StructField("ZipCodeType",StringType(),True),
    StructField("City",StringType(),True),
    StructField("State", StringType(), True)
  ])

dfJSON = dfFromTxt.withColumn("jsonData",from_json(col("value"),schema)) \
                   .select("jsonData.*")
dfJSON.printSchema()
dfJSON.show(truncate=False)


#Read json from string
data= [(""" {"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"} """)]
rdd = spark.sparkContext.parallelize(data)
df2 = spark.read.json(rdd)
df2.show()


rawDF = spark.read.json("resources/sample.json", multiLine = "true")
rawDF.printSchema()
sampleDF = rawDF.withColumnRenamed("id", "key")
batDF = sampleDF.select("key", "batters.batter")
batDF.printSchema()

batDF.show(1, False)
print('explode')
bat2DF = batDF.select("key", explode("batter").alias("new_batter"))
bat2DF.show()

bat2DF.printSchema()
bat2DF.select("key", "new_batter.*").show()

finalBatDF = (sampleDF
        .select("key",
explode("batters.batter").alias("new_batter"))
        .select("key", "new_batter.*")
        .withColumnRenamed("id", "bat_id")
        .withColumnRenamed("type", "bat_type"))


finalBatDF.printSchema()

finalBatDF.show()
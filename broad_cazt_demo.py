from pyspark.sql import SparkSession

states = {"NY":"New York", "CA":"California", "FL":"Florida"}
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
broadcast_value = spark.sparkContext.broadcast(states)
data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

def findCountryName(country_code):
    return  broadcast_value.value[country_code]


data_rdd = spark.sparkContext.parallelize(data)

result = data_rdd.map(lambda x : (x[0],x[1],x[2],findCountryName(x[3]))).collect()
print(result)
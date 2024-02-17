from pyspark.sql.functions import count, when, col, udf, lit
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
def func(rankings):
    list = rankings.split(",")
    return len(list)

def avarageRanking(rankings):
    list = rankings.split(",")
    sum = 0.0
    for i in list:
        sum = int(i) + sum
    avg = sum / len(list)
    return avg
func_udf = udf(func, IntegerType())
avg_udf = udf(avarageRanking, FloatType())

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
json_df = spark.read.option("multiline","true").\
    json(r"C:\Users\kasir\PycharmProjects\python_spark\com\kasi\definative_guide\mocked_test_data.json")

#1.	List of participants who has participated the maximum number of times.

count_max_num_parti = json_df.withColumn('max_parti',func_udf(json_df['rankings']))
max_num_times  = count_max_num_parti.agg({"max_parti": "max"}).collect()[0][0]
count_max_num_parti.where(F.col('max_parti') == max_num_times).select('name','max_parti').show(5)

#3.	The country with maximum participants.

json_max_games = json_df.groupby('alphanumeric').count().sort(F.col('count').desc())
max_num_times = json_max_games.take(1)
dict = max_num_times[0].asDict()
print(dict)

#4.	The country whose participants had highest average ranking.

avarage_parti = json_df.withColumn('avg_parti',avg_udf(json_df['rankings']))
max_avg_parti  = avarage_parti.agg({"avg_parti": "max"}).collect()[0][0]
avarage_parti.where(F.col('avg_parti') == max_avg_parti).select('name','avg_parti').show()





#5.	List of participants who participated the least number of times.

count_min_num_parti = json_df.withColumn('min_parti',func_udf(json_df['rankings']))
min_num_times  = count_min_num_parti.agg({"min_parti": "min"}).collect()[0][0]
count_min_num_parti.where(F.col('min_parti') == min_num_times).select('name','min_parti').show(5)



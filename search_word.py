from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local")\
                    .appName('Firstprogram')\
                    .getOrCreate()
sc=spark.sparkContext

# Read the input file and Calculating words count
text_file_rdd = sc.textFile("firstprogram.txt")

result_test = text_file_rdd.flatMap(lambda line : { str(line).__contains__('sparksession')})
for str in result_test.collect():
    if str :
        print('it containes the values ')
print(result_test.collect())
#print(result_test.count())

'''
result_test = text_file_rdd.filter(lambda line : 'sparksession' in line)
print(result_test.collect())
print(result_test.count())
'''
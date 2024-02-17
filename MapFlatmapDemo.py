from pyspark.sql import SparkSession
'''
def split(x):
    return x.split(' ')
    '''
spark = SparkSession.builder.appName('flatmap and map demo').getOrCreate()
list = ['this is kasi this is kasi']

list_rdd = spark.sparkContext.parallelize(list)
flat_map = list_rdd.flatMap(lambda x : x.split(' '))
print(flat_map.collect())

map_rdd = list_rdd.map(lambda x : (x,1))
print(map_rdd.collect())

map_rdd = flat_map.map(lambda x : (x,1))
print(map_rdd.collect())

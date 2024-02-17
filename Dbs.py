from pyspark.sql import SparkSession


list = [("Banana", 1000, "USA"),
        ("Carrots", 1500, "USA"),
        ("Beans ", 1600, "USA"),
        ("Orange", 2000, "USA"),
        ("Orange", 2000, "USA"),
        ("Banana", 400, "China"),
        ("Carrots", 1200, "China"),
        ("Beans", 1500, "China"),
        ("Orange", 4000, "China"),
        ("Banana", 2000, "Canada"),
        ("Carrots", 2000, "Canada"),
        ("Beans", 2000, "Mexico")
        ]

spark = SparkSession.builder.appName("test").getOrCreate()

product_df  = spark.createDataFrame(list,['product','amount','country'])
pivotDF = product_df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
pivotDF.show(truncate=False)
product_df.printSchema()
product_df.show()


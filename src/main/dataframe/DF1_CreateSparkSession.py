from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSessionExample").master("local[2]").getOrCreate()

data = [(1,"John"),(2,"Bob"),(3,"Alice")]
columns = ["id","name"]

input_df = spark.createDataFrame(data=data,schema=columns)
input_df.show()
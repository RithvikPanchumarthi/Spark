from pyspark import SparkContext, SparkConf

conf = ((((SparkConf()
           .setAppName("SparkContext_created_with_SparkConf")
           .setMaster("local[3]")
           .set("spark.executor.memory", "2g"))
          .set("spark.driver.memory", "1g"))
         .set("spark.executor.cores", "4"))
        .set("spark.executor.instances", "2"))

# Creating Spark Context 1st way
sc1 = SparkContext(conf=conf)
print("ApplicationName: ", sc1.appName)
print("Master: ", sc1.master)

'''
    Output:
        ApplicationName:  SparkContext_created_with_SparkConf
        Master:  local[3]
'''

#Creating Spark Context 2nd way

sc2 = SparkContext("local[*]", "SparkContext_Example")
print("ApplicationName: ", sc2.appName)
print("Master:", sc2.master)

'''
    Output : 
        ApplicationName:  SparkContext_Example
        Master: local[*]'''
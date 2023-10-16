from pyspark import SparkContext

sc = SparkContext("local[2]", "CreateRDD_Example")

#RDD from List.

list_data = [23,43,52,5435,65]

rdd_list = sc.parallelize(list_data)
print(rdd_list.collect())

''' Output:
        [23, 43, 52, 5435, 65]'''

#RDD   from Tuple.

tuple_data = [(1, "ABC"), (2, "BCD"), (3, "DEF")]

rdd_tuple = sc.parallelize(tuple_data)
print(rdd_tuple.collect())

''' Output: 
        [(1, "ABC"), (2, "BCD"), (3, "DEF")]
'''

#RDD from File.

rdd_file = sc.textFile("..\\resources\\input\\employee.csv")
print(rdd_file.collect())

''' Output:
        ['empid,name,age,dept,salary,gender', 
        "1,Daniel,32,IT,80000.50,'M'", 
        "2,Alex,33,IT,78000.25,'M'", 
        "3,Angelina,25,IT,93000.30,'F'", 
        "4,Bob,44,Finance,90000.99,'M'", 
        "5,Charlie,42,Finance,65000.99,'F'", 
        "6,Tom,40,Finance,55000.90,'M'", 
        "7,Harry,38,Audit,69000.10,'M'", 
        "8,Steve,52,Audit,65000.99,'F'", 
        "9,John,40,Audit,59000.90,'M'", 
        "10,Diana,38,Audit,69000.10,'F'"]
'''

#Empty RDD creation.

rdd_empty = sc.emptyRDD()
print("Empty RDD: ", rdd_empty.collect())

'''
    Output:
            Empty RDD:  []
'''
#Parallelizing Empty RDD

rdd_empty_list = sc.parallelize([])
print("Empty RDD List: ", rdd_empty_list.collect())

'''
    Output:
            Empty RDD:  []
'''
#Getting First Record in RDD
print("First Record: ", rdd_file.first())

'''
    Output:
            First Record:  empid,name,age,dept,salary,gender
'''
#Getting Count of Records in RDD
print("Count File: ", rdd_file.count())

'''
    Output:
            Count File:  11
'''

sc.stop()
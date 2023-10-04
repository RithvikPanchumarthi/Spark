from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, FloatType

spark = SparkSession.builder.appName("CrateDataframeExample").master("local[2]").getOrCreate()

data = [("Alice","Smith","511012","F",30000.10,34),
        ("Bob","Labu","511013","M",30300.10,35),
        ("Cat","Gil","511014","F",30040.10,23),
        ("Dark","Gibbs","511015","M",20000.10,34),
        ("Eve","Ville","511312","F",30500.10,24)]

emp_schema = StructType(
    [
        StructField("FirstName",StringType(),nullable=True),
        StructField("LastName",StringType(),nullable=True),
        StructField("Pincode",StringType(),nullable=True),
        StructField("Gender",StringType(),nullable=True),
        StructField("FirstName",FloatType(),nullable=True),
        StructField("Age",IntegerType(),nullable=True)
    ]
)

defined_schema_df = spark.createDataFrame(data = data, schema=emp_schema)

defined_schema_df.show()
defined_schema_df.printSchema()

spark.stop()

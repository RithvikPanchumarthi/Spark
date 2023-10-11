import lit as lit

from src.main.utilities.SparkSessionHandler import create_spark_session
from pyspark.sql.functions import col,concat,lit

spark = create_spark_session("TransformationsExample", "local[2]")

emp_input_path = ""
emp_df = spark.read.option("header",True).option("inferschema",True).csv(emp_input_path)
emp_df.printSchema()

emp_df2 = (emp_df
           .withColumn("salary", col("salary").cast("float"))
           .withColumn("increase_salary",col("salary")*1.10)
           .withColumn("increase_salary",round(col("salary")*1.10))
           .withColumn("increase_salary",round(col("salary")*1.10).cast("float"))
           .withColumn("company_name",lit("XYZ"))
           .withColumn("empid_name",concat(col("empid"),col("name")))
           .withColumn("empid_name",concat(col("empid"),lit("_")))
           .withColumn("")

            )

emp_df2.show(5,False)
emp_df2.printSchema()

spark.stop()
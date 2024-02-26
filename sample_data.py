from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(spark, input_data):
   df = spark.createDataFrame(input_data, ["name", "age", "city"])
   df = df.withColumnRenamed("city", "location")
   return df
   
   
if __name__ == "__main__":
   spark = SparkSession.builder.appName("DataTransformation").getOrCreate()
   sample_data = [("Alice", 25, "New York"), ("Bob", 30, "San Francisco")]
   result_df = transform_data(spark, sample_data)
   result_df.show()
   spark.stop()

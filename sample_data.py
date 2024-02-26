from pyspark.sql import SparkSession
from pyspark.sql.functions import col
def transform_data(spark, input_data):
   # Create DataFrame from sample data
   df = spark.createDataFrame(input_data, ["name", "age", "city"])
   # Perform transformations
   df = df.withColumnRenamed("city", "location")  # Intentional mistake
   return df
if __name__ == "__main__":
   # Initialize Spark session
   spark = SparkSession.builder.appName("DataTransformation").getOrCreate()
   # Sample data
   sample_data = [("Alice", 25, "New York"), ("Bob", 30, "San Francisco")]
   # Perform transformation
   result_df = transform_data(spark, sample_data)
   # Show the result
   result_df.show()
   # Stop Spark session
   spark.stop()

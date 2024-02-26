from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(spark, input_data):
   df = spark.createDataFrame(input_data, ["name", "age", "city"])
   df = df.withColumnRenamed("city", "location")
   return df
   
   
if __name__ == "__main__":
   spark = SparkSession.builder.appName("DataTransformation").getOrCreate()
   sample_data = [("Jay", 25, "Mumbai"),("ajay",25,"nashik"), ("Rahul", 30, "Pune"),("ANiket",34,"Aurangabad"),("ketki",33,"Pune")]
   result_df = transform_data(spark, sample_data)
   result_df.show()
   spark.stop()

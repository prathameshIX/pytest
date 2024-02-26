from transformation_script import transform_data
from pyspark.sql import SparkSession
import pytest

@pytest.fixture
def spark_session():
   return SparkSession.builder.appName("TestTransformation").getOrCreate()
  
def test_transformation(spark_session):
   input_data = [("Alice", 25, "New York"), ("Bob", 30, "San Francisco")]
   result_df = transform_data(spark_session, input_data)
   assert "location" in result_df.columns, "Column renaming failed"

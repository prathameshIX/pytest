import pytest
from transformation import perform_transformations
@pytest.fixture
def sample_data(spark):
   file1_path = "sample_data/countyData.csv"
   file2_path = "sample_data/fullData.csv"
   df1 = spark.read.csv(file1_path, header=True, inferSchema=True)
   df2 = spark.read.csv(file2_path, header=True, inferSchema=True)
   df1.createOrReplaceTempView("test_df1")
   df2.createOrReplaceTempView("test_df2")
   yield df1, df2

def test_transformations_with_tables(spark, sample_data):
   df1, df2 = sample_data
   result_df = perform_transformations("test_df1", "test_df2")
   assert result_df.count() > 0

def test_sql_transformations_with_tables(spark, sample_data):
   df1, df2 = sample_data
   result_df = perform_transformations("test_df1", "test_df2")
   result_df.createOrReplaceTempView("test_table")
   sql_result = spark.sql("SELECT * FROM test_table WHERE total_column1 > 0")
   assert sql_result.count() > 0

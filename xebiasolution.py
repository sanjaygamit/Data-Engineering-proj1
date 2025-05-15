from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType

# Create a SparkSession
spark = SparkSession.builder.appName("EmployeeAnalysis").getOrCreate()

# Define the schema for the employee data
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("join_date", DateType(), True),
    StructField("is_manager", BooleanType(), True)
])

# Sample data as a list of tuples
# Note: The date strings need to be in 'yyyy-MM-dd' format for direct conversion to DateType
data = [
    (1, "Alice", 25, "HR", 50000, "2020-03-15", False),
    (2, "Bob", 35, "IT", 80000, "2018-07-10", True),
    (3, "Charlie", 29, "Finance", 70000, "2019-05-20", False),
    (4, "Diana", 42, "IT", 120000, "2016-11-01", True),
    (5, "Ethan", 31, "HR", 60000, "2021-01-12", False)
]

# Create DataFrame
employee_df = spark.createDataFrame(data, schema=schema)

print("Original DataFrame:")
employee_df.show()

# 1. Find out employees who work in the IT department.
print("\n1. Employees in the IT department:")
it_employees_df = employee_df.filter(col("department") == "IT")
it_employees_df.show()

# 2. Count Employees in Each Department
print("\n2. Count of Employees in Each Department:")
department_counts_df = employee_df.groupBy("department").agg(count("*").alias("num_employees"))
department_counts_df.show()

# 3. Find all employees who are managers
print("\n3. All employees who are managers:")
managers_df = employee_df.filter(col("is_manager") == True)
managers_df.show()

# 4. Count Employees Who Joined After 2020
print("\n4. Count of Employees Who Joined After 2020:")
# We extract the year from the join_date column
employees_joined_after_2020_count = employee_df.filter(year(col("join_date")) > 2020).count()
print(f"Number of employees who joined after 2020: {employees_joined_after_2020_count}")

# To show the employees who joined after 2020 (optional)
employees_joined_after_2020_df = employee_df.filter(year(col("join_date")) > 2020)
print("\nEmployees who joined after 2020 (details):")
employees_joined_after_2020_df.show()


# Stop the SparkSession
spark.stop()

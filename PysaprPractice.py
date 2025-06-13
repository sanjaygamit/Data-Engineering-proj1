import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc # Alias sum to avoid conflict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Write a pyspark query  using below input to get below output.

# # input : 
# name  | Hobbies           |
# --------------------------|
# Alice | Badminton, Tennis |
# Bob   | Tennis, Cricket   |
# Julie | Cricket, Carroms  | 


# # output 

# Name | Hobbies 
# Alice | Badminton
# Alice | Tennis
# Bob   | Tennis
# Bob   | Cricket
# Julie | Cricket
# Julie | Carroms


data = [('Alice', 'Badminton, Tennis'),
        ('Bob', 'Tennis, Cricket'),
        ('Julie', 'Cricket, Carroms')]
columns = ['Name','Hobbies']
spark = SparkSession.builder.appName("HobbiesSplit").getOrCreate()
df = spark.createDataFrame(data,columns)
df.show()






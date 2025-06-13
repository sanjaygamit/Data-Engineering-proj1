import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc # Alias sum to avoid conflict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import * 

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


# data = [('Alice', 'Badminton, Tennis'),
#         ('Bob', 'Tennis, Cricket'),
#         ('Julie', 'Cricket, Carroms')]
# columns = ['name','hobbies']
# spark = SparkSession.builder.appName("HobbiesSplit").getOrCreate()
# df = spark.createDataFrame(data,columns)
# # df.show()

# df1 =  df.select(df.name,explode(split(df.hobbies,',')).alias('Hobbies'))
# df1.show()

# Write a pyspark query using below input to get below output. 

# city1 | city2 | city3
# goa   |       | ap
#       |  ap   | null
# null  |       | bglr 


# #Result 

# Result
# goa 
# ap
# bglr

data = [ ('goa', '', 'ap'), ('', 'ap', None), (None, '', 'bglr')]

columns = ["city1","city2","city3"]

spark = SparkSession.builder.appName("CitySplit").getOrCreate()

df = spark.createDataFrame(data,columns)
# df.show()

# df1 = df.withColumn('FirstNotNull',coalesce(df.city1,df.city2,df.city3))
df1 = df.withColumn('FirstNotNull',coalesce(
    when(df.city1 == '', None).otherwise(df.city1),
    when(df.city2 == '',None).otherwise(df.city2),
    when(df.city3 == '',None).otherwise(df.city3)) )

df3 = df1.select(df1.FirstNotNull)
df3.show()

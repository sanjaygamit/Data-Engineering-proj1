import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.functions import col, sum as _sum, desc # Alias sum to avoid conflict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import explode, split


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

# data = [ ('goa', '', 'ap'), ('', 'ap', None), (None, '', 'bglr')]

# columns = ["city1","city2","city3"]

# spark = SparkSession.builder.appName("CitySplit").getOrCreate()

# df = spark.createDataFrame(data,columns)
# df.show()

# df1 = df.withColumn('FirstNotNull',coalesce(df.city1,df.city2,df.city3))
# df1 = df.withColumn('FirstNotNull',coalesce(
#     when(df.city1 == '', None).otherwise(df.city1),
#     when(df.city2 == '',None).otherwise(df.city2),
#     when(df.city3 == '',None).otherwise(df.city3)) )

# df3 = df1.select(df1.FirstNotNull)
# df3.show()

# Q : 
# Student_id | Student_name 
# 1          | Steve
# 2          | David
# 3          | Aryan 

# Student_Id | Subject_name | Marks 
# 1          | pyspark      | 80  
# 1          | sql          | 100
# 2          | sql          | 70
# 2          | pyspark      | 60
# 3          | sql          | 30
# 3          | pyspark      | 20 


# data1 = [(1,"Steve"), (2,"David"), (3,"John"), (4,"Shree"), (5,"Helen")]
# data2 = [(1,"sql",90), (1,"pyspark",100), (2,"sql",70), (2,"pyspark",60), (3,"sql",30), (3,"pyspark",20), (4,"sql",50), (4,"pyspark",50), (5,"sql",45), (5,"pyspark",45)]

# schema1 = ["id", "name"]
# schema2 = ["id", "subject", "marks"]
# spark = SparkSession.builder.appName("student_marks").getOrCreate()

# df1 = spark.createDataFrame(data1, schema1)
# df2 = spark.createDataFrame(data2, schema2)

# df1.show()
# df2.show()

# df_join = df1.join(df2, df1.id == df2.id, how = 'inner').drop(df2.id)

# df_join.show()
# df_per = df_join.groupBy('id','name').agg((sum('marks')/count('*')).alias('Percentage'))
# df_per.show()
# df_per.select('*',
#               when(df_per.Percentage >= 80, 'A')
#               .when((df_per.Percentage >= 60) & (df_per.Percentage < 80), 'B')
#               .when((df_per.Percentage >= 40) & (df_per.Percentage < 60), 'C')
#               .otherwise('D').alias('Grade')).show()


spark = SparkSession.builder.appName("Nthhighest_salary").getOrCreate()

data1 = [(1,"A",1000,"IT"), (1,"B",1500,"IT"), (3,"C",2500,"IT"), (4,"D",3000,"HR"), (5,"E",2000,"HR"), (6,"F",1000,"HR"), (7,"G",4000,"SALES"), (8,"H",4000,"SALES"), (9,"I",1000,"SALES"), (10,"J",2000,"SALES")]

schema1 = ["EmpId", "EmpName", "Salary", "Department"]

df = spark.createDataFrame(data1,schema1)
df.show()


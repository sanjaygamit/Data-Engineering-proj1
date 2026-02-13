import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.functions import col, sum as _sum, desc # Alias sum to avoid conflict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import explode, split
from pyspark.sql.window import *



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
# # df1 = df.select(df.name,explode(split(df.hobbies,',')).alias('HB'))
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

# df1 = df.withColumn('FirstNotNull',coalesce(when(df.city1=='',None).otherwise(df.city1),when(df.city2 == '',None).otherwise(df.city2),when(df.city3 == '',None).otherwise(df.city3)))
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
# df_join = df1.join(df2, df1.id == df2.id, how = 'inner').drop(df2.id).show()

# df_join = df1.join(df2, df1.id == df2.id, how = 'inner').drop(df2.id)

# df_join.show()
# df_per = df_join.groupBy('id','name').agg((sum('marks')/count('*')).alias('Percentage'))
# df_per = df_join.groupBy('id','name').agg((sum('marks')/count('*')).alias('Percentage'))
# df_per.show()
# df_per.select('*',
#               when(df_per.Percentage >= 80, 'A')
#               .when((df_per.Percentage >= 60) & (df_per.Percentage < 80), 'B')
#               .when((df_per.Percentage >= 40) & (df_per.Percentage < 60), 'C')
#               .otherwise('D').alias('Grade')).show()


# spark = SparkSession.builder.appName("Nthhighest_salary").getOrCreate()

# data1 = [(1,"A",1000,"IT"), (1,"B",1500,"IT"), (3,"C",2500,"IT"), (4,"D",3000,"HR"), (5,"E",2000,"HR"), (6,"F",1000,"HR"), (7,"G",4000,"SALES"), (8,"H",4000,"SALES"), (9,"I",1000,"SALES"), (10,"J",2000,"SALES")]

# schema1 = ["EmpId", "EmpName", "Salary", "Department"]

# df = spark.createDataFrame(data1,schema1)
# df.show()

# df_rank = df.select('*',
                    # dense_rank().over(Window.partitionB y(df.Department).orderBy(df.Salary.desc())).alias('Rank'))

# df_rank = df.select('*',
#                     dense_rank().over(Window.partitionBy(df.Department).orderBy(df.Salary.desc())).alias('Rank'))

# df_rank.show()

# df_rank = df.select('*',
#                     dense_rank().over(Window.partitionBy(df.Department).orderBy(df.Salary.desc())))


# df_rank.filter(df_rank.Rank ==1).show()

# df_rank.filter(df_rank.Rank == 1).show()

spark = SparkSession.builder.appName("EmployeeSalary").getOrCreate()

data1 = [(100,"RAJ",None,1,'01-04-23',5000),(200,"Joanne",100,1,'01-04-23',4000),(200,"Joanne",100,1,'13-04-23',4500),(200,"Joanne",100,1,'14-04-23',4020)]

schema1 = ["EmpId","EmpName","MgrId","DeptId","SalaryDate","Salary"]

df_salary = spark.createDataFrame(data1,schema1)
# df_salary.show()
data2 = [(1,"IT"),(2,"HR")]
schema2 = ["DeptId","DeptName"]
df_dept = spark.createDataFrame(data2,schema2)
# df_dept.show()

# df = df_salary.withColumn('newsaldt',to_date('SalaryDate','dd-mm-yy'))

df = df_salary.withColumn('newsaldt',to_date('SalaryDate','dd-mm-yy'))
# df.show()

df1 = df.join(df_dept,df.DeptId == df_dept.DeptId, how = 'inner').drop(df_dept.DeptId)
# # df1 = df.join(df_dept,['DeptId'])
# df1.show()

df2 = df1.alias('a').join(df1.alias('b'),col('a.MgrId') ==col('b.EmpId'),'left').select(col('a.DeptName'),col('b.EmpName').alias('ManagerName'),col('a.EmpName').alias('EmployeeName'),col('a.SalaryDAte'),col('a.newsaldt'),col('a.Salary'))
# df2.show()

# df3 = df2.groupBy('DeptName','ManagerName','EmployeeName',year('NewSaldt').alias('Year'),date_format('NewSaldt','MMM').alias('Month')).sum('Salary').withColumnRenamed('sum(Salary)', 'TotalSalary')

df3 =df2.groupBy('DeptName','ManagerName','EmployeeName',year('newsaldt').alias('Year'),date_format('Newsaldt','MMM').alias('Month')).sum('Salary').withColumnRenamed('sum(Salary)', 'TotalSalary')


# df3.show()

df = spark.read.option('header',True).csv('dbfs:/mnt/input/sales.csv')
df.show()
df.rdd.getNumPartitions()
df.repartition(10)
df.rdd.getNumPartitions()
df1 = df.select(spark_partition_id().alias('partid')).groupBy('partid').count()
df1.show()



df.select(spark_partition_id().alias('partid')).groupBy('partid').count()

# 7. How to process files those are received Before/after specified time? 
# option(modifiedBefore, modifiedAfter)
# modifiedBefore : This attribute can be used to read files that were modified before the specified timestamp. 
# modifiedAfter : This attribute can be used to read files that were modified after the specified timestamp. 

df1 = spark.read.option('header',True).csv(path='dbfs:/mnt/input/sales.csv',modifiedBefore = '2024-01-29 00:00:00')
df1 = spark.read.option('header',True).csv(path='dbfs:/mnt/input/sales.csv',modifiedAfter = '2024-01-30 00:00:00')
# display(df1)
df1.show()

# 8. How to read file from folders & subfolders? how to create Zip/UnZip files? 
# option (recursiveFileLookup, codec)
# 1. recursiveFileLookup : This attribute used to recursively scan the directories to read files(it will read data from sub folders too)
# codec : This attirbute can be used to compress csv or other delimited files using passed compression method. it only workds on csv or normal delimited files. Spark can read gzip without specifying codec but for writting gzip codec must be specified. Compression is the synonym for codec. 

df = spark.raed.option('header',True).option('recursiveFileLookup',True).csv(path='dbfs:/mnt/input/sales.csv')

df.write.option('header',True).mode('overwrite').csv(path='dbfs:/mnt/output/sales.csv.gzip',sep='|',compression = 'gzip',encode='cp1252')

df = spark.read.option('header',True).csv(path='dbfs:/mnt/output/sales.csv.gzip',sep='|',encode='cp1252')


# 9. Delimiter in pyspark | linesep in pyspark | inferSchema in pyspark 
# 1. delimiter : This attribute can be used to specify single/multiple character(s) as a separated for each, column which reading 
# or writing using with option or option function. 

# 2. inferSchema : it will automatically guess the datatypes for each field. If we set this option to Ture, the API will read some sample records from the file to infer the schema. If we want to set this value to false, we must specify a schema explicitly. 

# 3. linesep : This attribute can be used to specify single as a separated for each row while reading or writing a file using either 
# option or options. 

# df = spark.read.option('header',True).load('/mnt/input/sales.csv',format='csv')

# df.write.option('header',True).mode('overwrite').csv(path = '/mnt/input/salesep.csv',sep='|',linesep = '/n')

# df1 = spark.read.option('header',True).option('inferSchema',True).csv(path = '/mnt/input/salesep.csv',sep='|',linesep = '/n')


# 10. How to load only correct records while reading data from file? 
# options mode(PERMISSIVE, DROPMALFORMED, FAILFAST)

# PERMISSIVE : When it meets a corrupted record, puts the malformed string into a field configured by columnNameofCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set a string type field named columnNameofCorruptRecord in an user defined schema.  


# Unity Catalogs 
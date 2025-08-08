import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.functions import col,cast
from pyspark.sql.functions import col, sum as _sum, desc # Alias sum to avoid conflict
from pyspark.sql.types import *
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

# df = spark.read.option('header',True).csv('/Users/sanjaykumarshantilal/Documents/Python/Databricks/Data-Engineering-proj1/retail_db/orders/part-00000', header=True, inferSchema=True)

# d/Users/sanjaykumarshantilal/Documents/Python/Databricks/Data-Engineering-proj1/retail_db/orders_csv') f_csv = df.write.mode('overwrite').parquet(' 
# df_parquet = spark.read.option('header',True).csv('/Users/sanjaykumarshantilal/Documents/Python/Databricks/Data-Engineering-proj1/retail_db/orders_csv/part-00000-2d552c22-2a90-4718-8cec-eaba85347d40-c000.snappy.parquet', header=True, inferSchema=True)

# df_parquet.show()

# df = df.withcolumn('len',len(df.name)).withColumn('age',cast()df.age.cast(IntegerType()))

# df = spark.createDataFrame(data,columns)
# df.show()

# df.printSchema()
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

# df2.withColumn('srn',row_number().over(Window.orderBy(df2.marks))).withColumn('r_srn',rank().over(Window.orderBy(df2.marks))).withColumn('d_srn',dense_rank().over(Window.orderBy(df2.marks))).show()


# df2.withColumn('srn', row_number().over(Window.partitionBy(df2.subject).orderBy(df2.marks.desc()))).show()
# df2.printSchema()

# df3 = df2.withColumn('id',df2.id.cast(StringType()))
# df3.printSchema()

# df2.withColumn('len',length(col("subject"))).withColumn('trim',trim(df2.subject)).show()


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
                    # dense_rank().over(Window.partitionBy(df.Department).orderBy(df.Salary.desc())).alias('Rank'))

# df_rank = df.select('*',
#                     dense_rank().over(Window.partitionBy(df.Department).orderBy(df.Salary.desc())).alias('Rank'))

# df_rank.show()

# df_rank = df.select('*',
#                     dense_rank().over(Window.partitionBy(df.Department).orderBy(df.Salary.desc())))


# df_rank.filter(df_rank.Rank ==1).show()

# df_rank.filter(df_rank.Rank == 1).show()

# spark = (
#     SparkSession
#     .builder
#     .appName("EmployeeSalary")
#     .master("local[*]")
    # .config("spark.sql.shuffle.partitions", "2")  # Set number of shuffle partitions
    # .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # Enable Arrow for better performance
    # .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")  # Fallback to non-Arrow execution if needed
    # .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")  # Set max records per batch for Arrow
    # .config("spark.sql.files.ignoreCorruptFiles", "true")  # Ignore corrupt files
    # .config("spark.sql.files.ignoreMissingFiles", "true")  # Ignore missing files
    # .config("spark.sql.files.maxPartitionBytes", "134217728")  # Set max partition size to 128MB
    # .config("spark.sql.files.openCostInBytes", "4194304")  # Set open cost in bytes to 4MB
    # .config("spark.sql.files.minPartitionNum", "1")  # Set minimum number of partitions to 1
    # .config("spark.sql.files.maxPartitionNum", "100")  # Set maximum number of partitions to 100
    # .config("spark.sql.files.maxRecordsPerFile", "1000000")  # Set max records per file to 1 million

    # .getOrCreate()
    # )

# data1 = [(100,"RAJ",None,1,'01-04-23',50000),(200,"Joanne",100,1,'01-04-23',40000),(200,"Joanne",100,1,'13-04-23',45000),(200,"Joanne",100,1,'14-04-23',40200)]

# schema1 = ["EmpId","EmpName","MgrId","DeptId","SalaryDate","Salary"]
# df_salary = spark.createDataFrame(data1,schema1)
# df_salary = spark.createDataFrame(data1,schema1).where("salary>4000")

# emp_df_csv = df_salary.write.format("csv").save("/Data-Engineering-proj1/emp_df.csv")
# df_salary.show()
# select_cols = df_salary.select(col('EmpId'),expr('EmpName'),df_salary.MgrId,df_salary.Salary)
# select_cols.show()
# df_salary.printSchema()
# df_salary.show()
# Pt = df_salary.rdd.getNumPartitions()
# print(Pt)
# data2 = [(1,"IT"),(2,"HR")]
# schema2 = ["DeptId","DeptName"]
# df_dept = spark.createDataFrame(data2,schema2)
# df_dept.show()

# schema_str = "name string, age int"
# from pyspark.sql.types import _parse_datatype_string 
# schema_spark = _parse_datatype_string(schema_str)

# from pyspark.sql.functions import lit
# # df_salary.show()
# df_salary_tax = df_salary.withColumnRenamed("EmpName","EmployeeName").withColumn("Tax",col("salary") * 0.1).withColumn("ColumnOne",lit(1)).withColumn("ColumnTwo",lit(2))
# df_salary_tax = df_salary.withColumnRenamed("EmpName","EmployeeName")
# df_salary_tax.drop("ColumnTwo").show()
# df_salary_tax.limit(2).show()
# df_dept.printSchema()

# columns = {
#     "tax": col("salary") * 0.2, 
#     "oneNumber": lit(1),
#     "twoNumber": lit(2)
# }

# emp_final = df_salary.withColumns(columns)
# emp_final.show()

# from pyspark.sql.functions import when

# emp_gender = df_salary.withColumn("salary1", when(col("salary") == 50000, 'More then 50000').when(col("salary")<50000, 'Less then 50000').otherwise(None))

# emp_gender.show()

# from pyspark.sql.functions import current_date, current_timestamp
# from pyspark.sql.functions import desc,asc,col

# df_orderby = df_salary.orderBy(col("salary").desc())

# df_orderby.show()



# adding multiple columns 

# df = df_salary.withColumn('newsaldt',to_date('SalaryDate','dd-mm-yy'))

# df = df_salary.withColumn('newsaldt',to_date('SalaryDate','dd-mm-yy'))
# df.show()

# df1 = df.join(df_dept,df.DeptId == df_dept.DeptId, how = 'inner').drop(df_dept.DeptId)
# # df1 = df.join(df_dept,['DeptId'])
# df1.show()

# from pyspark.sql.functions import avg 

# df_avg = df1.groupBy('DeptName').agg(avg("salary").alias("AvgSalary")).where(col("AvgSalary") > 40000)
# df_avg.show()

# df_distinct =df1.select('DeptName').distinct()
# df_distinct.show()

# from pyspark.sql.window import Window

# window_spec = Window.partitionBy(col('DeptName')).orderBy(col('salary').desc())
# max_func = max(col("salary")).over(window_spec)
# d_rank = dense_rank().over(window_spec)

# df_max = df1.withColumn('max_salary',max_func)
# df_rnk = df1.withColumn('Rank',d_rank).withColumn('max_salary',max_func)
# df_max.show()
# df_rnk.show()

# from pyspark.sql.window import Window 
# from pyspark.sql.functions import row_number, desc, col

# window_spec = Window.partitionBy(col('DeptName')).orderBy(col('salary').desc())

# rn = row_number().over(window_spec)
# df_row_num = df1.withColumn("RowNum", rn).withColumn('max_salary',max_func).withColumn('Rank',d_rank)
# df_row_num.show()

# df_count = df1.groupBy('DeptName').agg(sum("salary").alias("TotalSalary"))
# df_count.show()

# df2 = df1.alias('a').join(df1.alias('b'),col('a.MgrId') ==col('b.EmpId'),'left').select(col('a.DeptName'),col('b.EmpName').alias('ManagerName'),col('a.EmpName').alias('EmployeeName'),col('a.SalaryDAte'),col('a.newsaldt'),col('a.Salary'))
# df2.show()

# df3 = df2.groupBy('DeptName','ManagerName','EmployeeName',year('NewSaldt').alias('Year'),date_format('NewSaldt','MMM').alias('Month')).sum('Salary').withColumnRenamed('sum(Salary)', 'TotalSalary')

# df3 =df2.groupBy('DeptName','ManagerName','EmployeeName',year('newsaldt').alias('Year'),date_format('Newsaldt','MMM').alias('Month')).sum('Salary').withColumnRenamed('sum(Salary)', 'TotalSalary')


# df3.show()

# df = spark.read.option('header',True).csv('dbfs:/mnt/input/sales.csv')
# df.show()
# df.rdd.getNumPartitions()
# df.repartition(10)
# df.rdd.getNumPartitions()
# df1 = df.select(spark_partition_id().alias('partid')).groupBy('partid').count()
# df1.show()



# df.select(spark_partition_id().alias('partid')).groupBy('partid').count()

# 7. How to process files those are received Before/after specified time? 
# option(modifiedBefore, modifiedAfter)
# modifiedBefore : This attribute can be used to read files that were modified before the specified timestamp. 
# modifiedAfter : This attribute can be used to read files that were modified after the specified timestamp. 

# df1 = spark.read.option('header',True).csv(path='dbfs:/mnt/input/sales.csv',modifiedBefore = '2024-01-29 00:00:00')
# df1 = spark.read.option('header',True).csv(path='dbfs:/mnt/input/sales.csv',modifiedAfter = '2024-01-30 00:00:00')
# display(df1)
# df1.show()

# 8. How to read file from folders & subfolders? how to create Zip/UnZip files? 
# option (recursiveFileLookup, codec)
# 1. recursiveFileLookup : This attribute used to recursively scan the directories to read files(it will read data from sub folders too)
# codec : This attirbute can be used to compress csv or other delimited files using passed compression method. it only workds on csv or normal delimited files. Spark can read gzip without specifying codec but for writting gzip codec must be specified. Compression is the synonym for codec. 

# df = spark.raed.option('header',True).option('recursiveFileLookup',True).csv(path='dbfs:/mnt/input/sales.csv')

# df.write.option('header',True).mode('overwrite').csv(path='dbfs:/mnt/output/sales.csv.gzip',sep='|',compression = 'gzip',encode='cp1252')

# df = spark.read.option('header',True).csv(path='dbfs:/mnt/output/sales.csv.gzip',sep='|',encode='cp1252')


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
# spark =SparkSession.builder.appName("array_example").getOrCreate()



# df = spark.createDataFrame(data, schema)

# # df.printSchema()
# # df.show()


# df_1 = df.withColumn('New_col',array(df.currentState,df.previousState))
# # df_1.show()

# df_2 = df.withColumn('array_contains',array_contains(df.skills,'Scala'))
# df_2.show()

from pyspark.sql.types import * 

spark = SparkSession.builder.appName('MapExample').getOrCreate()

data = [
    ('India',{"Uttar Pradesh": "Lucknow", "Bihar":"Patna","Madhya Pradesh":"Bhopal","Delhi":"New Delhi"}),
    ('USA',{"California": "Sacramento", "Texas":"Austin","Florida":"Tallahassee","New York":"Albany"})
]

schema = StructType([
    StructField("Country", StringType(), True),
    StructField("States", MapType(StringType(), StringType()), True)
])

data1 = [
    ("James,,Smith",["ADF","Scala","PySpark"],["Pyspark","ADF"],"OH","CA"),
    ("Michael,Rose,",["Java","C++","Python"],["Java","Python"],"NY","NJ"),
    ("Robert,,Williams",[],["CSharp"],"UT","NV")
]
schema1 = StructType([
    StructField("name", StringType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("workprofile", ArrayType(StringType()), True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
])

data2 = [(1,"Steve"), (2,"David"), (3,"John"), (4,"Shree"), (5,"Helen")]
data3 = [(1,"sql",90), (1,"pyspark",100), (2,"sql",70), (2,"pyspark",60), (3,"sql",30), (3,"pyspark",20), (4,"sql",50), (4,"pyspark",50), (5,"sql",45), (5,"pyspark",45)]

schema2 = ["id", "name"]
schema3 = ["id", "subject", "marks"]
# spark = SparkSession.builder.appName("student_marks").getOrCreate()

data4 = [
    ("Banana",1000,"USA"),("Carrot",1500,"USA"),("Beans",1600,"USA"),\
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),\
    ("Carrot",1200,"China"),("Beans",1500,"China"),("Orange",1500,"China"),\
    ("Banana",2000,"Canada"),("Carrot",2000,"Canada"),("Beans",1600,"Maxico")]
schema4 = ["Product","Amount","Country"]

data5 = [
    ("Finance",10),("Marketing",20),("Sales",30),("It",48)
]
schema5 = ["DeptName","DeptId"]

data6 = [
    (1,['A']),(2,['B','L','B']),(3,['K','A','K']),(4,['K']),(3,['B','P'])
       ]
schema6 = ["id","fullname"]

data7 = [(1,('A-424','Noida','India')),(2,('M.15','Unnao','India'))]
schema7 = StructType([
    StructField('AddId', IntegerType(),True),
    StructField('Address', StructType([
        StructField('Add1',StringType(),True),
        StructField('City',StringType(),True),
        StructField('Country',StringType(),True)
    ]))
])

data8 = [
    (1,{'Laptop':'Apple',"Mobile":"OnePlus","Headphone":"boat"}),(2,{'Laptop':'Apple',"Mobile":"OnePlus"})]
schema8 = StructType([
    StructField('EmpId', IntegerType(),True),
    StructField('Items', MapType(StringType(),StringType()),True)
])

data9 = [(1,"Mike","2018","10",30000),
          (2,"John","2010","20",40000),
          (2,"John","2010","20",40000),
          (3,"Jack","2010","10",40000),
          (4,"Charles","2005","60",35000),
          (5,"Guo","2010","40",38000)]

data10 = [(1,"Mike","2018","10",30000),
          (4,"Charles","2005","60",35000),
          (5,"Guo","2010","40",38000)]

schema910 = ['empid','empname','doj','deptid','salary']

data11 = [
    ("Abhishek",85,90,88,"2009-01-13"),
    ("Shyam",92,87,93,"2010-01-13"),
    ("Sandhya",88,89,91,"2009-01-13")
]

from datetime import datetime 

data12 =  [
    (1,"Japan", datetime.strptime("2022-06-21","%Y-%m-%d")),
    (2,"Italy",datetime.strptime("2021-04-21","%Y-%m-%d")),
    (3,"France",datetime.strptime("2020-09-06","%Y-%m-%d"))
]

schema12 = StructType([
    StructField("cid",IntegerType(),True),
    StructField("country",StringType(),True),
    StructField("makeyear",DateType(),True)
])

schema11 = ["name","maths","physics","chemistry","Passyear"]


df = spark.createDataFrame(data, schema)

df1 = spark.createDataFrame(data1, schema1)

df2 = spark.createDataFrame(data2, schema2)

df3 = spark.createDataFrame(data3, schema3)

df4 = spark.createDataFrame(data4, schema4) 

df5 = spark.createDataFrame(data5, schema5)

df6 = spark.createDataFrame(data6,schema6)

df7 = spark.createDataFrame(data7,schema7)

df8 = spark.createDataFrame(data8, schema8)

df9 = spark.createDataFrame(data9, schema910)

df10 = spark.createDataFrame(data10, schema910)

df11 = spark.createDataFrame(data11, schema11)

df12 = spark.createDataFrame(data12, schema12)

df12.show()
# df9.show()

# df7.show()
# df6.show()

# df1.select("name",df1.skills,explode(df1.skills).alias("skill")).show()
# df1.select("name",df1.skills,posexplode(df1.skills)).show()
# df1.select("name",df1.skills,explode_outer(df1.skills).alias('ex_skill')).show()
# df1.select("name",df1.skills,posexplode_outer(df1.skills)).show()

# df.select("Country", explode("States").alias("State", "Capital")).show()

# df.select(df.Country,df.States,explode(df.States).alias("State","Capital")).show()
# df.select(df.Country,df.States, map_keys(df.States).alias('State'),map_values(df.States).alias('Capital'),explode(df.States).alias('State','Capital')).show()

# df4.show()

# country = ['Canada','China','USA']

# df_o = df4.groupBy('Product').pivot('Country').sum('Amount')
# df_o = df4.groupBy('Product').pivot('Country',country).sum('Amount')
# df_o.show()

# df5.show()
# for i in df5.collect():
#     print(f"Department: {i.DeptName}, ID : {i.DeptId}")

# for i in df5:
#     print(i[1])

# print(list(set(df5.schema)))

# allcol = df5.columns + df4.columns
# unique_col = list(set(allcol))
# print(unique_col)

# for i in unique_col:
#     if i in df5.columns:
#         print(f"Column {i} is present in df5")
#     else:
#         print(f"Column {i} is not present in df5")  


# from pyspark.sql.functions import lit 

# for i in unique_col:
#     if i not in df1.columns:
#         df1 = df1.withColumn(i,lit(None))
#     if i not in df2.columns:
#         df2 = df2.withColumn(i,lit(None))



# def schemacompare(df1,df2):
#     allcol = df1.columns + df2.columns
#     unique_col = list(set(allcol))
#     for i in unique_col:
#         if i not in df1.columns:
#             df1 = df1.withColumn(i,lit(None))
#         if i not in df2.columns:
#             df2 = df2.withColumn(i,lit(None))    
#     return df1, df2 


# from pyspark.sql.functions import * 

# array_repeat() # array_position()        

# df6.select(df6.id,explode(df6.fullname).alias('FullName')).show()

# df6.select(df6.id,df6.fullname,array_repeat(df6.fullname,4)).show()

# df6.select(df6.fullname,array_position(df6.fullname,'K'),array_position(df6.fullname,'A')).show()

# df7.printSchema()
# df7.show()

# df7.select('*',df7.Address.Add1.alias('Add1'),df7.Address.City.alias('City'),df7.Address.Country.alias('Country')).show()


# df8.show()

# df8.select('*',explode(df8.Items).alias('Item','Value')).show()

# df9.show()

# df10.show()


# df9.subtract(df10).show()

# df9.exceptAll(df10).show()

# df11.show()

# df11.withColumn('NewPassYear',to_date(df11.Passyear)).show()

# df11_1 = df11.select('*',to_date(df11.Passyear,'yyyy-mm-dd').alias('date'))
# df.select('*',current_timestamp()).show()

# df.select('*',to_timestamp(df.Ne))

# df11_0 = df11_1.withColumn('datediff',datediff(current_date(),df11_1.date)).withColumn('monthdiff',months_between(current_date(),df11_1.date)).withColumn('monthdiff',round(months_between(current_date(),df11_1.date),2)).withColumn('Yeardiff',round(months_between(current_date(),df11_1.date)/lit(2),2)).withColumn('Trunyear',trunc(df11_1.date,'year')).withColumn('TruncMonth',trunc(df11_1.date,'month'))


# df11_0.show()

df12.select('*',add_months(df12.makeyear,3).alias('addmonths'),date_add(df12.makeyear,1).alias('dateadded')).show()
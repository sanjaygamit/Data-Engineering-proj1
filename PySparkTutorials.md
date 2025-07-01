https://www.youtube.com/watch?v=9kwxwCww4zI&list=PLNRxk1s77zfiaFhN8RSWYY3_bi60x0uUM&index=3

dbutils.fs.mounts()
dbutils.fs.ls('/mnt/input/sales/')

df_sales = spark.read.option('header',True).csv('/mnt/input/sales/sales.csv')
display(df_sales)

df_sales.write.csv('mnt/output/sales20230409.csv')

###### Write modes in PySpark

1. Overwrite
2. Append
3. Ignore
4. Error

df_sales = spark.read.option('header',True).csv('/mnt/input/sales/sales.csv')

df_sales.count() # shows the total count

df_sales.write.csv('/mnt/output/Sales')

df_sales.write.option('header',True).mode('overwrite').csv('/mnt/output/Sales')

df = spark.read.option('header',True).csv('/mnt/output/sales')

df.count() # 799

df_sales.write.option('header',True).mode('append').csv('/mnt/output/Sales')

df.count() # 1598

df_sales.write.option('header',True).mode('ignore').csv('/mnt/output/Sales')

df_sales.write.option('header',True).mode('error').csv('/mnt/output/Sales')

## Write data to parquet file | read data from parquet file.

1. Read Data from CSV file & load into dataframe.
2. Load Data From dataframe to parquet file.
3. Read data from parquet file and load it to dataframe.

dbutils.fs.mounts()

df_csv = spark.read.option('header',True).csv('/mnt/input/sales/Sales.csv')

display(df_csv)  
df_csv.count()

df_csv.write.option('header',True).parquet('/mnt/output/Sales')

df_par = spark.read.option('header',True).parquet('/mnt/output/Sales')

display(df_par)
df_par.count()

## Datatypes

    - IntegerType
    - LongType
    - FloatType
    - DoubleType
    - StringType
    - BooleanType
    - DateType
    - TimestampType

## How to define the schema in pyspark | structtype & structfield in pyspark

---------------------- SQL -------------------------
CREATE TABLE table_name (column_name datatype [constraints], column_name datatype [constraints]);

---

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType

schema = StructType([StructField("id", IntegerType(),True),
StructField("name", StringType(),False),
StructField("age", IntegerType(), True),
StructField("salary", DecimalType(), True)])

df = spark.createDataFrame([],schema)

data = [(1,'susheel',30,4000),(2,'prabhu',32,5000)]
schema = StructType([
StructField("id", IntegerType(), True),
StructField("name", StringType(), False),
StructField("age", IntegerType(), True),
StructField("salary", DecimalType(), True)
])

df = spark.createDataFrame(data,schema)
display(df)

## How to read csv file with schema opotion.

df = spark.read.option('header',True).csv('/mnt/input/sales/sales.csv')

df.schema

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType

schema = StructType([
StructField("SOID", IntegerType(), False),
StructField("SODate", DateType(), False),
StructField("ItemCode", StringType(), False),
StructField("ItemName", StringType(), False),
StructField("Qty", IntegerType(), False),
StructField("Value", IntegerType(),False)
])

df_new = spark.read.option('header',True).schema(schema).csv('/mnt/input/sales/sales.csv')

# 9. Read Single line, Multiline & Complex type Json file.

    1. Single line Json file.
    2. Multiline Json file.
    3. Single line Complex(Nested) Json file.
    4. Multiline Complex(Nested) Json file.

    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    AddressSchema = StructType([ StructField("City",StringType(),False),
                                 StructField("State",StringType(),False)
                               ])

    CustSchema = StructType([
                            StructField("name", StringType(),False),
                            StructField("age",IntegerType(),False),
                            StructField("Address",AddressSchema)
                           ])

df_nsl = spark.read.option("singleLine",True).schema(custSchema).json('/mnt/input/CustomerNSL.json')

df_nsl = spark.read.option("multiLine",True).schema(custSchema).json('/mnt/input/CustomerNML.json')

# 11. filter and like operator

df = spark.read.option('header',True).csv('/mnt/input/Sales/Sales.csv')

display(df)

df = df.withColumnRenamed('Item Name','ItemName')

display(df)

df.filter(df.ItemName=="Total income")

import pyspark.sql.functions import \*

#df1 = df.filter(col("ItemName") == "Total income")

#display(df1)

#df1 = df.filter(col("ItemName").like("%Total%"))

#display(df1)

df.filter(((df.ItemName == "Total income") & (df.Qty == 5)) | (df.SOID <= 63 ))

# 12 STARTSWITH & ENDSWITH in PySpark

from pyspark.sql.functions import \*

df1 = df.filter(df.ItemName.startswith('Total'))
df2 = df.filter(df.ItemName.endswith('income'))
df3 = df.filter(df.ItemName.contains('operating'))

df.filter(df.ItemName.isin('Total income','Total expenditure','Total profit'))
df.filter(~df.ItemName.isin('Total income','Total expenditure','Total profit'))

# 13. Select in Pyspark | alias in Pyspark

display(df.select('SOID','SODate','ItemName','Qty','Value))

display(df.select('\*'))

display(df.select('_',(df.qty _ df.value).alias('amount')))

# 14. When(case statement)

df.select('\*', when(df.Itemname == 'Total income', df.qty+100)
.when(df.Itemname == 'Sales government funding and subsidied',df.qty + 200)
.when(df.ItemName == 'Interest dividends and donations',df.qty+300)
.otherwise(1000).alias('NewQty)
)

# 15 NULL handling in PySpark DataFrame

    1. isnull
    2. isnot null
    3. Realtime use of isnull & isnot null

display(df.select('\*',df.Itemname.isnull().alias('ItemName )))

display(df.select('\*',df.Itemname.isNotNull().alias('ItemName )))

display(df.filter(df.ItemName.isnull()) )

df.select('\*',when(df.ItemName.isnull(),'NA').otherwise(df.Itemname).alias('ItemName'))

# 16. fill() & fillna() functions in PySpark | How to replace null values in pyspark.

df.na.fill('NA')
display(df1)

df.na.fill('NA',['ItemName'])

df.na.fill('NA',['ItemName','Item Code'])

df.na.fillna('NA',['ItemName','Item Code'])

# 17. GroupBy function

    1. Group By
    2. Aggregate functions
    3. Agg function

df1 = df.groupBy('ItemName').count()
df1 = df.groupBy('ItemName').max('Qty')
df1 = df.groupBy('ItemName').min('Qty')
df1 = df.groupBy('ItemName').sum('Qty')
df1 = df.groupBy('ItemName').avg('Qty')
df1 = df.groupBy('ItemName').sum('Qty','value')
df1 = df.groupBy('ItemCode','ItemName').sum('Qty','value')
df1 = df.groupBy('ItemName').agg(sum('Qty'),avg('value'))

# 18 count and countDistinct function in PySpark

    1. count
    2. count with groupBy
    3. distinct() with count()
    4. countDistinct()

df.count()

df1 = df.groupBy('ItemName').count()

df.distinct().count() -- Total distinct count on this dataframe.

from pyspark.sql.functions import countDistinct

df1 =df.select(countDistinct('ItemName'))
df1 =df.select(countDistinct('ItemName','SOID'))
display(df1)

# 19. OrderBy in pyspark | Sort in pyspark | Difference between orderby and sort.

    1. sort()
    2. orderby()

    We can use either sort() or orderBy() function in PySpark to sort DF by ascending and descending order.

df1 = df.sort('qty')
df1 = df.orderBy('qty')

df1 = df.orderBy(df.Qty.desc())
df1 = df.orderBy(col('qty').desc(),df.value.asc())
display

# 20 Distinct and dropduplicate in PySpark | How to remove duplicate

    1. distinct()
    2. dropduplicate()
    3. distinct() vs dropDuplicates()

    difference is distinct() performs on all columns whereas dropDuplicates() is used on selected columns.

df1 = df.distinct()
display(df1)

df1 = df.dropDuplicates(['ItemName'])
df1 = df.dropDuplicates(['ItemName']).select('ItemName)'
display(df1)

# 21 Join

    1. Inner
    2. outer(outer, full,fullouter,full_outer)
    3. left(left,leftouter,left_outer)
    4. Right(right,rightouter,right_outer)
    5. Anti(anti,leftanti,left_anti)
    6. Semi(semi,leftsemi,left_semi)

data = [(1,"Susheel","10","M",4000),
(2,"Bhallar","20","M",3000),
(3,"Prabhu","10","M",4000),
(4,"Sandhya","10","F",2000),
(5,"Vaibhav","40","M",3500),
(6,"Amrita","50","F",2500)]

schema = ["empid","empname","deptid","gender","salary"]

empDF = spark.createDataFrame(data,schema)

dept = [("finance",10),
("marketing",20),
("sales",30),
("it",40)]

deptschema = ["deptname","deptid"]

deptDF = spark.createDataFrame(dept,deptschea)

df = empDF.join(deptDF, empDF.deptid == deptDF.deptid ,"inner")
df = empDF.join(deptDF, empDF.deptid == deptDF.deptid ,"outer")
df = empDF.join(deptDF, empDF.deptid == deptDF.deptid ,"fullouter")
df = empDF.join(deptDF, empDF.deptid == deptDF.deptid ,"full_outer")
df = empDF.join(deptDF, empDF.deptid == deptDF.deptid ,"full")
df = empDF.join(deptDF, empDF.deptid == deptDF.deptid ,"anti")
df = empDF.join(deptDF, empDF.deptid == deptDF.deptid ,"semi") -- it will work as inner join.

display(df)

# 22. concat and concat_ws in pyspark | concat vs concat_ws in pyspark

    1. Concat
    2. Concat_ws
    3. Concat vs Concat_ws

from pyspark.sql.functions import concat,col,lit,concat_ws,regexp_replace

df1 = df.select(concat(df.firstname,lit(','),df.middlename,lit(','),df.lastname).alias("fullname))
display(df1)

df1 = df.select(concat_ws(',',df.firstname,df.middlename,df.lastname).alias("fullname"))
display(df1)

df1.select(regexp_replace(df1.fullname,"||","|").alias("fullnamenew"),df.gender)

# 23. Split()

    Pyspark SQL provides split() function to convert delimiter separated string to an Array column on DF. This can be done by splitting a string column based on delimiter like pipeline, comma, space etc.

df1 = df.withColumn('year',split(df['dob'],'-').getitem(0))\
.withColumn('month',split(df['dob'],'-').getitem(1))

split = split(df['dob'],'-')

df1 = df.select("firstname","middlename","lastneme,split.getItem(0).alias('year'),split.getItem(1).alias('month'))
display(df1)

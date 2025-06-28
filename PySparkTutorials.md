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

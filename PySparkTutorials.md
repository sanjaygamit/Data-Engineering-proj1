# https://www.youtube.com/watch?v=9kwxwCww4zI&list=PLNRxk1s77zfiaFhN8RSWYY3_bi60x0uUM&index=3

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

spark.read.option('header',True).csv('/mnt/input/sales/sales.csv')

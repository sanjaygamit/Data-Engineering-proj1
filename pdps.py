#USING PANDAS TO READ CSV FILES
import glob
import pandas as pd

files = glob.glob('data/nyse_all/nyse_data/*')
# rec_count = 0
# for file in files:
#     df =  pd.read_csv(file, names = ['stock_id','trans_date','open_price','low_price','high_price''close_price','volume','close_price_adj','stock_name'])
#     rec_count += df.shape[0]

# print(rec_count)

# USING DASK 

# import dask.dataframe as dd
# df =  dd.read_csv(files, names = ['stock_id','trans_date','open_price','low_price','high_price''close_price','volume','close_price_adj','stock_name'])

# print(df.shape[0].compute())

#USING SPARK
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('NYSE COUNT').master('local').getOrCreate()
df = spark.read.csv('data/nyse_all/nyse_data/*', schema = '''stock_id STRING, trans_date STRING, open_price FLOAT, low_price FLOAT, high_price FLOAT, close_price FLOAT, volume BIGINT''')

print(df.count())
##
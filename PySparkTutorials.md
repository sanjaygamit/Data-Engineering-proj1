# https://www.youtube.com/watch?v=9kwxwCww4zI&list=PLNRxk1s77zfiaFhN8RSWYY3_bi60x0uUM&index=3

dbutils.fs.mounts()
dbutils.fs.ls('/mnt/input/sales/')

df_sales = spark.read.option('header',True).csv('/mnt/input/sales/sales.csv')
display(df_sales)

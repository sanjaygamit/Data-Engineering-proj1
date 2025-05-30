from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RetailDataPipeline").getOrCreate()

data = [(1,'2024-10-1'),
        (1,'2024-10-2'),
        (1,'2024-10-4'),
        (2,'2024-10-3'),
        (2,'2024-10-5'),
        (3,'2024-10-1'),
        (3,'2024-10-2'),
        (3,'2024-10-3'),]

df = spark.createDataFrame(data,['customer_id','order_date'])
#df.show()

# Define a window specification to partition by customer_id and order by order_date
window_spec = Window.partitionBy("Customer_id").orderBy("order_date")

# Use lag to get the previous order date

df_lag = df.withColumn("prev_order_date", lag("order_date").over(window_spec))
# df_lag.show()
df_diff = df_lag.withColumn("data_diff",datediff("order_date","prev_order_date"))
# df_diff.show()
consecutive_orders = df_diff.filter(col("data_diff") == 1)
consecutive_orders.show()
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc # Alias sum to avoid conflict
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Configuration
SCHEMA_FILE_PATH = 'data-engineering-proj1/retail_db/schemas.json'
# For Databricks, you might use DBFS paths, e.g., "/dbfs/mnt/your_data/retail_db"
BASE_DATA_PATH = 'Data-Engineering-proj1/retail_db' 

def get_column_names(schemas_config, ds_name, sorting_key='column_position'):
    """Gets sorted column names from the schemas configuration."""
    column_details = schemas_config[ds_name]
    columns = sorted(column_details, key=lambda c: c[sorting_key])
    return [c['column_name'] for c in columns]

def generate_spark_schema(ds_name, column_names_list):
    """
    Generates a PySpark StructType schema based on dataset name and column names.
    Infers types based on common naming conventions (e.g., '_id', '_price').
    """
    fields = []
    for name in column_names_list:
        field_type = StringType()  # Default to StringType

        if name.endswith('_id'):
            field_type = IntegerType()
        elif name.endswith('_quantity'):
            field_type = IntegerType()
        elif name.endswith('_price') or name.endswith('_subtotal') or name.endswith('amount'):
            field_type = FloatType()
        # Dates are read as StringType by default with this heuristic;
        # cast to TimestampType or DateType later if specific date operations are needed.
        # Example: .withColumn(name, to_timestamp(col(name), "yyyy-MM-dd HH:mm:ss.S"))

        # Specific overrides based on known column properties for higher accuracy
        if ds_name == "products" and name == "product_price":
            field_type = FloatType()
        if ds_name == "order_items":
            if name == "order_item_quantity": field_type = IntegerType()
            if name == "order_item_subtotal": field_type = FloatType()
            # Assuming 'order_item_product_price' might exist and should be Float
            if name == "order_item_product_price": field_type = FloatType()
        
        # Ensure primary key like columns are integers if not caught by suffix
        if name in ("department_id", "category_id", "category_department_id", 
                     "product_id", "order_id", "customer_id", "order_item_order_id", 
                     "order_item_product_id"):
            field_type = IntegerType()

        fields.append(StructField(name, field_type, True))
    return StructType(fields)

def get_file_data_spark(spark_session, base_path, ds_name, schemas_config):
    """Reads CSV data into a Spark DataFrame using a generated schema."""
    column_names = get_column_names(schemas_config, ds_name)
    spark_schema = generate_spark_schema(ds_name, column_names)
    
    # Spark can read all part-files in a directory matching the pattern
    file_path_pattern = f"{base_path}/{ds_name}/part-*"
    
    df = spark_session.read.format("csv") \
        .schema(spark_schema) \
        .option("header", "false") \
        .load(file_path_pattern)
    return df

def main():
    # Initialize SparkSession
    spark = SparkSession.builder.appName("RetailDataPipeline").getOrCreate()

    # Load schemas
    with open(SCHEMA_FILE_PATH, 'r') as f:
        schemas_config = json.load(f)

    # Load DataFrames
    print("Loading data into Spark DataFrames...")
    df_dept = get_file_data_spark(spark, BASE_DATA_PATH, 'departments', schemas_config)
    df_cate = get_file_data_spark(spark, BASE_DATA_PATH, 'categories', schemas_config)
    df_ord = get_file_data_spark(spark, BASE_DATA_PATH, 'orders', schemas_config)
    df_prod = get_file_data_spark(spark, BASE_DATA_PATH, 'products', schemas_config)
    df_cust = get_file_data_spark(spark, BASE_DATA_PATH, 'customers', schemas_config)
    df_item = get_file_data_spark(spark, BASE_DATA_PATH, 'order_items', schemas_config)

    print("\nOrders DataFrame (first 5 rows):")
    df_ord.show(5)

    # Join categories and departments
    print("\nJoining categories and departments...")
    df_dept_cate_joined = df_cate.join(
        df_dept,
        df_cate.category_department_id == df_dept.department_id,
        how='inner'
    )
    print("\nJoined Categories and Departments DataFrame (first 5 rows):")
    df_dept_cate_joined.show(5)

    # --- Query to find the highest selling item ---
    print("\nFinding the highest selling item...")

    # 1. Join df_item (order_items) with df_prod (products)
    merged_items_products = df_item.join(
        df_prod,
        df_item.order_item_product_id == df_prod.product_id,
        how='inner'
    )
    print("\nJoined Order Items and Products DataFrame (first 5 rows):")
    merged_items_products.select("order_item_id", "product_name", "order_item_quantity").show(5)

    # 2. Group by product_name and sum the order_item_quantity
    product_sales = merged_items_products.groupBy("product_name") \
        .agg(_sum("order_item_quantity").alias("total_quantity_sold"))

    # 3. Sort by total_quantity_sold in descending order
    highest_selling_products = product_sales.orderBy(desc("total_quantity_sold"))

    print("\nTotal quantity sold per product (Top 5):")
    highest_selling_products.show(5)

    # 4. Get the highest selling item
    highest_selling_item = highest_selling_products.first()

    if highest_selling_item:
        print("\nHighest selling item:")
        print(f"Product Name: {highest_selling_item['product_name']}, Total Quantity Sold: {highest_selling_item['total_quantity_sold']}")
    else:
        print("\nNo sales data found to determine the highest selling item.")

    # Stop the SparkSession
    print("\nStopping SparkSession.")
    spark.stop()

if __name__ == "__main__":
    main()
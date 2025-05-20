import json
import pandas as pd
import glob
import re
file_path = 'data-engineering-proj1/retail_db/schemas.json' 
schemas = json.load(open(file_path, 'r'))   
def get_column_names(schemas, ds_name, sorting_key = 'column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key = lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]
    # print(column_details)


def get_file_data(ds_name):
    src_file_names = glob.glob('Data-Engineering-proj1/retail_db/*/part-*', recursive=True)
    data_path_filter = filter( lambda d1: d1.split('/')[-2]==ds_name, src_file_names)
    customers_col = get_column_names(schemas, ds_name, 'column_position')
    df=pd.read_csv(list(data_path_filter)[0], header=None , names=customers_col)
    return df



df_dept = get_file_data('departments')
df_cate = get_file_data('categories') 
df_ord = get_file_data('orders')
df_prod = get_file_data('products')
df_cust = get_file_data('customers')
df_item = get_file_data('order_items')

print (df_ord.head(5))

df_dept_cate_joined = pd.merge(df_cate, df_dept, left_on='category_department_id', right_on='department_id', how='inner')

# print ("\nJoined DataFrame (Categories and Departments) head:")
# print (df_dept_cate_joined.head())
# --- Query to find the highest selling item ---

# 1. Join df_item (order_items) with df_prod (products)
# Assuming 'order_item_product_id' in df_item links to 'product_id' in df_prod
merged_items_products = pd.merge(
    df_item,
    df_prod,
    left_on='order_item_product_id',
    right_on='product_id',
    how='inner'
)

print("\nJoined Order Items and Products DataFrame head:")
print(merged_items_products.head())

# 2. Group by product_name and sum the order_item_quantity
# We'll select relevant columns for clarity before grouping
product_sales = merged_items_products.groupby('product_name')['order_item_quantity'].sum()

# Convert the series to a DataFrame for better display and sorting
product_sales_df = product_sales.reset_index(name='total_quantity_sold')

# 3. Sort by total_quantity_sold in descending order
highest_selling_products = product_sales_df.sort_values(by='total_quantity_sold', ascending=False)

print("\nTotal quantity sold per product (Top 5):")
print(highest_selling_products.head())

# 4. Get the highest selling item
if not highest_selling_products.empty:
    highest_selling_item = highest_selling_products.iloc[0]
    print("\nHighest selling item:")
    print(highest_selling_item)
else:
    print("\nNo sales data found to determine the highest selling item.")
import pandas as pd
import json
import glob
import re

file_path = 'data-engineering-proj1/retail_db/schemas.json'
schemas = json.load(open(file_path, 'r'))
def get_column_names(schemas, ds_name, sorting_key = 'column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key = lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

# departments_col = get_column_names(schemas, 'departments', 'column_position')
# categories_col = get_column_names(schemas, 'categories', 'column_position')
# orders_col = get_column_names(schemas, 'orders', 'column_position')
# products_col = get_column_names(schemas, 'products', 'column_position')
customers_col = get_column_names(schemas, 'customers', 'column_position')
# order_items_col = get_column_names(schemas, 'order_items', 'column_position')

# print(order_items_col)

# categories = pd.read_csv('Data-Engineering-proj1/retail_db/categories/part-00000', header=None, names=categories_col)
customers = pd.read_csv('Data-Engineering-proj1/retail_db/customers/part-00000', header=None, names=customers_col)
# departments = pd.read_csv('Data-Engineering-proj1/retail_db/departments/part-00000', header=None, names=departments_col)
# order_items = pd.read_csv('Data-Engineering-proj1/retail_db/order_items/part-00000', header=None, names=order_items_col)
# orders = pd.read_csv('Data-Engineering-proj1/retail_db/orders/part-00000', header=None, names=orders_col)
# products = pd.read_csv('Data-Engineering-proj1/retail_db/products/part-00000', header=None, names=products_col)

# print(orders['order_status'])
# print(orders['order_status'].unique())
# print(orders.query('order_status == "COMPLETE" and order_date > "2014-01-01 00:00:00.0"'))
# print(orders.groupby('order_status')['order_id'].agg(order_count='count'))
# orders['orders_month'] = orders.apply( lambda order: order.order_date[:7], axis = 1)
# print(orders.groupby(['orders_month','order_status'])['order_id'].agg(order_count='count'))

# print(categories)
# print(customers)
# print(departments)
# print(order_items)
# print(orders)
# print(products)

# Ensure the data types of the indices match
# customers['customer_id'] = customers['customer_id'].astype(str)
# orders['order_customer_id'] = orders['order_customer_id'].astype(str)

# customers.set_index('customer_id')
# customers = customers.set_index('customer_id')
# orders = orders.set_index('order_customer_id')
# customer_orders = customers.join(orders, how='inner')
# customer_orders = customers.join(orders, how='inner').reset_index(names = 'customer_id').groupby('customer_id')['customer_id'].agg(order_count='count') 

# print(customers.index.unique())
# print(orders.index.unique())
# print(customer_orders)
# print(orders.sort_values(['order_customer_id','order_date']))
# print(orders.sort_values(['order_customer_id','order_date'],ascending=False))
# print(customer_orders.shape)
# print(orders)
# print(customer_orders.query('customer_id >=10'))
# import os 
# os.makedirs('data/retail_db/orders_json/part-00000', exist_ok=True) 
# orders.to_json('data/retail_db/orders_json/part-00000.json', orient='records', lines=True)


# file_path = glob.glob('Data-Engineering-proj1/retail_db/**', recursive=True)
# file_path = glob.glob('Data-Engineering-proj1/retail_db/*/*', recursive=True)
src_file_names = glob.glob('Data-Engineering-proj1/retail_db/*/part-*', recursive=True)
# print([file for file in src_file_names])
# print(src_file_names)
# for file in src_file_names:
#     print(re.split('/',file))
    # print(re.split('/'),file)


data_path_filter = filter( lambda d1: d1.split('/')[2]=='customers', src_file_names)
print(type(data_path_filter))
df=pd.read_csv(list(data_path_filter)[0], header=None , names=customers_col)
print(df.head(5))
# for file_name in src_file_names: 
#     df = pd.read_csv(file_name, header=None)
#     # print(f'Shape of {file_name} is {df.shape}')
#     print(df.head(5))



# schemas = json.load(open('Data-Engineering-proj1/retail_db/schemas.json', 'r'))
# print(schemas)  


# filter(lambda order :order.split(',')[3]=='COMPLETE, orders)



import pandas as pd
import json

file_path = 'data/retail_db/schemas.json'
schemas = json.load(open(file_path, 'r'))
def get_column_names(schemas, ds_name, sorting_key = 'column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key = lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

departments_col = get_column_names(schemas, 'departments', 'column_position')
categories_col = get_column_names(schemas, 'categories', 'column_position')
orders_col = get_column_names(schemas, 'orders', 'column_position')
products_col = get_column_names(schemas, 'products', 'column_position')
customers_col = get_column_names(schemas, 'customers', 'column_position')
order_items_col = get_column_names(schemas, 'order_items', 'column_position')

categories = pd.read_csv('data/retail_db/categories/part-00000', header=None, names=categories_col)
customers = pd.read_csv('data/retail_db/customers/part-00000', header=None, names=customers_col)
departments = pd.read_csv('data/retail_db/departments/part-00000', header=None, names=departments_col)
order_items = pd.read_csv('data/retail_db/order_items/part-00000', header=None, names=order_items_col)
orders = pd.read_csv('data/retail_db/orders/part-00000', header=None, names=orders_col)
products = pd.read_csv('data/retail_db/products/part-00000', header=None, names=products_col)

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

import glob

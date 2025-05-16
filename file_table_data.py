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
    data_path_filter = filter( lambda d1: d1.split('/')[2]==ds_name, src_file_names)
    customers_col = get_column_names(schemas, ds_name, 'column_position')
    df=pd.read_csv(list(data_path_filter)[0], header=None , names=customers_col)
    print(df.head(5))



# get_file_data('departments')
# get_file_data('categories') 
# get_file_data('orders')
# get_file_data('products')
# get_file_data('customers')
# get_file_data('order_items')


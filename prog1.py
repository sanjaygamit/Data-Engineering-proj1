# import os
# orders_file = open('data-engineering-proj1/retail_db/orders/part-00000', 'r')
# print(orders_file.read())
# orders_str = orders_file.read()
# orders = orders_str.splitlines()
# print(orders)
# print(len(orders))
# orders_list = orders[:10]
# print(orders_list)
# print(orders[-10:])
# orders_file.close()
# print(orders[0])
# print(orders_list.sort())
# print(len(orders_list))

# l = [1,2,3,4]  # [1,3,6,10]
# n = int(input("Enter a number: "))  

# def sum_d(n):
#     return (n * (n+1))/2

# print(sum_d(n))
# print([sum_d(n) for n in range(n)])

# sum_l = lambda n : (n * (n+1))/2 

# print(sum_l(n))
# print([sum_l(n) for n in range(n)])
############ fibonacci series #############
# def fibonaci(i):
#     if i == 0:
#         return 0 
#     elif i == 1:
#         return 1
#     else: 
#         return fibonaci(i-1)+fibonaci(i-2)

# n = int(input("Enter the number of terms: "))
# for x in range(n):
#     print(fibonaci(x))   

######## find out common letters ##########

# def common_letters():
#     str1 = input("Enter first string: ")
#     str2 = input("Enter second string: ")
#     common = set(str1) & set(str2)
#     print("Common letters are: ", common)

# common_letters()
### common letters in strings ######
# def common_letters():
#     a = input("Enter first string:")
#     b = input("Enter second string:")
#     c = set(a) & set(b)    
# common_letters()

# Write a python program to Count the frequency of words appearing a string. 

# Sheena loves eating apple and mango. Her sister also loves eating apple and mango. 
# def count_frequency():
#     str = input("Enter a string: ")
#     li = str.split()
#     d = {}
#     for i in li:
#         if i not in d.keys():
#             d[i] = 0
#         d[i] = d[i] + 1
#     print(d)        

# count_frequency()


# write a python program to convert two lists into a dictionary. 

# def list_to_dict():
#     keys = [1,2,3]
#     values = ["one", "two", "three"]
#     result = dict(zip(keys,values))
#     print(result)

# list_to_dict()    

# def dict_to_touple():
#     d = {1: 'one', 2: 'two', 3: 'three'}
#     for x in d.items():
#         print(x)

# dict_to_touple()

# orders = ['1,2013-07-25 00:00:00.0,11599,CLOSED',
# '2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT',
# '3,2013-07-25 00:00:00.0,12111,COMPLETE',
# '4,2013-07-25 00:00:00.0,8827,CLOSED',
# '5,2013-07-25 00:00:00.0,11318,COMPLETE',
# '6,2013-07-25 00:00:00.0,7130,COMPLETE',
# '7,2013-07-25 00:00:00.0,4530,COMPLETE',
# '8,2013-07-25 00:00:00.0,2911,PROCESSING',
# '9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT',
# '10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT']


# print(orders)  
# order = orders[0]
# print(order.split(',')[0] + ' ' + order.split(',')[1] + ' ' + order.split(',')[2] + ' ' + order.split(',')[3]) 

# print(orders)

# for i in list(filter(lambda order : order.split(',')[3] in ('PROCESSING','PENDING_PAYMENT') ,orders)):
#     print(i)

# for i in list(map(lambda order : order.split(',')[3]  ,orders)):
#     print(i)
    
# for i in sorted(orders,key=lambda order: int(order.split(',')[2])):
#     print(i)

# import json
# order_jd = '{\"order_id\": 1, \"order_date\": "2013-07-25"}'

# # print(order_jd)

# order = json.loads(order_jd)['order_id']
# print(type(order))
import json

file_path = 'data-engineering-proj1/retail_db/schemas.json'

# f_obj = open(file_path, 'r')
# print(f_obj.read())
# f_obj.close()
# f_obj = open(file_path, 'r')

schemas = json.load(open(file_path, 'r'))
# print(schemas)
# print(type(schemas))
# schemas.close()

# print(schemas.keys())
# print(schemas.get('orders'))

# for i in range(1,500000):
#     print(i)

# print(schemas['orders'])

# get_column_names(schemas, 'orders', 'column_name')
# ['order_customer_id', 'order_date','order_id',  'order_status']

# column_details = schemas['orders']
# print(column_details)
# for x in range(2):
#     print('#')

# print([ col['column_name'] for col in column_details])

# print(list(map(lambda col: col['column_name'],column_details)))
# for x in range(2):
#     print('#')

# sorting_key = input('Enter sorting key: ')

# print(sorted(column_details, key = lambda col: col[sorting_key]))


def get_column_names(schemas, ds_name, sorting_key = 'column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key = lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

departments_col = get_column_names(schemas, 'departments', 'column_name')
categories_col = get_column_names(schemas, 'categories', 'column_name')
orders_col = get_column_names(schemas, 'orders', 'column_name')
products_col = get_column_names(schemas, 'products', 'column_name')
customers_col = get_column_names(schemas, 'customers', 'column_name')
order_items_col = get_column_names(schemas, 'order_items', 'column_name')


print(order_items_col)

# https://github.com/sanjaygamit/Data-Engineering-proj1.git

##
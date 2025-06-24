import logging

logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w" format = "%(asctime)s - %(levelname)s - %(message)s")




logging.debug("debug")
logging.info("info")
logging.warning("warning")
logging.error("error")
logging.critical("critical")

import json
import os


def process_files(ds_names = None):
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    tgt_base_dir = os.environ.get('TGT_BASE_DIR')
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        print(f'Processing dataset: {ds_name}')
    

# find the pairs with given sum value of an array. 

# def find_pairs_of_sum(l,target_sum):
#     l.sort()
#     left = 0
#     right = len(l) - 1
#     while (left <= right):
#         if (l[left] + l[right] > target_sum):
#             right -= 1
#         elif (l[left] + l[right] < target_sum):
#             left +=1
#         elif (l[left] + l[right] == target_sum):
#             print("Values of pair are", l[left], "&", l[right])
#             right -= 1
#             left += 1


# l = [5, 7, 4, 3, 9, 8, 19, 21,18,22]
# target_sum = 40
# find_pairs_of_sum(l, target_sum)                    
        
# Height of Binary Tree(Max Depth) - Recursion    


# Height = 1 + No of edges longest from root to leaf


# class Node: 
#     def __init__(self,key):
#         self.data = key
#         self.left = None
#         self.right = None 

# def min_height_binary(root):
#     if(root == None):
#         return 0
#     else:
#         ldepth = min_height_binary(root.left)
#         rdepth = min_height_binary(root.right)
#         if(ldepth > rdepth):
#             return (rdepth + 1)
#         else:
#             return (ldepth + 1)


# root = Node(1)
# root.left = Node(2)
# root.right = Node(3)
# root.left.left = Node(4)
# root.left.right = Node(5)
# root.left.left.right = Node(7)
# root.right.right = Node(6)

# print("Minimum height of the binary tree is", min_height_binary(root))

# 	select 
#         substr(s,rownum,1) output1,
#         substr(s,rownum*-1,1) output2,
#         substr(s,1,rownum) output3,
#         substr(s,rownum) output4, 
#         rpad(' ',rownum,' ')||substr(s,rownum) output5,
#         rpad(' ',length(s)+1-rownum,' ')||substr(s,rownum) output6,
#         rpad(' ',rownum,' ')||substr(s,rownum) output5,
# from dual, (select 'WELCOME' S from dual) 
# connect by level <= length('S');
	
    
#   extract name from email id; 

with d as (select 'sanju.gamit@gmai.com' m from dual),
  ds as (    select * from d;   
select substr(m,1,instr(m,'@')-1) n,  
substr(m,instr(m,'@')+1) d 
from d ) 
select 
select substr(n,1,instr(n,'.',1,1))
from ds;

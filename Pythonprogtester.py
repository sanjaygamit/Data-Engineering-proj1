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

def find_pairs_of_sum(l,target_sum):
    l.sort()
    left = 0
    right = len(l) - 1
    while (left <= right):
        if (l[left] + l[right] > target_sum):
            right -= 1
        elif (l[left] + l[right] < target_sum):
            left +=1
        elif (l[left] + l[right] == target_sum):
            print("Values of pair are", l[left], "&", l[right])
            right -= 1
            left += 1


l = [5, 7, 4, 3, 9, 8, 19, 21,18,22]
target_sum = 40
find_pairs_of_sum(l, target_sum)                    
        
    


	 
	
	
	
    
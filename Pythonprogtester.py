# import logging

# logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w" format = "%(asctime)s - %(levelname)s - %(message)s")




# logging.debug("debug")
# logging.info("info")
# logging.warning("warning")
# logging.error("error")
# logging.critical("critical")

# import json
# import os


# def process_files(ds_names = None):
#     src_base_dir = os.environ.get('SRC_BASE_DIR')
#     tgt_base_dir = os.environ.get('TGT_BASE_DIR')
#     schemas = json.load(open(f'{src_base_dir}/schemas.json'))
#     if not ds_names:
#         ds_names = schemas.keys()
#     for ds_name in ds_names:
#         print(f'Processing dataset: {ds_name}')
    

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


def check_even_or_odd(number: int) -> str:
    """
    Checks if a number is even or odd without using if-else or loops.

    An even number has a remainder of 0 when divided by 2.
    A boolean expression (like number % 2 == 0) evaluates to True (1) for even
    and False (0) for odd. This boolean result can then be used to index a list.

    Args:
        number: An integer to check.

    Returns:
        "Even" if the number is even, "Odd" if the number is odd.
    """
    # If (number % 2 == 0) is True, it means the number is even.
    # True evaluates to 1 when used as an index.
    # If (number % 2 == 0) is False, it means the number is odd.
    # False evaluates to 0 when used as an index.
    # So, the list should be ordered as [result_for_False, result_for_True].
    return ["Odd", "Even"][number % 2 == 0]

# Example usage:
print(f"4 is {check_even_or_odd(4)}")
print(f"7 is {check_even_or_odd(7)}")
print(f"0 is {check_even_or_odd(0)}")
print(f"-2 is {check_even_or_odd(-2)}")
print(f"-3 is {check_even_or_odd(-3)}")

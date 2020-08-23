
# dedicated module for data cacheing
# serve as a local python data server
# TO DO, implemented separation of namespace by packet ID from global

dag = None
reverse_dag = None
map_of_data = {}
num_of_children = {}
num_of_parent = {}
from collections import Counter
children_counter = Counter()
parent_counter = Counter()

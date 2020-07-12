# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 13:20:41 2020
@author: Pchan
"""
def load_data(data):
    import local_control_data
    from collections import Counter
    local_control_data.children_counter = Counter()
    dag = data['dag']
    local_control_data.num_of_children = dag.count_neighbour()
    local_control_data.num_of_parent = dag.reverse().count_neighbour()
    current_node_name = data['current_node_name']
    if current_node_name in local_control_data.map_of_data:
        local_control_data.map_of_data[current_node_name] = data

def all_parent_data_obtained2(next_node_name):
    
    import local_control_data
    return local_control_data.num_of_parent[next_node_name]  <= local_control_data.parent_counter[next_node_name]

def all_children_spawned(current_node_name):
    import local_control_data
    return local_control_data.children_counter[current_node_name] >= local_control_data.num_of_children[current_node_name]

def remove_parent_data2(current_node_name ):
    # remove only no longer dependent on 
    import local_control_data
    if all_children_spawned(current_node_name):
        local_control_data.map_of_data.pop(current_node_name)
    pass
def store_data_to_map(data):
    current_node_name = data['current_node_name']
    from copy import deepcopy
    import local_control_data
    data_to_next = deepcopy(data)
    local_control_data.map_of_data[current_node_name] = data_to_next
    pass
def spawn_child(data, q):
    import local_control_data
    current_node_name = data['current_node_name']
    dag = data['dag']
    store_data_to_map(data)
    for next_node_name in dag[current_node_name]:  
        # syntax to iterate neighbour
        local_control_data.children_counter[current_node_name] += 1
        local_control_data.parent_counter[next_node_name] += 1
        
        q.put(next_node_name)
    pass

def process(data):
    dag = data['dag']
    current_node_name = data['current_node_name']
    current_process_name = dag.nodes[current_node_name] ['process_name'] 
    import local_data_controller
    next_func_call = getattr(local_data_controller, current_process_name)
    if all_parent_data_obtained2(current_node_name):
        next_func_call(data)
    remove_parent_data2(current_node_name)
    
    # spawn children
    # spawn_child(data)
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

# def all_parent_data_obtained(next_node_name, reverse_dag):
    
#     import local_control_data
#     for upstream_node_name in  reverse_dag[next_node_name]:
#         if not upstream_node_name in local_control_data.map_of_data:
#             return False
#         pass
#     return True

def all_parent_data_obtained2(next_node_name):
    
    import local_control_data
    return local_control_data.num_of_parent[next_node_name]  <= local_control_data.parent_counter[next_node_name]


# def remove_parent_data(node_name , dag, reverse_dag):
#     import local_control_data
#     for parent_name in reverse_dag[node_name]:
#         if parent_name in local_control_data.map_of_data:
#             # consider delete only if data is not deleted
#             pass
#         else:
#             break
#         all_child_born = True
#         for parent_children_name in dag[parent_name]:
#             if parent_children_name in local_control_data.map_of_data:
#                 all_child_born = False
#                 break
#             pass
#         pass
#         if all_child_born:
#             local_control_data.map_of_data.pop(parent_name)
#             pass
        
#     pass

def all_children_spawned(current_node_name):
    import local_control_data
    return local_control_data.children_counter[current_node_name] >= local_control_data.num_of_children[current_node_name]

def remove_parent_data2(current_node_name ):
    # remove only no longer dependent on 
    import local_control_data
    if all_children_spawned(current_node_name):
        local_control_data.map_of_data.pop(current_node_name)
    pass


def process_child_processes(data):
    dag = data['dag']
    current_node_name = data['current_node_name']
    current_process_name = dag.nodes[current_node_name] ['process_name'] 
    for next_node_name in dag[current_node_name]:  
        # syntax to iterate neighbour
        local_control_data.children_counter[current_node_name] += 1
        local_control_data.parent_counter[next_node_name] += 1
        from local_data_controller_helper import all_parent_data_obtained2, remove_parent_data2
        next_process_name = dag.nodes[next_node_name]['process_name']
        
        import local_data_controller
        from copy import deepcopy
        
        next_func_call = getattr(local_data_controller, next_process_name)
        data_to_next = deepcopy(data)
        local_control_data.map_of_data[current_node_name] = data_to_next
        
        next_node_name
        reverse_dag = dag.reverse()
        if all_parent_data_obtained(next_node_name, reverse_dag):
            next_func_call()
        remove_parent_data2(current_node_name)
        
        pass
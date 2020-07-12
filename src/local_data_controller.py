# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 08:54:28 2020

@author: pchan
"""


# all data in format of 
# dag   : the dag map of the graph
# data  : data to process, a dict, where keys are the name of the upper steps, values are the data
# current_node_name : where the data is so far
# namespace that does not want to be detected by 


def input( data):
    assert 'passed_local_controller' in data
    pass
def output( data):
    assert 'passed_local_controller' in data
    pass
def add( data):
    assert 'passed_local_controller' in data
    pass



def to_control( data):
    data.pop('passed_local_controller')
    assert 'data' in data and 'dag' in data and 'current_node_name' in data
    import dara_ref as dr, connections
    my_data_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_packet_input')
        
    insert_result = my_data_ref.data_insert(data = data, connectionStr = None, mongoClient = connections.client)
    my_data_ref.documentID = insert_result.inserted_id
    return my_data_ref
    pass
def global_to_local_control(data):
    data.pop('passed_local_controller') 
    to_local_control(data)
    pass
def process_to_global_control(data):
    data['passed_local_controller'] = True
    return to_control(data)
    
    pass
def local_to_global_control(data):
    return to_control(data)
    pass
def to_local_control(data):
    
    
    process_to_local_control(data)
    pass
def process_to_local_control(data):
    ''' process that manager data flow locally'''
    # this route data from mongo to local process or from local process to next local process
    
    
    dag = data['dag']
    current_node_name = data['current_node_name']
    current_process_name = dag.nodes[current_node_name] ['process_name'] 
    
    data['passed_local_controller'] = True
    from local_data_controller_helper import load_data
    load_data(data)
    import local_control_data
    
    # ensure data has passed through local control 
    
    
    
    if (current_process_name == 'to_control' or 
        current_process_name == 'local_to_global_control' or 
        current_process_name == 'process_to_global_control')  : 
        to_control(data)
    else:
        from local_data_controller_helper import process_child_processes
        
        
        process_child_processes(data)
        pass
    pass



def print_to_console( data):
    assert 'passed_local_controller' in data
    from process_data import print_to_console
    pass


# if __name__ == '__main__':
#     import local_control_data
#     local_control_data.map_of_data = {}
#     import data_map_processor
#     my_process_dag = data_map_processor. process_dag()
#     my_process_dag.add_node('to_local_control0', process_name = 'to_local_control')
#     my_process_dag.add_node('input0', process_name = 'input')
#     my_process_dag.add_node('input1', process_name = 'input')
#     my_process_dag.add_node('print_to_console0', process_name = 'print_to_console')
#     my_process_dag.add_edge('to_local_control0', 'input0')
#     my_process_dag.add_edge('input0', 'input1')
#     my_process_dag.add_edge('input1', 'print_to_console0')
#     from data_map_processor import hiking_data
#     data = hiking_data(data = [1,2,3,4], 
#                        dag = my_process_dag, 
#                        current_node_name = 'to_local_control0')
#     data['passed_local_controller'] = True
#     process_to_local_control(data)
#     pass




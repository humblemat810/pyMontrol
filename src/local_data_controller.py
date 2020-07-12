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

from local_data_controller_helper import process_child_processes
def input( data):
    # TO_DO, implement
    

    pass
def output( data):
    # TO_DO, implement
    
    

    pass
def add( data):
    # TO_DO, implement
    

    pass



def to_global_control( data):
    # data.pop('passed_local_controller')
    assert 'data' in data and 'dag' in data and 'current_node_name' in data
    import dara_ref as dr, connections
    my_data_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_packet_input')
        
    insert_result = my_data_ref.data_insert(data = data, connectionStr = None, mongoClient = connections.client)
    my_data_ref.documentID = insert_result.inserted_id
    return my_data_ref
    pass

    
def to_control(data):
    #alias
    return to_global_control(data)
def local_control_init(data):
    from local_data_controller_helper import load_data
    load_data(data)
    from queue import Queue
    data['passed_local_controller'] = True
    from local_data_controller_helper import (spawn_child, process, 
                                              collect_parent_data, store_data_to_map)
    
    q = Queue()
    store_data_to_map(data)
    process(data)
    spawn_child(data,q)
    while not q.empty():
        current_node_name = q.get()
        data = collect_parent_data(current_node_name)
        data['passed_local_controller'] = True
        process(data)
        data.pop('passed_local_controller')
        spawn_child(data,q)
        pass
    
    pass
def local_control(data):
    ''' process that manage data flow locally'''
    # this route data from mongo to local process or from local process to next local process

    data['passed_local_controller'] = True
    return data
    
    # ensure data has passed through local control 
    pass



def print_to_console( data):
    assert 'passed_local_controller' in data
    from process_data import print_to_console
    pass

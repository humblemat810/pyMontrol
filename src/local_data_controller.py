# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 08:54:28 2020

@author: pchan
"""


"""
Each function in this class will be inspected,
this will be a node name in the dag class
#for example use :
from data_map_processor import process_dag
my_process_dag = process_dag()
my_process_dag.add_node('input0', 'init')
data = {'data' : np.arange(121), 'dag' : my_process_dag, 'current_node_name' : 'input0'}


import datetime
import threading
from copy import deepcopy
def send_data_by_thread(data):
    my_data_store_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_store')
    raw_data_insert_result = my_data_store_ref.data_insert(data = data, 
                                                     connectionStr = None, 
                                                     mongoClient = connections.client)
    my_data_store_ref.documentID = raw_data_insert_result.inserted_id
    my_data_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_packet_input')
    data_ref_insert_result = my_data_ref.data_insert(data = my_data_store_ref, 
                                                       connectionStr = None, 
                                                       mongoClient = connections.client)
    my_data_ref.documentID = data_ref_insert_result.inserted_id
    pass
def send_data_by_thread2(data):
    my_data_store_ref = dr.data_ref(db = worker_db, collection = 'test_worker0')
    raw_data_insert_result = my_data_store_ref.data_insert(data = data, 
                                                     connectionStr = None, 
                                                     mongoClient = connections.client)
    my_data_store_ref.documentID = raw_data_insert_result.inserted_id
    my_data_ref = dr.data_ref(db = worker_db, collection = 'test_worker0')
    data_ref_insert_result = my_data_ref.data_insert(data = my_data_store_ref, 
                                                       connectionStr = None, 
                                                       mongoClient = connections.client)
    my_data_ref.documentID = data_ref_insert_result.inserted_id
    pass
use_thread = True
for i in range(1):
    if use_thread:
        from copy import deepcopy
        x = threading.Thread(target = send_data_by_thread, args = (deepcopy(data),))
        x.start()
    else:
        send_data_by_thread(data)

"""




# all data in format of 
# dag   : the dag map of the graph
# data  : data to process, a dict, where keys are the name of the upper steps, values are the data
# current_node_name : where the data is so far
# e.g. data = {'data' : np.arange(121), 'dag' : my_process_dag, 'current_node_name' : 'input0'}

# namespace that does not want to be detected by 

def init(data):
    
    pass
def input( data):
    # TO_DO, implement
    

    pass
def output( data):
    # TO_DO, implement
    
    

    pass
def add( data):
    # TO_DO, implement
    

    pass
def load_tensorflow(data):
    import tensorflow as tf
    pass


def to_global_control( data):
    # data.pop('passed_local_controller')
    assert 'data' in data and 'dag' in data and 'current_node_name' in data
    import dara_ref as dr, connections
    my_data_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_packet_input')
        
    insert_result = my_data_ref.data_insert(data = data, connectionStr = None, mongoClient = connections.client)
    my_data_ref.documentID = insert_result.inserted_id
    print ('returned packet to global queue with ID = ', my_data_ref.documentID)
    return my_data_ref
    pass

    
def to_control(data):
    #alias
    return to_global_control(data)

  

def local_control(data):
    ''' process that manage data flow locally'''
    # this route data from mongo to local process or from local process to next local process
    assert 'passed_local_controller' not in data
    data['passed_local_controller'] = True
    return data
    
    # ensure data has passed through local control 
    pass



def print_to_console( data):
    assert 'passed_local_controller' in data
    from process_data import print_to_console
    pass



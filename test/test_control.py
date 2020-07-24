# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 00:04:10 2020

@author: pchan
"""

worker_db = 'LDS_worker'
import sys, os, pathlib
sys.path.append(str(pathlib.Path('../src/')))
import connections
import data_ref as dr
from data_map_processor import process_dag
import numpy as np
my_process_dag = process_dag()
my_process_dag.add_node('input0', 'load_tensorflow')
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
for i in range(10):
    if use_thread:
        from copy import deepcopy
        x = threading.Thread(target = send_data_by_thread, args = (deepcopy(data),))
        x.start()
    else:
        send_data_by_thread(data)
# connections.client['eventTrigger']['data_packet_input'].insert_one()

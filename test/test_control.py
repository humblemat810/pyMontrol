# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 00:04:10 2020

@author: pchan
"""
import sys, os, pathlib
sys.path.append(str(pathlib.Path('../src/')))
import connections
import data_ref as dr
from data_map_processor import process_dag
my_process_dag = process_dag()
my_process_dag.add_node('input0', 'load_tensorflow')
data = {'data' : 121, 'dag' : my_process_dag, 'current_node_name' : 'input0'}
my_data_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_packet_input')
import threading
def send_data_by_thread(data):
    insert_result = my_data_ref.data_insert(data = data, connectionStr = None, mongoClient = connections.client)
    my_data_ref.documentID = insert_result.inserted_id
    pass
for i in range(50):
    x = threading.Thread(target = send_data_by_thread, args = (data,))
    x.start()

# connections.client['eventTrigger']['data_packet_input'].insert_one()

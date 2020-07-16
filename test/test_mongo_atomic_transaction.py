# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 20:02:00 2020

@author: ASUS
"""

import sys, os, pathlib
sys.path.append(str(pathlib.Path('../src/')))
import connections
import data_ref as dr
from data_map_processor import process_dag
import numpy as np
my_process_dag = process_dag()
my_process_dag.add_node('input0', 'load_tensorflow')
data = {'data' : np.arange(121), 'dag' : my_process_dag, 'current_node_name' : 'input0'}


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
    my_data_store_ref = dr.data_ref(db = 'worker', collection = 'test_worker0')
    raw_data_insert_result = my_data_store_ref.data_insert(data = data, 
                                                     connectionStr = None, 
                                                     mongoClient = connections.client)
    my_data_store_ref.documentID = raw_data_insert_result.inserted_id
    my_data_ref = dr.data_ref(db = 'worker', collection = 'test_worker0')
    data_ref_insert_result = my_data_ref.data_insert(data = my_data_store_ref, 
                                                       connectionStr = None, 
                                                       mongoClient = connections.client)
    my_data_ref.documentID = data_ref_insert_result.inserted_id
    pass
use_thread = False
for i in range(1):
    if use_thread:
        from copy import deepcopy
        x = threading.Thread(target = send_data_by_thread2, args = (deepcopy(data),))
        x.start()
    else:
        send_data_by_thread(data)
        

        
mongoClient = connections.client
doc = mongoClient['worker']['test_worker0'].find_one({})
from copy import deepcopy
import pickle
replacement_doc = deepcopy(doc)
replacement_doc['data'] = pickle.dumps('ttttt cute')
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from pymongo.read_preferences import ReadPreference
wc_majority = WriteConcern("majority", wtimeout=2000)
worker_name = 'worker0'
db_from = 'worker'
db_to = 'worker1'
col_from = 'test_worker0'
col_to = 'test_worker1'
collection_from = mongoClient[db_from][col_from]
collection_to= mongoClient[db_to][col_to]
collection_to.insert_one({'beginning': 1})
session=  mongoClient.start_session()
session.start_transaction(read_concern=ReadConcern('local'),
                          write_concern=wc_majority)
# logging_info = 'packet id' + str(doc['_id']) + ' assigned to worker ' + str(worker_name)
collection_from.delete_one({'_id' : doc['_id']}, session=session)
collection_to.replace_one({'_id' : doc['_id']}, 
                                       replacement_doc , upsert = True, session = session)
# mongoClient['worker']['test_worker0'].insert_one(replacement_doc, session = session, upsert = True)

# mongoClient['log'] ['controller_log'].insert_one({'info' : logging_info}, session = session)
session.commit_transaction()
session.end_session()


collection_to.replace_one({'_id' : doc['_id']}, 
                                       replacement_doc , upsert = True)
collection_to.find_one({'_id' : doc['_id']})







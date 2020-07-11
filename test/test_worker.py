# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 22:43:47 2020

@author: pchan
"""


import data_ref as dr
import connections
my_data_true = dr.data_ref(db = 'worker', collection = 'data')
true_insert_result = my_data_true.data_insert(data = 'sdas', connectionStr = None, mongoClient = connections.client)
my_data_true.documentID = true_insert_result.inserted_id

my_data_ref = dr.data_ref(db = 'worker', collection = 'test_worker1')
ref_data_insert_result = my_data_ref.data_insert(data = my_data_true, connectionStr = None, mongoClient = connections.client)

my_data_ref.documentID=ref_data_insert_result.inserted_id
ref_delete_result = my_data_ref.delete_data(connectionStr = None, mongoClient = connections.client)



# my_data_true.documentID = true_insert_result.inserted_id
# true_delete_result = my_data_true.delete_data(connectionStr = None, mongoClient = connections.client)

# import worker_command
# kill_command =worker_command.worker_command('kill')
# my_data_ref = dr.data_ref(db = 'worker', collection = 'test_worker1')
# my_data_ref.data_insert(data = kill_command, connectionStr = None, mongoClient = connections.client)

from control import kill_worker
kill_worker('test_worker1')
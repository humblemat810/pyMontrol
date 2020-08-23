# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 08:18:16 2020

@author: pchan
"""

worker_db = 'pc_worker'
worker_name ='test_worker0'
worker_name ='test_controller0'
import sys, os, pathlib
sys.path.append(str(pathlib.Path('../src/')))
import connections, worker_command
import data_ref as dr
from connections import client


my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)


my_data_ref.data_insert(data = worker_command.reload_code(worker_name = worker_name), connectionStr = None, mongoClient = client)


my_data_ref.data_insert(data = worker_command.command_kill_worker(worker_name = worker_name), connectionStr = None, mongoClient = client)
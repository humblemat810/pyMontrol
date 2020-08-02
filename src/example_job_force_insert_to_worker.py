# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 18:06:46 2020

@author: pchan
"""
'''This example insert data to data store and pass the data-reference to specified 
worker store, by passing any worker check even if not exist
useful to test the worker only without any controller
'''

worker_db = 'pc_worker'
import sys, os, pathlib
sys.path.append(str(pathlib.Path('../src/')))
import connections
import data_ref as dr
import numpy as np
import datetime


data = {'trait' :  "value"}
my_data_ref = dr.data_ref(db = worker_db, collection = 'test_worker1')
my_data_ref.dd_insert(data, mongoClient=connections.client)
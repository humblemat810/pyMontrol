# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 18:06:46 2020

@author: pchan
"""
'''This example insert data to data store and pass the data-reference to the 
controller queue for job allocation
'''
import sys, os, pathlib
sys.path.append(str(pathlib.Path('../src/')))
import connections
import data_ref as dr
import numpy as np
import datetime


data = {'trait' :  "value"}
my_data_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_packet_input')
my_data_ref.dd_insert(data, mongoClient=connections.client)
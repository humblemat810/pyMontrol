# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 00:04:10 2020

@author: pchan
"""

import connections
import data_ref as dr
my_data_ref = dr.data_ref(db = 'eventTrigger', collection = 'data_packet_input')
    
insert_result = my_data_ref.data_insert(data = '121', connectionStr = None, mongoClient = connections.client)
my_data_ref.documentID = insert_result.inserted_id

# connections.client['eventTrigger']['data_packet_input'].insert_one()
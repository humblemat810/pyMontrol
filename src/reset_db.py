# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 14:39:12 2020

@author: ASUS
"""

import connections
client = connections.client

client.drop_database('worker')
client.drop_database('eventTrigger')
client.drop_database('log')


import glob, os

listing = glob.glob('*.log')
for filename in listing:
    os.remove(filename)
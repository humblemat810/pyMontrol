# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 20:02:41 2020

@author: pchan
"""

import yaml, pathlib

with open(str(pathlib.Path(r'./credentials/mongo.json')), 'r') as file:
    mongoconfig = yaml.safe_load(file)
if 'connectionstring' in mongoconfig:
    default_connection_str  = mongoconfig['connectionstring']
else:
    if mongoconfig['user'] == '' and mongoconfig['key'] == '':
        default_connection_str = (r"mongodb://" +mongoconfig['host'] + ':' + str(mongoconfig['port']) + "/")
    else:
        default_connection_str = (r"mongodb://"+mongoconfig['user']+':'+ mongoconfig['key']
                                        +  "@" +mongoconfig['host'] + ':' + str(mongoconfig['port']) + "/")


with open(str(pathlib.Path(r'./debugConfig.yaml')), 'r') as file:
    debugConfig = yaml.safe_load(file)
use_mockdb = False
try :
    use_mockdb = debugConfig['mockdb']
except:
    pass

if use_mockdb:
    import mongomock
    clientMethod= mongomock.mongo_client.MongoClient
    pass
else:
    import pymongo
    clientMethod= pymongo.MongoClient
    
# default_connection_str = "mongodb+srv://pchan:montroller@cluster0.qpjnb.azure.mongodb.net/test"
client = clientMethod(default_connection_str)


    
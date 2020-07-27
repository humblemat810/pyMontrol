# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 10:01:26 2020

@author: pchan
"""

# TO_DO: abstract base worker both controller
'''

1. logging
2. response to health heart beat
3. listen to its own db
4. registor work
5. process event
process_event_threadable
6. work
'''

from abc import ABCMeta
import pymongo, time
class exit_code_class:
    kill_worker = 0
    fail = 1
    break_current_and_continue = 2
    success = 3
exit_code = exit_code_class()
class base_worker(metaclass=ABCMeta):
    client : pymongo.MongoClient
    worker_db : str
    def worker_register(self, worker_collection_name = None,registration_collection = 'availableWorker'):
        assert not (worker_collection_name  in [i["_id"]for i in self.client[self.worker_db][registration_collection].find()])
        self.worker_collection_name = worker_collection_name
        self.name = worker_collection_name
        self.registration_collection = registration_collection
        if worker_collection_name not in self.client[self.worker_db].list_collection_names():
            self.client[self.worker_db][worker_collection_name].insert_one({'tag':'beginning'})
            
            time.sleep(0.5)
        try: 
            insert_result  = self.client[self.worker_db][registration_collection].insert_one({'_id' : worker_collection_name, 
                                                             'free-since' : int(time.time()),
                                                             'alive' : True
                                                             })
        except pymongo.errors.DuplicateKeyError as e:
            # logger.info('worker registered, try next, future will implement to test if that worker is dead and resume its role')
            return (exit_code.fail, e)
        
        # self.dataStream = 
        return exit_code.success, None
    def report_health(self, receiver):
        import psutil
        from dtype import health_report
        self.health_report = health_report( {'dtype' : 'dtype.health_report',   'data' : {'status':'alive', 'sender' : self.name, 'virtual_memory' : dict(psutil.virtual_memory()._asdict()),
                       'cpu' : psutil.cpu_percent()}})
        self.client[self.worker_db][receiver].insert_one(self.health_report)
        pass
    pass
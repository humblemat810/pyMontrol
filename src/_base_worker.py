# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 10:01:26 2020

@author: pchan
"""

# TO_DO: abstract base worker both controller


import connections, threading
from threading import Thread
worker_db  = 'pc_worker'
from abc import ABCMeta, abstractmethod
import pymongo, time
class exit_code_class:
    kill_worker = 0
    fail = 1
    break_current_and_continue = 2
    success = 3
exit_code = exit_code_class()
import worker_command


class base_worker(metaclass=ABCMeta):
    client : pymongo.MongoClient
    worker_db : str
    
    
    @property
    @abstractmethod
    def check_worker_on():
        pass
    
    
    def worker_register(self, worker_collection_name = None,registration_collection = 'availableWorker'):
        # assert not (worker_collection_name  in [i["_id"]for i in self.client[self.worker_db][registration_collection].find()])
        # col_list = [i["_id"] for i in self.client[self.worker_db][registration_collection].find()]
        c = self.client[self.worker_db][registration_collection].find_one({"_id" : worker_collection_name})
        if c is not None:
            from errorType import duplicate_worker_name_error
            raise duplicate_worker_name_error("duplicate worker name detected")
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
    def process_common_command(self, command : worker_command.worker_command):
        if 'kill' in command.command_json:
            logging_info = 'killing worker ' + str(self.worker_collection_name) +' by worker_command.worker_command'
            self.logger.info(logging_info)
            self.client[worker_db]['availableWorker'].remove({'_id': self.worker_collection_name})
            self.vomit_job_back_to_queue()
            self.client[worker_db][self.worker_collection_name].drop()
            
            
            print(logging_info)
            self.being_kill = True
            self.eventStream.close()
            return exit_code.kill_worker
        if 'reload_code' in command.command_json :
            import builtins
            from IPython.lib import deepreload
            builtins.reload = deepreload.reload
            logging_info = 'worker ' + self.worker_collection_name + ' reloaded code'
            print(logging_info)
            self.logger.info(logging_info)
            
            return
        if 'spawn_new_adjacent_worker' in command.command_json :
            self.spawn_new_worker()
            pass
        if 'report_health' in command.command_json:
            report_to = command.command_json['report_health'] ['to']
            self.report_health(receiver = report_to)
            pass
        pass

    @abstractmethod
    def command_handler_threadable(self):
        pass
    def report_health(self, receiver):
        import psutil, datetime
        from dtype import health_report
        self.health_report = health_report( {'dtype' : 'dtype.health_report',   'data' : {'status':'alive', 'sender' : self.name, 'virtual_memory' : dict(psutil.virtual_memory()._asdict()),
                       'cpu' : psutil.cpu_percent()}, 'datetime' : datetime.datetime.now()})
        self.client[self.worker_db][receiver].insert_one(self.health_report)
        pass

    def get_worker_health(self,worker_name):
        # if controller.worker_exist(worker_name):
        import worker_command
        import data_ref as dr
        # controller.worker_exist(worker_name)
        get_health_command = worker_command.command_report_health(worker_name, self.name)
        my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)
        my_data_ref.data_insert(get_health_command, mongoClient = connections.client)
        pass
    
    
    def interval_check_worker_availability(self, interval = 10):
        while(self.check_worker_on):
            self.find_and_remove_all_MIA_worker_availability()
            # t = Thread(target = self.find_and_remove_all_MIA_worker_availability)
            # t.start()
            time.sleep(interval)
        pass
    def process_doc(self, doc):
        to_return = self.process_doc_callback(doc)
        # self.logging_doc_results(doc)   # doc level logging
        return to_return
    # ============ to refactor =========
    
    def process_doc2(self, doc):
        if not self.check_document_integrity2():
            return
        to_return = self.process_doc_callback2(doc)
        
        return to_return
    # ============ refactor ends =========
    
    
    
    def find_and_remove_all_MIA_worker_availability(self):
        # if a worker missing in action, remove that worker from collection jobboard
        # print('find and remove fake worker')
        for workerBoardCollection in ['availableWorker', 'availableController']:
            worker_cursor = self.client[worker_db][workerBoardCollection].find({})
            for worker_record in worker_cursor:
                worker_name = worker_record['_id']
                # self.find_and_remove_MIA_worker_availability(worker_name)
                # print('found worker', worker_name)
                
                t = Thread(target = self.find_and_remove_MIA_worker_availability, args = (worker_name,))
                t.start()
        # print('all worker checked')
        pass
    def find_and_remove_MIA(self, worker_name, register_collection_name = 'availableWorker'):
        self.get_worker_health(worker_name)
        import time
        time.sleep(5)
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        from pymongo.read_preferences import ReadPreference
        wc_majority = WriteConcern("majority", wtimeout=2000)
        session=  self.client.start_session()
        session.start_transaction(read_concern=ReadConcern('local'),
                                  write_concern=wc_majority)
        record = self.client[worker_db] [self.name].find_one_and_delete({'data.sender' : worker_name})
        
        if record is None:
            print('lost touch with worker '+worker_name)
            # kill_worker
            self.client[worker_db][register_collection_name].delete_many({"_id": worker_name} )
            pass
        else:
            print('worker repored alive '+worker_name)
            self.client['log']['health_history'].insert_one(record)
        session.commit_transaction()
        session.end_session()
        import logging
        logging.info('removed worker' + worker_name + 'from ' +register_collection_name)
        pass
    def find_and_remove_MIA_controller_availability(self, worker_name):
        # if a worker missing in action, remove that worker from collection jobboard
        # print('heartbeating', worker_name)
        self.find_and_remove_MIA(worker_name, 'availableController')
        pass
    
    def find_and_remove_MIA_worker_availability(self, worker_name):
        # if a worker missing in action, remove that worker from collection jobboard
        # print('heartbeating', worker_name)
        self.find_and_remove_MIA(worker_name)
        pass
    def spawn_new_worker(self):
        from subprocess import Popen
        Popen('python', __file__)
        pass
    def process_existing_work_doc(self):
        for doc in self.client[worker_db][self.worker_collection_name].find({'tag' : None}):
            self.process_doc_callback(doc) 
        pass
  
            
    def process_if_being_killed(self):
        if self.being_kill:
            if hasattr(self, 'dataStream'):
                self.logger.info('closing data stream')
                self.dataStream.close()
            if hasattr(self, 'eventStream'):
                self.logger.info('closing event stream')
                self.eventStream.close()
            if hasattr(self, 'available_worker_event_stream'):
                self.logger.info('closing event stream')
                self.available_worker_event_stream.close()    
            self.client[worker_db][self.registration_collection].delete_one({'_id' : self.worker_collection_name})
            
            
        pass
    @abstractmethod
    def pre_work():
        # define  self.work_listenStream  and self.changeStream_process_callback here
        pass
    
    @staticmethod
    def worker_or_controller_exist(worker_name, collection_name):
        if not worker_name.startswith('test_controller') and not worker_name.startswith('test_worker') and not worker_name.startswith(worker_db):
            return False
            # raise(ValueError('bad collection name : '+ worker_name))
            pass
        if (not(worker_name  in  connections.client[worker_db].list_collection_names())
            and  connections.client[worker_db][collection_name].find_one({'_id' : worker_name}) is None):
            return False
            # raise(ValueError('collection name not in worker collections'))
            pass
        return True
        
    @staticmethod
    def worker_exist(worker_name):
        return base_worker.worker_or_controller_exist(worker_name, 'availableWorker')
        
    @staticmethod
    def controller_exist(worker_name):
        return base_worker.worker_or_controller_exist(worker_name, 'availableController')
    @staticmethod
    def kill_worker(worker_name):
        if base_worker.worker_exist(worker_name):
            base_worker.worker_exist(worker_name)
            import worker_command
            import data_ref as dr
            # kill_command =worker_command.worker_command('kill')
            my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)
            my_data_ref.data_insert(data = worker_command.command_kill_worker(worker_name = worker_name), connectionStr = None, mongoClient = connections.client)
        pass
    

    
    @staticmethod
    def worker_reload(worker_name):
        if base_worker.worker_exist(worker_name):
            import worker_command
            import data_ref as dr
            reload_code_command =worker_command.worker_command('reload_code')
            my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)
            my_data_ref.data_insert(data = reload_code_command, connectionStr = None, mongoClient = connections.client)
        pass
    
    def work(self):
        # apply call back on the event stream that worker / controller watching
        self.pre_work()
        listen_stream = self.work_listenStream
        # first process existing doc for controller
        self.process_existing_work_doc()
        # next listen to new controller commands
        y = threading.Thread(target = self.command_handler_threadable)  # controller need, worker no need
        y.start()
        # self.command_handler_threadable()
        try:
            for i in listen_stream:
                if not self.filter_eventStream(i):
                    continue
                self.changeStream_process_callback(i)
        except KeyboardInterrupt:
            self.being_kill = True
            import worker_command
            command = worker_command.worker_command()
            command.kill_worker(self.name)
            self.process_common_command(command)
            raise
        except Exception as e:
            if e.args[0] == 'Error in $cursor stage :: caused by :: operation was interrupted':
                # ok
                pass
            else:
                self.logger.info('err')
                raise
            pass
            
        if self.being_kill:
            self.logger.info('=========== being killed ==========')
            return exit_code.kill_worker
        pass
    pass

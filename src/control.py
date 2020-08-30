


import logging, pathlib, yaml, controller_command
logger = None
with open(str(pathlib.Path(r'./control_config.yaml')), 'r') as file:
    control_config = yaml.safe_load(file)
with open(str(pathlib.Path(r'./config.yaml')), 'r') as file:
    config = yaml.safe_load(file)    
worker_db = config['worker_db']
strict_mode = False
if 'strict_mode' in control_config:
    strict_mode = control_config['strict_mode']
use_thread = False
if 'use_thread' in control_config:
    use_thread = control_config['use_thread']    
from  all_decorate import for_all_methods, debug_decorater
import connections
import time , threading, datetime
import pickle, pymongo

from exit_code_class import exit_code_class

exit_code = exit_code_class()
from _base_worker import base_worker
# @for_all_methods(debug_decorater)
class controller(base_worker):
    # def atomic_auto_assign_new_data(self, new_doc, mongoClient):
    name : str
    check_worker_on = True
    #     pass
    def find_and_retry_unfinished_task(self):
        pass
    def atomic_assign_data(self, 
                           doc, 
                           mongoClient, 
                           db_from , col_from ,
                           db_to,  col_to):
        """
        atomic transaction on mongodb to allocate task to worker
        """
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        from pymongo.read_preferences import ReadPreference
        wc_majority = WriteConcern("majority", wtimeout=2000)
        
        from copy import deepcopy
        import numpy as np, pickle, datetime
        
        doc ['assignment_time'] = datetime.datetime.now()
        to_send = deepcopy(doc)
        
        to_send.pop('_id')
        collection_from = mongoClient[db_from][col_from]
        collection_to= mongoClient[db_to][col_to]
        session=  mongoClient.start_session()
        logging_info = ('packet id' + str(doc['_id']) 
                            + ' assigned to worker ' 
                            + str(worker_name))
        logging.info(logging_info + ' start')
        session.start_transaction(read_concern=ReadConcern('local'),
                                  write_concern=wc_majority)
        logging_info = ('packet id' 
                        + str(doc['_id']) 
                        + ' assigned to worker ' 
                        + str(worker_name))
        # Important:: You must pass the session to the operations.
        collection_from.find_one_and_delete(
            {'_id' : doc['_id']}, 
            session=session)
        collection_to.replace_one(
            {'_id' : doc['_id']}, 
            doc , 
            upsert = True, 
            session = session)
        mongoClient['log'] ['controller_log'].insert_one(
                {'info' : logging_info, 
                 "utctime": datetime.datetime.utcnow()
                 }, 
            session = session)
        session.commit_transaction()
        session.end_session()
        logging.info(logging_info + ' end')
        
        pass

    def worker_listen(self,):
        worker_collection_name = self.worker_collection_name
        self.logger.info("listening to " + worker_db + worker_collection_name)
        self.eventStream = self.client[worker_db][worker_collection_name].watch()
        
        try:
            resume_token = (connections.client['eventTrigger']
                            ['resume_token'].find_one()['value'])
            # resume_token = bytes(resume_token, 'utf-8')
            self.dataStream = (connections.client['eventTrigger']
                               ['data_packet_input'].
                               watch(resume_after = resume_token))
            self.logger.info('resume_after' + str(resume_token))
        except:
            self.dataStream = connections.client['eventTrigger']['data_packet_input'].watch()
        self.available_worker_event_stream = connections.client[worker_db]['availableWorker'].watch()
        pass
    def pop_free_worker(self, mongoClient, worker_colection  = "availableWorker"):
        
        # availableWorker = mongoClient[worker_db]['availableWorker'].find_one_and_delete({})
        availableWorker = mongoClient[worker_db]['availableWorker'].find_one({})
        return(availableWorker)
        pass
    

    
    def routeDataStream(self,fullDocument, mongoClient):
        
        # to be abstracted by using other load balancing algorithms====
        availableWorker = self.pop_free_worker(mongoClient)
        #===========
        worker_found = availableWorker is not None
        while not worker_found:
            logging_info = 'no free worker found'
            connections.client['log'] ['controller_log'].insert_one({'info' : logging_info, 
                                                                     "utctime": datetime.datetime.utcnow()}
                                                                    )
            # logger.info(logging_info)
            for i in self.available_worker_event_stream:
                if i['operationType'] == 'insert':
                    availableWorker = self.pop_free_worker(mongoClient)
                    
                    if availableWorker is not None:
                        worker_found = True
                        logging_info = 'found free worker ' + availableWorker
                        connections.client['log'] ['controller_log'].insert_one({'info' : logging_info, "utctime": datetime.datetime.utcnow()})
                    break
        
        worker_name = availableWorker['_id']
        self.logger.info('worker assign '+ worker_name)

        db_from = 'eventTrigger'
        col_from = 'data_packet_input'
        db_to = worker_db
        col_to = worker_name
        update_dict = {}
        self.atomic_assign_data(fullDocument,mongoClient, db_from, col_from, db_to, col_to)
        
        # logger.info(logging_info)
        
        pass
    
    def __init__(self):
        self.worker_db = worker_db
        self.being_kill = False
        import connections
        self.client = connections.client
        pass

    def worker_management(self,mongoClient):
        # kill or wake up sleeping worker
        c = mongoClient[worker_db]['availableWorker'].find()
        for worker in c:
            if int(time.time()) - worker['free-since'] > control_config['worker_time_out']:
                self.kill_worker(worker['_id'])
                pass
            pass
        pass    
    def record_post_threadable_event(self):
        self.logging_doc_results()
        self.event_cnt += 1
        
        self.logger.info('event count' + str( self.event_cnt))
        
        connections.client['log'] ['controller_log'].insert_one(
            {'event count' : self.event_cnt, 
             "utctime": datetime.datetime.utcnow()}
            )
    def kill_controller(self, controller_ref):
        raise NotImplementedError
    
    
    
    def process_event_threadable(self, j):   # worker thread process data
        # process existing doc
        
        doc = j['fullDocument']
        if not self.check_document_integrity(doc):
            return

        self.process_doc(doc)
        
        self.record_post_threadable_event() 
        
    def retry_failed_job(time_lag_to_retry = 300, # seconds
                         
                         ):
        
        
        pass
        
        # =============== to_refactor ==============================
    def process_event2(self, i):  # controller route data
        from copy import deepcopy
        
        j = deepcopy(i)
        
        if 'command' in j['fullDocument']:

            pass
        elif i['operationType'] == 'insert':
            pass
        else:
            return 
        
        if use_thread:
            x = threading.Thread(
                target=self.process_event_threadable, 
                args=(j,)
                )
            x.start()
        else:
            self.process_event_threadable(j)
    def check_document_integrity2(self, j):
        # 2 check what it listen to, original check the one with its name
        if 'tag' in j:
            return False
        return True
        pass
    def process_event_threadable2(self, j):
        doc = j['fullDocument']
        if not self.check_document_integrity2():
            return

        self.process_doc2(doc)
        self.record_post_threadable_event2(j)   
    def record_post_threadable_event2(self, j):
        self.logging_doc_results2()
        self.logger.info('b4 resume token')
        resume_token = j['_id']
        connections.client['eventTrigger']['resume_token'].replace_one(
            {'_id': 'resume_token'}, 
            {'_id': 'resume_token', 'value': resume_token},
            upsert = True
            )
        self.logger.info('after resume token')
        pass
    
    #  ================= refactor_end
    
    def wrapped_routeDataStream(self, i):
        self.routeDataStream(i['fullDocument'], connections.client)
        pass
    def command_handler_threadable(self):
        '''
        import controller_command,connections
        import data_ref as dr
        controller_to_kill = 'test_controller0'
        kill_command = controller_command.controller_command('kill')
        my_data_ref = dr.data_ref(db = worker_db, collection = controller_to_kill)
        my_data_ref.data_insert(data = kill_command, connectionStr = None, mongoClient = connections.client)
        '''
        try:
            self.logger.info('thread....')
            for i in self.eventStream:
                from copy import deepcopy
                if self.filter_command_eventStream(i):
                    pass
                else:
                    continue
                self.process_event2(deepcopy(i))
                self.process_if_being_killed()
            self.logger.info('end thread')
        except KeyboardInterrupt:
            import worker_command
            command = worker_command.command_kill_worker(self.name)
            self.process_common_command(command)
            self.being_kill = True
            self.process_if_being_killed()
            pass
        pass


    def pre_work(self):
        self.event_cnt = 0
        self.work_listenStream = self.dataStream
        self.command_listenStream = self.eventStream
        
        self.changeStream_process_callback = self.process_event2
        self.process_doc_callback = self.process_controller_command
        self.process_doc_callback2 = self.wrapped_routeDataStream
        pass
    def filter_command_eventStream(self, i):
        if i['operationType'] == 'insert':
            return True
        else:
            return False
        pass
    def filter_eventStream(self, i):
        if i['operationType'] == 'insert':
            return True
        else:
            return False


    def check_document_integrity(self, doc):
        if 'tag' in doc:
            return False
        return True
        pass

    def logging_doc_results(self, doc = None):
        pass
    def logging_doc_results2(self, doc = None):
        pass
    def vomit_job_back_to_queue(self):
        
        pass
    def process_controller_command(self, doc):
        import dtype
        data = doc['data']
        if 'tag' in doc:
            print('tagging omitted ', str(doc))
            return
        if 'dtype' in doc:
            if  doc['dtype'] == 'dtype.health_report':
                from pymongo.read_concern import ReadConcern
                from pymongo.write_concern import WriteConcern
                from pymongo.read_preferences import ReadPreference
                wc_majority = WriteConcern("majority", wtimeout=2000)
                session=  self.client.start_session()
                session.start_transaction(read_concern=ReadConcern('local'),
                                          write_concern=wc_majority)
                record = self.client[worker_db] [self.name].find_one_and_delete({'_id' : doc['_id']})
                self.client['log']['health_history'].insert_one(record)
                session.commit_transaction()
                session.end_session()
                print({ 'from' : self.name, 'to': {'log' : 'health_history'} , 'content' : record, 'file' : __file__})
                pass
        
        else:  # process pickled data
            data_unpickled = pickle.loads(data)
            self.logger.info('command unpickled '+ 'type' + str(type(data_unpickled)))
            if type(data_unpickled) is controller_command.controller_command:
                if type(data_unpickled.command) is str:
                    if data_unpickled.command == 'kill':
                        logger.info('kill_command_received')
                        self.being_kill = True
                if type(data_unpickled.command) is dict:
                    if 'kill' in data_unpickled.command:
                        if self.command['kill'] == self.worker_collection_name:
                            logger.info('kill_command_received')
                            self.being_kill = True
            else:
                import worker_command
                if  issubclass(type(data_unpickled),  worker_command.worker_command):
                    self.process_common_command(command = data_unpickled)
                    
                    pass
            record = self.client[worker_db] [self.name].find_one_and_delete({'_id' : doc['_id']})
        

        
# so far single controller supported
# if __name__ == '__main__':
#     my_controller = controller()
#     my_controller.worker_register()
#     my_controller.manage_new_data_for_execution()
#     pass

if __name__ == '__main__':
    # import control as ct
    # import data_ref as dr
    my_worker = controller()
    
    worker_name_prefix = 'test_controller'
    num = 0
    fail_cnt = 0
    import pathlib, logging
    while fail_cnt < 10:
        try:
            worker_name = worker_name_prefix  + str(num)
            # TO_DO, check if that controller exist and kill if neccessary
            my_worker.name = 'temp'
            my_worker.client[worker_db][my_worker.name].delete_many({'datetime' : 
                                                             {'$lt' : datetime.datetime.now() - datetime.timedelta(seconds=30)
                                                              }
                                                         })
            my_worker.find_and_remove_MIA_controller_availability(worker_name)
            try:
                from errorType import duplicate_worker_name_error
                result = my_worker.worker_register(worker_collection_name = worker_name, 
                                               registration_collection = 'availableController')
            except (pymongo.errors.DuplicateKeyError, duplicate_worker_name_error) as e:
                
                fail_cnt +=1
                num+=1
                if fail_cnt >= 10:
                    print('maximum retry exceeded')
                    raise
                    
                continue
            log_f_name = str(pathlib.Path( worker_name + '.log'))
            
            import os
            print('log file set as ' + str(pathlib.Path(os.getcwd()) / log_f_name))
            # log_ff_name = str(pathlib.Path(os.getcwd()) / log_f_name)
            FORMAT = "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"
            
            logger = logging.getLogger(__file__ + '_' + worker_name)
            for hdlr in logger.handlers[:]:  # remove all old handlers
                logger.removeHandler(hdlr)
            formatter = logging.Formatter(FORMAT)
            fileh = logging.FileHandler('./' +worker_name + '.log', 'a')
            fileh.setFormatter(formatter)
            logger.setLevel(logging.DEBUG)
            logger.addHandler(fileh) 
            logger.info('start')
            from threading import Thread
            
            my_worker.logger = logger
            
            
            if result is None:
                raise(Exception('abnormal register exit'))
            result_code, success_or_err = result
            if result_code == exit_code.fail:
                if type(success_or_err) is pymongo.errors.DuplicateKeyError:
                    raise(success_or_err)
                pass
            else:
                # my_worker.interval_check_worker_availability()
                t = Thread(target = my_worker.interval_check_worker_availability)
                t.start()
                logger.info('worker registered as '+ worker_name)
                my_worker.worker_listen()
                worker_exit_code = my_worker.work()
                if worker_exit_code == exit_code.kill_worker:
                    break
                else:
                    fail_cnt = 0
                if worker_exit_code == exit_code.break_current_and_continue:
                    continue
                # connections.client['log'] ['controller_log'].insert_one({'event count' : event_cnt})
                logger.info('this line should show up once when the controller start up with resume after enabled, when the resume after queue is emptied')
            
        except Exception as e:
            
            my_worker.check_worker_on = False
            if strict_mode:
                raise
            fail_cnt +=1
            num+=1
    
    # import threading
    
    
    # x = threading.Thread(target=my_worker.work)
    # x.start()
    
    pass

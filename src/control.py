


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


class exit_code_class:
    kill_worker = 0
    fail = 1
    break_current_and_continue = 2
    success = 3
exit_code = exit_code_class()

# @for_all_methods(debug_decorater)
class controller:
    # def atomic_auto_assign_new_data(self, new_doc, mongoClient):
    name : str
    #     pass
    def controller_register(self, controller_collection_name):
        # lower priority, as one controller should already be able to afford huge loads, not tested
        pass
    def atomic_assign_data(self, doc, mongoClient, db_from , col_from ,db_to,  col_to):
        from pymongo.read_concern import ReadConcern
        from pymongo.write_concern import WriteConcern
        from pymongo.read_preferences import ReadPreference
        wc_majority = WriteConcern("majority", wtimeout=2000)
        
        from copy import deepcopy
        import numpy as np, pickle
        to_send = deepcopy(doc)
        to_send.pop('_id')
        collection_from = mongoClient[db_from][col_from]
        collection_to= mongoClient[db_to][col_to]
        session=  mongoClient.start_session()
        logging_info = 'packet id' + str(doc['_id']) + ' assigned to worker ' + str(worker_name)
        logging.info(logging_info + ' start')
        session.start_transaction(read_concern=ReadConcern('local'),
                                  write_concern=wc_majority)
        logging_info = 'packet id' + str(doc['_id']) + ' assigned to worker ' + str(worker_name)
        # Important:: You must pass the session to the operations.
        collection_from.find_one_and_delete({'_id' : doc['_id']}, session=session)
        collection_to.replace_one({'_id' : doc['_id']}, doc , upsert = True, session = session)
        mongoClient['log'] ['controller_log'].insert_one({'info' : logging_info, "utctime": datetime.datetime.utcnow()}, session = session)
        session.commit_transaction()
        session.end_session()
        logging.info(logging_info + ' end')
        
        pass
    def worker_register(self, worker_collection_name = None, registration_collection = 'availableController'):
        assert not (worker_collection_name  in [i["_id"]for i in self.client[worker_db]['availableController'].find()])
        self.worker_collection_name = worker_collection_name
        self.name = worker_collection_name
        self.registration_collection = registration_collection
        if worker_collection_name not in self.client[worker_db].list_collection_names():
            self.client[worker_db][worker_collection_name].insert_one({'tag':'beginning'})
            
            time.sleep(0.5)
        try: 
            insert_result  = self.client[worker_db][registration_collection].insert_one({'_id' : worker_collection_name, 
                                                             'free-since' : int(time.time()),
                                                             'alive' : True
                                                             })
        except pymongo.errors.DuplicateKeyError as e:
            # logger.info('worker registered, try next, future will implement to test if that worker is dead and resume its role')
            return (exit_code.fail, e)
        
        # self.dataStream = 
        return exit_code.success, None
        pass
    def worker_listen(self,):
        worker_collection_name = self.worker_collection_name
        self.logger.info("listening to " + worker_db + worker_collection_name)
        self.eventStream = self.client[worker_db][worker_collection_name].watch()
        event_stream_pipeline = [{"$match" : 
                                  {'operationType' :
                                   {"$in" : 
                                    ['insert', 'delete']
                                    }
                                   }
                                  }]
        event_stream_pipeline = []
        
        
        try:
            resume_token = connections.client['eventTrigger']['resume_token'].find_one()['value']
            # resume_token = bytes(resume_token, 'utf-8')
            self.dataStream = connections.client['eventTrigger']['data_packet_input'].watch(resume_after = resume_token)
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
    check_worker_on = True
    def interval_check_worker_availability(self, interval = 120):
        while(self.check_worker_on):
            self.find_and_remove_all_MIA_worker_availability()
            t = Thread(target = self.find_and_remove_all_MIA_worker_availability)
            t.start()
            time.sleep(interval)
        pass
    def find_and_remove_all_MIA_worker_availability(self):
        # if a worker missing in action, remove that worker from collection jobboard
        # print('find and remove fake worker')
        worker_cursor = self.client[worker_db]['availableWorker'].find({})
        for worker_record in worker_cursor:
            worker_name = worker_record['_id']
            # self.find_and_remove_MIA_worker_availability(worker_name)
            # print('found worker', worker_name)
            from threading import Thread
            t = Thread(target = self.find_and_remove_MIA_worker_availability, args = (worker_name,))
            t.start()
        # print('all worker checked')
        pass

    
    def find_and_remove_MIA_worker_availability(self, worker_name):
        # if a worker missing in action, remove that worker from collection jobboard
        # print('heartbeating', worker_name)
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
            # kill_worker
            self.client[worker_db]['availableWorker'].delete_many({"_id": worker_name} )
            pass
        else:
            self.client['log']['health_history'].insert_one(record)
        session.commit_transaction()
        session.end_session()
        logging.info('removed worker' + worker_name + 'from availableWorker')
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
        self.atomic_assign_data(fullDocument,mongoClient, db_from, col_from, db_to, col_to)
        
        # logger.info(logging_info)
        
        pass
    
    def __init__(self):
        self.being_kill = False
        import connections
        self.client = connections.client
        pass
    @staticmethod
    def assert_worker_exist(worker_name):
        if not worker_name.startswith('test_worker') and not worker_name.startswith(worker_db):
            raise(ValueError('bad collection name : '+ worker_name))
            pass
        if (not(worker_name  in  connections.client[worker_db].list_collection_names())
            and  connections.client[worker_db]['availableWorker'].find_one({'_id' : worker_name}) is None):
            raise(ValueError('collection name not in worker collections'))
            pass
        pass
    @staticmethod
    def kill_worker(worker_name):
        controller.assert_worker_exist(worker_name)
        import worker_command
        import data_ref as dr
        # kill_command =worker_command.worker_command('kill')
        my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)
        my_data_ref.data_insert(data = worker_command.command_kill_worker(worker_name = worker_name), connectionStr = None, mongoClient = connections.client)
        pass
    
    def get_worker_health(self,worker_name):
        import worker_command
        import data_ref as dr
        controller.assert_worker_exist(worker_name)
        get_health_command = worker_command.command_report_health(worker_name, self.name)
        my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)
        my_data_ref.data_insert(get_health_command, mongoClient = connections.client)
        pass
    
    @staticmethod
    def worker_reload(worker_name):
        controller.assert_worker_exist(worker_name)
        import worker_command
        import data_ref as dr
        reload_code_command =worker_command.worker_command('reload_code')
        my_data_ref = dr.data_ref(db = worker_db, collection = worker_name)
        my_data_ref.data_insert(data = reload_code_command, connectionStr = None, mongoClient = connections.client)
        pass
    def worker_managerment(self,mongoClient):
        # kill or wake up sleeping worker
        c = mongoClient[worker_db]['availableWorker'].find()
        for worker in c:
            if int(time.time()) - worker['free-since'] > control_config['worker_time_out']:
                self.kill_worker(worker['_id'])
                pass
            pass
        pass    
    def kill_controller(self, controller_ref):
        raise NotImplementedError
        pass

    def process_dataStream(self, i):
        # logger.info(i)
        if 'command' in i['fullDocument']:
            # controller non specific commands here
            # if i['command'] == 'break':
            #     resume_token = i['_id']
            #     connections.client['eventTrigger']['resume_token'].replace_one({'_id': 'resume_token'}, 
            #                                                            {'_id': 'resume_token', 'value': resume_token['_data']},
            #                                                            upsert = True
            #                                                            )    
                # pass
            pass
        elif i['operationType'] == 'insert':

            self.routeDataStream(i['fullDocument'], connections.client)
        self.logger.info('b4 resume token')
        resume_token = i['_id']
        connections.client['eventTrigger']['resume_token'].replace_one({'_id': 'resume_token'}, 
                                                                       {'_id': 'resume_token', 'value': resume_token},
                                                                       upsert = True
                                                                       )
        self.logger.info('after resume token')
        pass
    def work(self):
        event_cnt = 0
        import threading
        
        # first process existing doc for controller
        for doc in self.client[worker_db][self.worker_collection_name].find({'tag' : None}):
            self.process_doc(doc) 
        # next listen to new controller commands
        y = threading.Thread(target = self.command_handler_threadable)
        y.start()
        # self.command_handler_threadable()
        try:
            for i in self.dataStream:
                event_cnt += 1
                self.logger.info('event count' + str( event_cnt))
                connections.client['log'] ['controller_log'].insert_one({'event count' : event_cnt, "utctime": datetime.datetime.utcnow()})
                if i['operationType'] == 'insert':
                    pass
                else:
                    continue
                from copy import deepcopy
                # time.sleep(1)
                
                if use_thread:
                    x = threading.Thread(target = self.process_dataStream, args = (deepcopy(i),))
                    x.start()
                else:
                    self.process_dataStream(i)
        except Exception as e:
            if e.args[0] == 'Error in $cursor stage :: caused by :: operation was interrupted':
                # ok
                pass
            else:
                self.logger.info('err')
                raise
            pass
        if self.being_kill:
            self.logger.info('=========== being killed ==========manage_new_data_for_execution')
            return exit_code.kill_worker
        # return self.manage_new_data_for_execution()
        pass
    def process_event_threadable(self, j):
        doc = j['fullDocument']
        # process doc here
        self.process_doc(doc)
        pass
    def process_event(self, i):
        from copy import deepcopy
        j = deepcopy(i)
        use_thread = True
        # print(i["_id"])
        
        if j['operationType'] == 'insert':
            
            # print(i["_id"])
            pass
        else:
            logging_info = ('discarded activity ' + j['operationType'])
            print(logging_info)
            self.logger.info(logging_info)
            return
        if use_thread:
            x = threading.Thread(target=self.process_event_threadable, args=(j,))
            x.start()
            
        else:
            self.process_event_threadable(j)
              
        if self.being_kill:
            logger.info('closing data stream')
            self.dataStream.close()
            logger.info('closing event stream')
            self.eventStream.close()
            self.client[worker_db][self.registration_collection].delete_one({'_id' : self.worker_collection_name})
            
            
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
        
        self.logger.info('thread....')
        for i in self.eventStream:
            self.process_event(i)
            if self.being_kill:
                break
        self.logger.info('end thread')
        pass
    def process_doc(self, doc):
        import dtype
        data = doc['data']
        if 'tag' in doc:
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
                pass
        else:  # process pickled data
            data_unpickled = pickle.loads(data)
            logger.info('command unpickled '+ 'type' + str(type(data_unpickled)))
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
            result = my_worker.worker_register(worker_collection_name = worker_name)
            
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
            # my_worker.interval_check_worker_availability()
            t = Thread(target = my_worker.interval_check_worker_availability)
            t.start()
            my_worker.worker_listen()
            if result is None:
                raise(Exception('abnormal register exit'))
            result_code, success_or_err = result
            if result_code == exit_code.fail:
                if type(success_or_err) is pymongo.errors.DuplicateKeyError:
                    raise(success_or_err)
                pass
            else:
                logger.info('worker registered as '+ worker_name)
                worker_exit_code = my_worker.work()
                if worker_exit_code == exit_code.kill_worker:
                    break
                else:
                    fail_cnt = 0
                if worker_exit_code == exit_code.break_current_and_continue:
                    continue
                # connections.client['log'] ['controller_log'].insert_one({'event count' : event_cnt})
                logger.info('this line should show up once when the controller start up with resume after enabled, when the resume after queue is emptied')
            
        except:
            my_worker.check_worker_on = False
            if strict_mode:
                raise
            fail_cnt +=1
            num+=1
    
    # import threading
    
    
    # x = threading.Thread(target=my_worker.work)
    # x.start()
    
    pass

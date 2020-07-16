import logging, pathlib, yaml, controller_command

with open(str(pathlib.Path(r'./control_config.yaml')), 'r') as file:
    control_config = yaml.safe_load(file)
strict_mode = False
if 'strict_mode' in control_config:
    strict_mode = control_config['strict_mode']
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
@for_all_methods(debug_decorater)
class controller:
    def atomic_auto_assign_new_data(self, new_doc, mongoClient):
        
        pass
    def controller_register(self, controller_collection_name):
        # lower priority, as one controller should already be able to afford huge loads, not tested
        pass
    def atomic_assign_data(self, doc, mongoClient):
        # 1. listen to doc event stream, break if removed
        # within the event loop try get free worker
        # if none free worker, listen for free worker add event
        # atomic transaction : 1. remove worker from free, 2. delete doc from in data packet stream, 3, add data packet to free worker
        
        
        
        pass
    def worker_register(self, worker_collection_name = None, registration_collection = 'availableController'):
        self.worker_collection_name = worker_collection_name
        self.name = worker_collection_name
        self.registration_collection = registration_collection
        try: 
            insert_result  = self.client['worker'][registration_collection].insert_one({'_id' : worker_collection_name, 
                                                             'free-since' : int(time.time()),
                                                             'alive' : True
                                                             })
        except pymongo.errors.DuplicateKeyError as e:
            logging.info('worker registered, try next, future will implement to test if that worker is dead and resume its role')
            return (exit_code.fail, e)
        logging.info("listening to " + 'worker' + worker_collection_name)
        self.eventStream = self.client['worker'][worker_collection_name].watch()
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
            logging.info('resume_after' + str(resume_token))
        except:
            self.dataStream = connections.client['eventTrigger']['data_packet_input'].watch()
        self.available_worker_event_stream = connections.client['worker']['availableWorker'].watch()
        # self.dataStream = 
        return exit_code.success, None
        pass
    
    def pop_free_worker(self, mongoClient, worker_colection  = "availableWorker"):
        
        availableWorker = mongoClient['worker']['availableWorker'].find_one_and_delete({})
        
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
            # logging.info(logging_info)
            for i in self.available_worker_event_stream:
                if i['operationType'] == 'insert':
                    availableWorker = i['fullDocument']
                    worker_name = availableWorker['_id']
                    logging_info = 'found free worker ' + worker_name
                    connections.client['log'] ['controller_log'].insert_one({'info' : logging_info})
                    # logging.info(logging_info)
                    worker_found = True
                    break
        
        worker_name = availableWorker['_id']
        logging.info('worker assign '+ worker_name)
        # ========need to be change to atomic operation
        insert_result = mongoClient['worker'][worker_name].insert_one(fullDocument)
        # mark fullDocument routed by moving
        logging_info = 'packet id' + str(fullDocument['_id']) + ' assigned to worker ' + str(worker_name)
        connections.client['log'] ['controller_log'].insert_one({'info' : logging_info})
        #========= atomic finish
        
        logging.info(logging_info )
        # logging.info(logging_info)
        
        pass
    
    def __init__(self):
        self.being_kill = False
        import connections
        self.client = connections.client
        pass
    @staticmethod
    def assert_worker_exist(worker_name):
        if not worker_name.startswith('test_worker') and not worker_name.startswith('worker'):
            raise(ValueError('bad collection name : '+ worker_name))
            pass
        if (not(worker_name  in  connections.client['worker'].list_collection_names())
            and  connections.client['worker']['availableWorker'].find_one({'_id' : worker_name}) is None):
            raise(ValueError('collection name not in worker collections'))
            pass
        pass
    @staticmethod
    def kill_worker(worker_name):
        controller.assert_worker_exist(worker_name)
        import worker_command
        import data_ref as dr
        kill_command =worker_command.worker_command('kill')
        my_data_ref = dr.data_ref(db = 'worker', collection = worker_name)
        my_data_ref.data_insert(data = kill_command, connectionStr = None, mongoClient = connections.client)
        pass
    @staticmethod
    def worker_reload(worker_name):
        controller.assert_worker_exist(worker_name)
        import worker_command
        import data_ref as dr
        reload_code_command =worker_command.worker_command('reload_code')
        my_data_ref = dr.data_ref(db = 'worker', collection = worker_name)
        my_data_ref.data_insert(data = reload_code_command, connectionStr = None, mongoClient = connections.client)
        pass
    def worker_managerment(self,mongoClient):
        # kill or wake up sleeping worker
        c = mongoClient['worker']['availableWorker'].find()
        for worker in c:
            if int(time.time()) - worker['free-since'] > control_config['worker_time_out']:
                self.kill_worker(worker['_id'])
                pass
            pass
        pass    
    def kill_controller(self, controller_ref):
        
        pass

    def process_dataStream(self, i):
        # logging.info(i)
        if 'command' in i:
            # controller non specific commands here
            if i['command'] == 'break':
                resume_token = i['_id']
                connections.client['eventTrigger']['resume_token'].replace_one({'_id': 'resume_token'}, 
                                                                       {'_id': 'resume_token', 'value': resume_token['_data']},
                                                                       upsert = True
                                                                       )    
                pass
        
        elif i['operationType'] == 'insert':

            self.routeDataStream(i['fullDocument'], connections.client)
        logging.info('b4 resume token')
        resume_token = i['_id']
        connections.client['eventTrigger']['resume_token'].replace_one({'_id': 'resume_token'}, 
                                                                       {'_id': 'resume_token', 'value': resume_token},
                                                                       upsert = True
                                                                       )
        logging.info('after resume token')
        pass
    def work(self):
        return self.manage_new_data_for_execution()
        pass
    def to_be_threaded_to_handle_controller_command(self):
        '''
        import controller_command,connections
        import data_ref as dr
        controller_to_kill = 'test_controller0'
        kill_command = controller_command.controller_command('kill')
        my_data_ref = dr.data_ref(db = 'worker', collection = controller_to_kill)
        my_data_ref.data_insert(data = kill_command, connectionStr = None, mongoClient = connections.client)
        '''
        
        logging.info('thread....')
        for i in self.eventStream:
            doc = i['fullDocument']
            data_unpickled = pickle.loads(doc ['data'])
            logging.info('command unpickled '+ 'type' + str(type(data_unpickled)))
            if type(data_unpickled) is controller_command.controller_command:
                if type(data_unpickled.command) is str:
                    if data_unpickled.command == 'kill':
                        logging.info('kill_command_received')
                        self.being_kill = True
                if type(data_unpickled.command) is dict:
                    if 'kill' in data_unpickled.command:
                        if self.command['kill'] == self.worker_collection_name:
                            logging.info('kill_command_received')
                            self.being_kill = True
                        
                if self.being_kill:
                    logging.info('closing data stream')
                    self.dataStream.close()
                    logging.info('closing event stream')
                    self.eventStream.close()
                    self.client['worker'][self.registration_collection].delete_one({'_id' : self.worker_collection_name})
                    
                    break
                pass
            
            pass
        logging.info('end thread')
        pass
        
    def manage_new_data_for_execution(self):
        event_cnt = 0
        import threading
        y = threading.Thread(target = self.to_be_threaded_to_handle_controller_command)
        y.start()
        # self.to_be_threaded_to_handle_controller_command()
        try:
            for i in self.dataStream:
                event_cnt += 1
                # logging.info('event count', event_cnt)
                connections.client['log'] ['controller_log'].insert_one({'event count' : event_cnt})
                if i['operationType'] == 'insert':
                    pass
                else:
                    continue
                from copy import deepcopy
                # time.sleep(1)
                use_thread = True
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
                logging.info('err')
                raise
            pass
        if self.being_kill:
            logging.info('=========== being killed ==========manage_new_data_for_execution')
            return exit_code.kill_worker
        # reach here if resume after is done
        
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
            result = my_worker.worker_register(worker_collection_name = worker_name)
            log_f_name = str(pathlib.Path( worker_name + '.log'))
            logging.basicConfig(filename=log_f_name, filemode='a', 
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
            import os
            print('log file set as ' + str(pathlib.Path(os.getcwd()) / log_f_name))
            logging.info('start...')
            
            if result is None:
                raise(Exception('abnormal register exit'))
            result_code, success_or_err = result
            if result_code == exit_code.fail:
                if type(success_or_err) is pymongo.errors.DuplicateKeyError:
                    raise(success_or_err)
                pass
            else:
                logging.info('worker registered as '+ worker_name)
                worker_exit_code = my_worker.work()
                if worker_exit_code == exit_code.kill_worker:
                    break
                else:
                    fail_cnt = 0
                if worker_exit_code == exit_code.break_current_and_continue:
                    continue
                # connections.client['log'] ['controller_log'].insert_one({'event count' : event_cnt})
                logging.info('this line should show up once when the controller start up with resume after enabled, when the resume after queue is emptied')
            
        except:
            if strict_mode:
                raise
            fail_cnt +=1
            num+=1
    
    # import threading
    
    
    # x = threading.Thread(target=my_worker.work)
    # x.start()
    
    pass

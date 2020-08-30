worker_db  = 'pc_worker'
import data_ref, worker_command, pathlib, yaml
from datetime import datetime
import logging, time, threading, pymongo
with open(str(pathlib.Path(r'./worker_config.yaml')), 'r') as file:
    worker_config = yaml.safe_load(file)
strict_mode = False
if 'strict_mode' in worker_config:
    strict_mode = worker_config['strict_mode']
use_thread = False
if 'use_thread' in worker_config:
    use_thread = worker_config['use_thread']    
class exit_code_class:
    kill_worker = 0
    fail = 1
    break_current_and_continue = 2
import time
exit_code = exit_code_class()
from  pymongo.errors import DuplicateKeyError
global process_id
process_id = set()
with open(str(pathlib.Path(r'./config.yaml')), 'r') as file:
    config = yaml.safe_load(file)    
worker_db = config['worker_db']
from _base_worker import base_worker

class worker(base_worker):
    check_worker_on = False
    def __init__(self):
        self.worker_db = worker_db
        self.being_kill = False
        import connections
        self.client = connections.client
        pass

    def worker_listen(self):
        self.eventStream = self.client[self.worker_db][self.worker_collection_name].watch()
    def worker_instance_report_alive(self):
        self.client[worker_db]['availableWorker'].update_one(
                {"_id" : self.worker_collection_name} , 
                {'$set' : {'alive' : True}}
            )
        pass
    def worker_report_free(self):
        self.client[worker_db]['availableWorker'].update_one(
                {"_id" : self.worker_collection_name} , 
                {
                    '$set' : {'alive' : True, 'free-since' : int(time.time())}
                }
            )
        pass
    
    # def process_worker_command(self, command : worker_command.worker_command):
    #     if 'kill' in command.command_json:
    #         logging_info = 'killing worker ' + str(self.worker_collection_name) +' by worker_command.worker_command'
    #         self.logger.info(logging_info)
    #         self.client[worker_db]['availableWorker'].remove({'_id': self.worker_collection_name})
    #         self.vomit_job_back_to_queue()
    #         self.client[worker_db][self.worker_collection_name].drop()
            
            
    #         print(logging_info)
    #         self.being_kill = True
    #         self.eventStream.close()
    #         return exit_code.kill_worker
    #     if 'reload_code' in command.command_json :
    #         import builtins
    #         from IPython.lib import deepreload
    #         builtins.reload = deepreload.reload
    #         logging_info = 'worker ' + self.worker_collection_name + ' reloaded code'
    #         print(logging_info)
    #         self.logger.info(logging_info)
            
    #         return
    #     if 'spawn_new_adjacent_worker' in command.command_json :
    #         self.spawn_new_worker()
    #         pass
    #     if 'report_health' in command.command_json:
    #         report_to = command.command_json['report_health'] ['to']
    #         self.report_health(receiver = report_to)
    #         pass
    #     pass

    def kill_worker(self):
        from errorType import worker_method_unauthorized
        print('worker type not authorized to run this method')
        raise
        pass


    def vomit_job_back_to_queue(self):
        
        pipeline = []
        stage = { "$merge": { "into": { "db":"eventTrigger", "coll":"data_packet_input" }, "on": "_id", "whenMatched": "keepExisting", "whenNotMatched": "insert" } }
        pipeline.append(stage)
        self.client[worker_db][self.name].aggregate(pipeline)
        
        pass
    def logging_doc_results(self, i):
        from copy import deepcopy
        j = deepcopy(i)
        import time
        # time.sleep(1)
        self.event_cnt+=1
        event_cnt = self.event_cnt
        global process_id
        # try:   removed due to mongo db provide good ID already
        #     assert j["_id"]['_data'] not in process_id
        #     process_id.add( j["_id"]['_data'])
        #     self.client['log']['log'].insert_one( { 'packetID' : j["_id"]['_data'],
        #                                            'activity'  : 'threadStarted' } )
        # except KeyboardInterrupt:
        #     raise
        # except AssertionError:
        #     logging_info = j["_id"] + ' already in process_id'
        #     print(logging_info)
        #     self.logger.info(logging_info)
        
        logging_info = 'processed' + str(event_cnt) + 'packets'
        print(logging_info)
        self.logger.info(logging_info)

    def process_command_or_data(self, doc):
        import pickle
        data_unpickled = pickle.loads(doc ['data'])
        print('data_tyoe '+ str(type(data_unpickled)) + ' received')
        # print('data type : '  , type(data_unpickled) )
        try:
            if  type(data_unpickled) is data_ref.data_ref:
                
                data = data_unpickled.deref_data(mongoClient = self.client)
                self.logger.info('deref_data')
                # print('deref_data')
            elif  issubclass(type(data_unpickled),  worker_command.worker_command):
                self.process_common_command(command = data_unpickled)
                return
                pass
            else :
                # assume data_unpickled is direct data
                data = data_unpickled
                self.logger.info('raw_data')
            import process_data
        
            process_data.process_data(data)
        except Exception as e:
            
            from data_ref import _base_dd_insert
            err_data_pickled, err_data_ref = _base_dd_insert(e, 'error', 'py_Exception_Store', 'error', 'py_Exception_Ref' ,mongoClient = self.client)
            from copy import deepcopy
            err_doc = deepcopy(doc)
            err_doc['data_ref_error_doc_ID'] = err_data_ref.documentID
            from _base_worker import mongo_transaction
            db_from = worker_db
            col_from = self.name
            db_to = 'error'
            col_to = 'error_job'
            logging_info = 'error found' + e.__repr__()
            mongo_transaction(err_doc, self.client, db_from, col_from, db_to, col_to, logging_info)
            
            

    def check_document_integrity(self, doc):
        if 'tag' in doc:
            return False
        if 'data' not in doc:
            print('irrelevant data format inputted', doc)
            return False
        else:
            return True
        pass
    def process_event(self, i):  # worker thread process data
        from copy import deepcopy
        j = deepcopy(i)
        
        if j['operationType'] == 'insert':
            
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
    def update_doc_start_process(self, doc):
        myid = doc["_id"]
        self.client[worker_db][self.worker_collection_name].update_one(
                {
                    "_id" : myid, 
                },
                {"$set" : {'worker_start_time' : datetime.now()}}
                
            )
    def update_doc_end_process(self, doc):
        myid = doc["_id"]
        self.client[worker_db][self.worker_collection_name].update_one(
                {
                    "_id" : myid, 
                },
                {"$set" : {'worker_end_time' : datetime.now()}}
                
            )
    def process_event_threadable(self, j):   # worker thread process data
        # process existing doc
        
        doc = j['fullDocument']
        if not self.check_document_integrity(doc):
            return
        self.update_doc_start_process(doc)
        self.process_doc(doc)
        self.update_doc_end_process(doc)
        self.record_post_threadable_event(j)        # worker level log 
    def record_post_threadable_event(self, j):
        """
        make the logs after task finish,
        log time and event description
        """
        self.logging_doc_results(j)   # doc level logging
        try:
            insert_result = self.client[worker_db]['availableWorker'].insert_one(
                {'_id' : self.worker_collection_name,
                 'free-since' : int(time.time())})
        except DuplicateKeyError:
            pass
        logging_info = self.worker_collection_name + ' is now free'
        print(logging_info)
        self.logger.info(logging_info)
        doc = j['fullDocument']
        
        log_packet_ref_on = True
        if log_packet_ref_on:
            
            from _base_worker import processed_data_col, mongo_transaction
            mongo_client = self.client
            db_from = worker_db
            db_to   = "eventTrigger"
            col_to = processed_data_col
            col_from = self.worker_collection_name
            collection_from = mongo_client[db_from][col_from]
            collection_to= mongo_client[db_to][col_to]
            logging_info = 'packet id' + str(doc['_id']) + ' finished and logged to ' + col_to +" " +col_to
            mongo_transaction(doc , mongo_client, db_from, col_from, db_to, col_to, logging_info)
            
        else:
            self.client[worker_db][self.worker_collection_name].delete_one(doc)
        logging_info = ('packet with _id', str(j['fullDocument']["_id"]) 
                        + 'processed and removed from worker')
        print(logging_info)
        self.logger.info(logging_info)
        self.client['log']['log'].insert_one( 
            { 'packetID' : str(j['fullDocument']["_id"]) ,
             'activity'  : 'data_processed' } )
        pass
        

    def command_handler_threadable(self):
        pass


    
    def pre_work(self):
        """
        initialisations before worker start working
        related to worker and control implementation specifically for eachg worker

        """
        
        self.event_cnt = 0
        
        self.work_listenStream = self.eventStream
        self.changeStream_process_callback = self.process_event
        self.process_doc_callback = self.process_command_or_data
        
        pass
    def filter_eventStream(self, i):
        return True
        pass


    
    
    
    
    
    pass


if __name__ == '__main__':
    import worker as wk
    # import data_ref as dr
    my_worker = wk.worker()
    
    worker_name_prefix = 'test_worker'
    num = 0
    fail_cnt = 0
    try:
        while fail_cnt < 10:
            
            worker_name = worker_name_prefix  + str(num)
            from errorType import duplicate_worker_name_error
            try:
                my_worker.worker_register(worker_collection_name = worker_name)
            
            except (pymongo.errors.DuplicateKeyError, duplicate_worker_name_error) as e:
                
                fail_cnt +=1
                num+=1
                if fail_cnt >= 10:
                    print('maximum retry exceeded')
                    raise
                    
                continue
            
            FORMAT = "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"
            
            logger = logging.getLogger(__file__ + '_' + worker_name)
            for hdlr in logger.handlers[:]:  # remove all old handlers
                logger.removeHandler(hdlr)
            formatter = logging.Formatter(FORMAT)
            fileh = logging.FileHandler('./' +worker_name + '.log', 'a')
            fileh.setFormatter(formatter)
            logger.addHandler(fileh) 
            logger.setLevel(logging.DEBUG)
            logger.info('start')
            logging_info = 'worker registered as '+ worker_name
            print(logging_info)
            logger.info(logging_info)
            my_worker.logger = logger
            my_worker.worker_listen()
            worker_exit_code = my_worker.work()
            if worker_exit_code == exit_code.kill_worker:
                break
            else:
                fail_cnt = 0
            if worker_exit_code == exit_code.break_current_and_continue:
                continue
    except KeyboardInterrupt:
        import sys
        print('bye :)')
        sys.exit(0)
    except :
        raise
    # import threading
    
    
    # x = threading.Thread(target=my_worker.work)
    # x.start()
    
    pass

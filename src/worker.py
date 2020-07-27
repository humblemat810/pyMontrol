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
class worker:
    
    def __init__(self):
        self.being_kill = False
        import connections
        self.client = connections.client
        pass
    def worker_register(self, worker_collection_name = None,registration_collection = 'availableWorker'):
        self.worker_collection_name = worker_collection_name
        self.name = worker_collection_name
        self.registration_collection = registration_collection
        insert_result  = self.client[worker_db][registration_collection].insert_one({'_id' : worker_collection_name, 
                                                             'free-since' : int(time.time()),
                                                             'alive' : True
                                                             })
        if worker_collection_name not in self.client[worker_db].list_collection_names():
            self.client[worker_db][worker_collection_name].insert_one({'tag':'beginning'})
            
            time.sleep(0.5)
        self.eventStream = self.client[worker_db][worker_collection_name].watch()
        pass
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
    def process_command(self, command : worker_command.worker_command):
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
    def vomit_job_back_to_queue(self):
        
        pipeline = []
        stage = { "$merge": { "into": { "db":"eventTrigger", "coll":"data_packet_input" }, "on": "_id", "whenMatched": "keepExisting", "whenNotMatched": "insert" } }
        pipeline.append(stage)
        self.client[worker_db][self.name].aggregate(pipeline)
        
        pass
    def process_doc(self, doc):
        if 'data' not in doc:
            print('irrelevant data format inputted', doc)
            return
        import pickle
        data_unpickled = pickle.loads(doc ['data'])
        
        # print('data type : '  , type(data_unpickled) )
        if  type(data_unpickled) is data_ref.data_ref:
            
            data = data_unpickled.deref_data(mongoClient = self.client)
            self.logger.info('deref_data')
            # print('deref_data')
        elif  issubclass(type(data_unpickled),  worker_command.worker_command):
            self.process_command(command = data_unpickled)
            return
            pass
        else :
            # assume data_unpickled is direct data
            data = data_unpickled
            self.logger.info('raw_data')
        import process_data
        process_data.process_data(data)
    def report_health(self, receiver):
        import psutil
        from dtype import health_report
        self.health_report = health_report( {'dtype' : 'dtype.health_report',   'data' : {'status':'alive', 'sender' : self.name, 'virtual_memory' : dict(psutil.virtual_memory()._asdict()),
                       'cpu' : psutil.cpu_percent()}})
        self.client[worker_db][receiver].insert_one(self.health_report)
        pass
    def process_event_threadable(self, j):
        # process existing doc
        
        
        doc = j['fullDocument']
        if 'data' not in doc:
            print('irrelevant data format inputted', doc)
            return
        self.process_doc(doc)
        try:
            insert_result = self.client[worker_db]['availableWorker'].insert_one({'_id' : self.worker_collection_name,
                                                             'free-since' : int(time.time())})
        except DuplicateKeyError:
            pass
        logging_info = self.worker_collection_name + ' is now free'
        print(logging_info)
        self.logger.info(logging_info)
        self.client[worker_db][self.worker_collection_name].delete_one(doc)
        logging_info = 'packet with _id', j["_id"]['_data'] + 'processed and removed from worker'
        print(logging_info)
        self.logger.info(logging_info)
        self.client['log']['log'].insert_one( { 'packetID' : j["_id"]['_data'],
                                                       'activity'  : 'data_processed' } )
        pass
        
    def spawn_new_worker(self):
        from subprocess import Popen
        Popen('python', __file__)
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
            
            
        import time
        # time.sleep(1)
        qcnt+=1
        global process_id
        try:
            assert j["_id"]['_data'] not in process_id
            process_id.add( j["_id"]['_data'])
            self.client['log']['log'].insert_one( { 'packetID' : j["_id"]['_data'],
                                                   'activity'  : 'threadStarted' } )
        except KeyboardInterrupt:
            raise
        except AssertionError:
            logging_info = j["_id"] + ' already in process_id'
            print(logging_info)
            self.logger.info(logging_info)
        
            
        
        logging_info = 'processed' + str(qcnt) + 'packets'
        print(logging_info)
        self.logger.info(logging_info)
    def command_handler_threadable(self):
        pass
    def work(self):
        from queue import Queue
        max_Queue_cnt = 10
        qcnt = 0
        # q = Queue()
        
        # y = threading.Thread(target = self.command_handler_threadable)
        # y.start()
        
        
        
        for doc in self.client[worker_db][self.worker_collection_name].find({'tag' : None}):
            self.process_doc(doc)
        try:
            
            for i in self.eventStream:
                self.process_event(i)
        except KeyboardInterrupt:
            self.being_kill = True
            import worker_command
            command = worker_command.worker_command()
            command.kill_worker(self.name)
            self.process_command(command)
            raise
   
        if self.being_kill:
            return exit_code.kill_worker
            

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
            try:
                my_worker.worker_register(worker_collection_name = worker_name)
            except pymongo.errors.DuplicateKeyError:
                
                fail_cnt +=1
                num+=1
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

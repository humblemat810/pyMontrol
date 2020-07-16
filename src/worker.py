
import data_ref, worker_command, pathlib, yaml
from datetime import datetime
import logging, time, threading
with open(str(pathlib.Path(r'./worker_config.yaml')), 'r') as file:
    worker_config = yaml.safe_load(file)
strict_mode = False
logger = None
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
logging.basicConfig(filename=__file__, filemode='a', 
                    format="%(asctime)s â€” %(name)s â€” %(levelname)s â€” %(funcName)s:%(lineno)d â€” %(message)s",
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__file__)
class worker:
    
    def __init__(self):
        self.being_kill = False
        import connections
        self.client = connections.client
        pass
    def worker_register(self, worker_collection_name = None,registration_collection = 'availableWorker'):
        self.worker_collection_name = worker_collection_name
        self.registration_collection = registration_collection
        insert_result  = self.client['worker'][registration_collection].insert_one({'_id' : worker_collection_name, 
                                                             'free-since' : int(time.time()),
                                                             'alive' : True
                                                             })
        if worker_collection_name not in self.client['worker'].list_collection_names():
            self.client['worker'][worker_collection_name].insert_one({'tag':'beginning'})
            
            time.sleep(0.5)
        self.eventStream = self.client['worker'][worker_collection_name].watch()
        pass
    def worker_instance_report_alive(self):
        self.client['worker']['availableWorker'].update_one(
                {"_id" : self.worker_collection_name} , 
                {'$set' : {'alive' : True}}
            )
        pass
    def worker_report_free(self):
        self.client['worker']['availableWorker'].update_one(
                {"_id" : self.worker_collection_name} , 
                {
                    '$set' : {'alive' : True, 'free-since' : int(time.time())}
                }
            )
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
            logger.info('deref_data')
            # print('deref_data')
        elif type(data_unpickled) is worker_command.worker_command:
            if data_unpickled.command_str == 'kill':
                logging_info = 'killing worker ' + str(self.worker_collection_name) +' by worker_command.worker_command'
                logger.info(logging_info)
                self.client['worker'][self.worker_collection_name].drop()
                self.client['worker']['availableWorker'].remove({'_id': self.worker_collection_name})
                print(logging_info)
                self.being_kill = True
                self.eventStream.close()
                return exit_code.kill_worker
            if data_unpickled.command_str == 'reload_code':
                import builtins
                from IPython.lib import deepreload
                builtins.reload = deepreload.reload
                logging_info = 'worker ' + self.worker_collection_name + ' reloaded code'
                print(logging_info)
                logger.info(logging_info)
                
                return
            pass
        else :
            # assume data_unpickled is direct data
            data = data_unpickled
            logger.info('raw_data')
        import process_data
        process_data.process_data(data)
    def process_eventStream(self,j):
        # process existing doc
        
        
        doc = j['fullDocument']
        if 'data' not in doc:
            print('irrelevant data format inputted', doc)
            return
        self.process_doc(doc)
        try:
            insert_result = self.client['worker']['availableWorker'].insert_one({'_id' : self.worker_collection_name,
                                                             'free-since' : int(time.time())})
        except DuplicateKeyError:
            pass
        logging_info = self.worker_collection_name + ' is now free'
        print(logging_info)
        logger.info(logging_info)
        self.client['worker'][self.worker_collection_name].delete_one(doc)
        logging_info = 'packet with _id', j["_id"]['_data'] + 'processed and removed from worker'
        print(logging_info)
        logger.info(logging_info)
        self.client['log']['log'].insert_one( { 'packetID' : j["_id"]['_data'],
                                                       'activity'  : 'data_processed' } )
        pass
        
        
    def work(self):
        from queue import Queue
        max_Queue_cnt = 10
        qcnt = 0
        # q = Queue()
        for doc in self.client['worker'][self.worker_collection_name].find({'tag' : None}):
            self.process_doc(doc)
        for i in self.eventStream:
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
                logger.info(logging_info)
                continue
            
            if use_thread:
                x = threading.Thread(target=self.process_eventStream, args=(j,))
                x.start()
                # q.put(x)
                
            else:
                self.process_eventStream(j)
            import time
            # time.sleep(1)
            qcnt+=1
            global process_id
            try:
                assert j["_id"]['_data'] not in process_id
                process_id.add( j["_id"]['_data'])
                self.client['log']['log'].insert_one( { 'packetID' : j["_id"]['_data'],
                                                       'activity'  : 'threadStarted' } )
            except:
                logging_info = j["_id"] + ' already in process_id'
                print(logging_info)
                logger.info(logging_info)
                
            
            logging_info = 'processed' + str(qcnt) + 'packets'
            print(logging_info)
            logger.info(logging_info)
            # if q.qsize() > max_Queue_cnt:
            #     my_list = []
            #     while not q.empty():
            #         my_list.append(q.get())
            #     for i in my_list:
            #         i.join()
            
            
        
                    
            
            # self.process_eventStream(i)
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
    while fail_cnt < 10:
        try:
            worker_name = worker_name_prefix  + str(num)
            my_worker.worker_register(worker_collection_name = worker_name)
            FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
            
            logger = logging.getLogger(__file__ + '_' + worker_name)
            for hdlr in logger.handlers[:]:  # remove all old handlers
                logger.removeHandler(hdlr)
            formatter = logging.Formatter(FORMAT)
            fileh = logging.FileHandler('./' +worker_name + '.log', 'a')
            fileh.setFormatter(formatter)
            logger.addHandler(fileh) 
            logger.info('start')
            logging_info = 'worker registered as '+ worker_name
            print(logging_info)
            logger.info(logging_info)
            worker_exit_code = my_worker.work()
            if worker_exit_code == exit_code.kill_worker:
                break
            else:
                fail_cnt = 0
            if worker_exit_code == exit_code.break_current_and_continue:
                continue
        except:
            # raise
            fail_cnt +=1
            num+=1
    
    # import threading
    
    
    # x = threading.Thread(target=my_worker.work)
    # x.start()
    
    pass
2020-07-16 22:04:43 — C:\Users\ASUS\Documents\python\pymontroller\src\worker.py_test_worker1 — INFO — <module>:217 — start
2020-07-16 22:04:43 — C:\Users\ASUS\Documents\python\pymontroller\src\worker.py_test_worker1 — INFO — <module>:220 — worker registered as test_worker1

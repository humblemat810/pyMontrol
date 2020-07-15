
import data_ref, worker_command
import logging, time, threading
class exit_code_class:
    kill_worker = 0
    fail = 1
    break_current_and_continue = 2
    
exit_code = exit_code_class()
from  pymongo.errors import DuplicateKeyError

class worker:
    
    def __init__(self):
        self.being_kill = False
        import connections
        self.client = connections.client
        pass
    def worker_register(self, worker_collection_name = None):
        self.worker_collection_name = worker_collection_name
        insert_result  = self.client['worker']['availableWorker'].insert_one({'_id' : worker_collection_name, 
                                                             'free-since' : int(time.time()),
                                                             'alive' : True
                                                             })
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
    def process_eventStream(self,i):
        
        logging.info('event found :'+ str(i))
        
        if i['operationType'] == 'insert':
            doc = i['fullDocument']
            import pickle
            data_unpickled = pickle.loads(doc ['data'])
            
            print('data type : '  , type(data_unpickled) )
            if  type(data_unpickled) is data_ref.data_ref:
                
                data = data_unpickled.deref_data(mongoClient = self.client)
                logging.info('deref_data')
                print('deref_data')
            elif type(data_unpickled) is worker_command.worker_command:
                if data_unpickled.command_str == 'kill':
                    logging_info = 'killing worker ' + str(self.worker_collection_name) +' by worker_command.worker_command'
                    logging.info(logging_info)
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
                    logging.info(logging_info)
                    
                    return
                pass
            else :
                # assume data_unpickled is direct data
                data = data_unpickled
                logging.info('raw_data')
            import process_data
            process_data.process_data(data)
            try:
                insert_result = self.client['worker']['availableWorker'].insert_one({'_id' : self.worker_collection_name,
                                                                 'free-since' : int(time.time())})
            except DuplicateKeyError:
                pass
            print(self.worker_collection_name + ' is now free')
            self.client['worker'][self.worker_collection_name].delete_one(doc)
            print('packet with _id', doc['_id'], 'processed and removed from worker')
            pass
        pass
        pass
    def work(self):
        from queue import Queue
        max_Queue_cnt = 10
        qcnt = 0
        q = Queue()
        for i in self.eventStream:
            use_thread = False
            if use_thread:
                x = threading.Thread(target=self.process_eventStream, args=(i,))
                x.start()
            else:
                self.process_eventStream(i)
            # q.put(x)
            # qcnt+=1
            # if q.qsize() > max_Queue_cnt:
            #     my_list = []
            #     while not q.empty():
            #         my_list.append(q.get())
                
                    
            
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
            logging.basicConfig(filename='./' + worker_name + '.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
            my_worker.worker_register(worker_collection_name = worker_name)
            print('worker registered as ', worker_name)
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

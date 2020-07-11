# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 21:10:45 2020

@author: pchan
"""

import logging, time
class worker:
    def __init__(self):
        import connections
        self.client = connections.client
        pass
    def worker_register(self, worker_collection_name = None):
        self.worker_collection_name = worker_collection_name
        self.client['worker']['availableWorker'].insert_one({'_id' : worker_collection_name, 
                                                             'free-since' : int(time.time()),
                                                             'alive' : True
                                                             })
        self.eventStream = self.client['worker'][_id].watch()
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
    def work(self):
        import data_ref, worker_command
        for i in self.eventStream:
            logging.info('event found :'+ str(i))
            
            if i['operationType'] == 'insert':
                doc = i['fullDocument']
                import pickle
                data_unpickled = pickle.loads(doc ['data'])
                
                
                if  type(data_unpickled) is data_ref.data_ref:
                    
                    data = data_unpickled.deref_data(mongoClient = self.client)
                    logging.info('deref_data')
                elif type(data_unpickled) is worker_command.worker_command:
                    if data_unpickled.command_str == 'kill':
                        logging.info('killing worker by worker_command.worker_command')
                        break
                    pass
                else :
                    # assume data_unpickled is direct data
                    data = data_unpickled
                    logging.info('raw_data')
                import process_data
                process_data.process_data(data)
                self.client['worker']['availableWorker'].insert_one({'_id' : self.worker_collection_name,
                                                                     'free-since' : int(time.time())})
                self.client['worker'][self.worker_collection_name].delete_one(doc)
                pass
            pass
            
            

        pass
    
    pass


if __name__ == '__main__':
    import worker as wk
    # import data_ref as dr
    my_worker = wk.worker()
    worker_name= 'test_worker1'
    
    logging.basicConfig(filename='./' + worker_name + '.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    my_worker.worker_register(worker_collection_name = worker_name)
    my_worker.work()
    
    # import threading
    
    
    # x = threading.Thread(target=my_worker.work)
    # x.start()
    
    pass
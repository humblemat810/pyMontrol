import logging, pathlib, yaml
logging.basicConfig(filename='./control.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
with open(str(pathlib.Path(r'./control_config.yaml')), 'r') as file:
    control_config = yaml.safe_load(file)
from  all_decorate import for_all_methods, debug_decorater
import connections
import time , threading
@for_all_methods(debug_decorater)
class controller:
    def atomic_auto_assign_new_data(self, new_doc, mongoClient):
        
        pass
    def atomic_assign_data(self, doc, mongoClient):
        # 1. listen to doc event stream, break if removed
        # within the event loop try get free worker
        # if none free worker, listen for free worker add event
        # atomic transaction : 1. remove worker from free, 2. delete doc from in data packet stream, 3, add data packet to free worker
        
        
        
        pass
    
    
    def pop_free_worker(self, mongoClient):
        
        availableWorker = mongoClient['worker']['availableWorker'].find_one_and_delete({})
        
        return(availableWorker)
        pass
    
    
    def routeEventStream(self,fullDocument, mongoClient):
        availableWorker = self.pop_free_worker(mongoClient)
        while availableWorker is None:
            logging_info = 'no free worker found'
            print(logging_info)
            for i in self.available_worker_event_stream:
                if i['operationType'] == 'insert':
                    availableWorker = i['fullDocument']
                    worker_name = availableWorker['_id']
                    logging_info = 'found free worker ' + worker_name
                    print(logging_info)
                    break
        worker_name = availableWorker['_id']
        mongoClient['worker'][worker_name].insert_one(fullDocument)
        logging_info = 'packet id' + str(fullDocument['_id']) + ' assigned to worker ' + str(worker_name)
        logging.info(logging_info )
        print(logging_info)
        
        pass
    
    def __init__(self):
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
            self.eventStream = connections.client['eventTrigger']['data_packet_input'].watch(resume_after = resume_token)
        except:
            self.eventStream = connections.client['eventTrigger']['data_packet_input'].watch()
        self.available_worker_event_stream = connections.client['worker']['availableWorker'].watch()
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
    def process_eventStream(self, i):
        # print(i)
        if 'command' in i:
            if i['command'] == 'break':
                resume_token = i['_id']
                connections.client['eventTrigger']['resume_token'].replace_one({'_id': 'resume_token'}, 
                                                                       {'_id': 'resume_token', 'value': resume_token['_data']},
                                                                       upsert = True
                                                                       )    
                pass
        
        elif i['operationType'] == 'insert':
            self.routeEventStream(i['fullDocument'], connections.client)
        
        resume_token = i['_id']
        connections.client['eventTrigger']['resume_token'].replace_one({'_id': 'resume_token'}, 
                                                                       {'_id': 'resume_token', 'value': resume_token},
                                                                       upsert = True
                                                                       )
        pass
    def manage_new_data_for_execution(self):
        event_cnt = 0
        for i in self.eventStream:
            print('event found', event_cnt)
            event_cnt += 1
            x = threading.Thread(target = self.process_eventStream, args = (i,))
            x.start()
            
if __name__ == '__main__':
    my_controller = controller()
    
    my_controller.manage_new_data_for_execution()
    pass

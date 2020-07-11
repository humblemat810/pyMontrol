import logging, pathlib, yaml
logging.basicConfig(filename='./control.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
with open(str(pathlib.Path(r'./control_config.yaml')), 'r') as file:
    control_config = yaml.safe_load(file)
from  all_decorate import for_all_methods, debug_decorater
import connections
import time 
@for_all_methods(debug_decorater)
class controller:
    def pop_free_worker(self, mongoClient):
        
        availableWorker = mongoClient['worker']['availableWorker'].find_one_and_delete({})['_id']
        return(availableWorker)
        pass
    
    
    def routeEventStream(self,fullDocument, mongoClient):
        worker_id = self.pop_free_worker(mongoClient)
        mongoClient['worker'][worker_id].insert_one(fullDocument)
        logging.info('packet id' + str(fullDocument['_id']) + ' assigned to worker ' + str(worker_id) )
        pass
    
    def __init__(self):
        try:
            resume_token = connections.client['eventTrigger']['resume_token'].find_one()['value']
        
            self.eventStream = connections.client['eventTrigger']['data_packet_input'].watch(resume_after=resume_token)
        except:
            self.eventStream = connections.client['eventTrigger']['data_packet_input'].watch()
        pass
    
    
    
    
    def kill_worker(self,worker_name):
        if not worker_name.startswith('test_worker') and not worker_name.startswith('worker'):
            raise(ValueError('bad collection name : '+ worker_name))
            pass
        if not(worker_name  in  connections.client['worker'].list_collection_names()):
            raise(ValueError('collection name not in worker collections'))
            pass
        import worker_command
        import data_ref as dr
        kill_command =worker_command.worker_command('kill')
        my_data_ref = dr.data_ref(db = 'worker', collection = worker_name)
        my_data_ref.data_insert(data = kill_command, connectionStr = None, mongoClient = connections.client)
        pass
       
    def worker_managerment(self,mongoClient):
        # kill or wake up sleeping worker
        c = mongoClient['worker']['availableWorker'].find()
        for worker in c:
            if int(time.time()) - worker['free-since'] > worker_time_out:
                kill_worker(worker['_id'])
                pass
            pass
        pass    
        
    def manage_new_data_for_execution(self,):
        for i in self.eventStream:
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
                                                                           {'_id': 'resume_token', 'value': resume_token['_data']},
                                                                           upsert = True
                                                                           )
if __name__ == '__main__':
    my_controller = controller()
    my_controller.manage_new_data_for_execution()
    pass

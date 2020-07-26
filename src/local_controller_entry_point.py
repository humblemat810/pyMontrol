def main_control_loop(q):
    from local_data_controller_helper import (spawn_child, process, 
                                              collect_parent_data, store_data_to_map)
    # q.put(data['current_node_name'])
    # process commands in topological order according to the workflow specified in data['dag']
    # data['data'], the data to be processed
    # data['dag']  a dag object consist of workflow to be applied to the data, 
    # and possibly including the settings in the option in that certain blocks
    # 
    while not q.empty():
        current_node_name = q.get()
        data = collect_parent_data(current_node_name)
        
        process(data)
        store_data_to_map(data)
        spawn_child(data,q)
        pass
    pass
def local_control_init(data):
    # to_do: generate packet ID if not exist in data
    from local_data_controller_helper import store_dag
    from copy import deepcopy
    dag = data['dag']
    modified_dag = deepcopy(dag)
    modified_dag.add_node('init0', process_name = 'init')
    modified_dag.add_edge('init0', data['current_node_name'])
    store_dag(modified_dag)
    
    from queue import Queue
    data['passed_local_controller'] = True
    from local_data_controller_helper import (spawn_child, process, 
                                              collect_parent_data, store_data_to_map)
    q = Queue()
    init_data = deepcopy(data)
    init_data['current_node_name'] = 'init0'
    store_data_to_map(init_data)
    init_data['dag'] = modified_dag
    process(init_data)
    init_data['dag'] = dag
    store_data_to_map(init_data)
    init_data['dag'] = modified_dag
    spawn_child(init_data,q)
    init_data['dag'] = dag
    return q
    
    
# sample entry point :
# def process_data(data):
#     # TO_DO: rewrite this class for custom local control
#     from local_data_controller import local_control_init, main_control_loop
#     q = local_control_init(data)
#     main_control_loop(q)
#     pass
# if __name__ == '__main__':
    # process_data()
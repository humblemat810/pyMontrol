# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 00:41:21 2020

@author: pchan
"""

def process_data(data):
    print('received data: '+ str(data))
    # uncommend to use the local control, data required to have special format, see example test_control.py
    '''   
    # uncomment to test local data control
    from data_map_processor import process_dag
    import numpy as np
    my_process_dag = process_dag()
    my_process_dag.add_node('input0', 'init')
    data = {'data' : np.arange(121), 'dag' : my_process_dag, 'current_node_name' : 'input0'}
    '''
    '''
    from local_controller_entry_point import local_control_init, main_control_loop
    queue_of_local_tasks = local_control_init(data)
    main_control_loop(queue_of_local_tasks)
    '''
    pass

# run this script for demo behaviour
if __name__ == '__main__':
    from data_map_processor import process_dag
    import numpy as np
    my_process_dag = process_dag()
    my_process_dag.add_node('input0', 'init')
    data = {'data' : np.arange(121), 'dag' : my_process_dag, 'current_node_name' : 'input0'}
    from local_controller_entry_point import local_control_init, main_control_loop
    queue_of_local_tasks = local_control_init(data)
    main_control_loop(queue_of_local_tasks)
    pass
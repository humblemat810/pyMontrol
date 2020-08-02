# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 00:41:21 2020

@author: pchan
"""

def process_data(data):
    print('received data: '+ str(data))
    # uncommend to use the local control, data required to have special format, see example test_control.py
    # from local_controller_entry_point import local_control_init, main_control_loop
    # queue_of_local_tasks = local_control_init(data)
    # main_control_loop(queue_of_local_tasks)
    pass

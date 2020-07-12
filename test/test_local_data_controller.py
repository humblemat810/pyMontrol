# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 13:58:36 2020

@author: PChan
"""
import sys, os, pathlib
sys.path.append(str(pathlib.Path('../src')))

import local_control_data
local_control_data.map_of_data = {}
import data_map_processor
my_process_dag = data_map_processor. process_dag()
my_process_dag.add_node('to_local_control0', process_name = 'to_local_control')
my_process_dag.add_node('input0', process_name = 'input')
my_process_dag.add_node('input1', process_name = 'input')
my_process_dag.add_node('print_to_console0', process_name = 'print_to_console')
my_process_dag.add_edge('to_local_control0', 'input0')
my_process_dag.add_edge('input0', 'input1')
my_process_dag.add_edge('input1', 'print_to_console0')
from data_map_processor import hiking_data
data = hiking_data(data = [1,2,3,4], 
                   dag = my_process_dag, 
                   current_node_name = 'to_local_control0')
import local_data_controller
local_data_controller.process_to_local_control(data)
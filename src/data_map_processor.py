# -*- coding: utf-8 -*-
"""
Created on Sun Jul 12 08:23:32 2020

@author: pchan
"""





from networkx import DiGraph
class process_dag(DiGraph):
    def check_valid_process_name():
        
        pass
    def __init__(self, *arg, **kwarg):
        super().__init__( *arg, **kwarg)
        pass
    def add_node(self, *arg, **kwarg):
        # if len(arg) > 0:
        #     node_for_adding = arg[0]
        if len(arg) > 1:
            kwarg['process_name'] = arg[1]
        
        current_node_name = kwarg['process_name']
        
        import pathlib, sys, os
        sys.path.append(str(pathlib.Path('../')))
        import local_data_controller
        assert current_node_name in dir(local_data_controller)
        
        super().add_node( *arg[0:1], **kwarg)
        
        pass
    def remove_node(self, *arg, **kwarg):
        
        super().remove_node( *arg, **kwarg)
        pass
    def next_nodes(self):
        
        pass
    
    def count_neighbour(self):
        dict_of_n_neighbour = {}
        for i in self.nodes:
            dict_of_n_neighbour[i] = len(self[i])
        return dict_of_n_neighbour
    
    pass

class hiking_data(dict):
    def __init__(self, data, dag, current_node_name):
        self["data"] = data
        self["dag"] = dag
        self["current_node_name"] = current_node_name
        
        pass
    
    
if __name__ == '__main__':
    my_process_dag = process_dag()
    my_process_dag.add_node('input0', 'input')
    my_process_dag.add_edge('input0', 'input')
    print(my_process_dag.count_neighbour())

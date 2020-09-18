# -*- coding: utf-8 -*-
"""
Created on Sat Aug  1 09:30:58 2020

@author: pchan
"""

class event_stream_processer:
    
    
    def __init__(self):
        self. collection  = None
        self. db = None
        self. changeStream = None
        self. event_use_thread_on = False

        pass
    def listen_change_stream(self):
        
        pass
    def preprocess(self):
        pass
    
    
    def process(self):
        pass
    
    def postprocess(self):
        pass
    
    
    def event_filter(self, event) :
        
        return True
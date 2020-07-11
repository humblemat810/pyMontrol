# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 07:56:38 2020

@author: pchan
"""

def debug_decorater( to_decorate):
    def func_to_decorate(to_decorate = to_decorate):
        try:
            to_decorate()
        except:
            # TO_DO: write your own debug code
            print('error')
        
        pass
    return func_to_decorate

def for_all_methods(decorator):
    def decorate(cls):
        for attr in cls.__dict__: # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate
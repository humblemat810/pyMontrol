# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 07:56:38 2020

@author: pchan
"""

def debug_decorater( to_decorate):
    def func_to_decorate(*arg, **kwarg):
        try:
            return(to_decorate(*arg, **kwarg))
        except:
            # TO_DO: write your own debug code
            print('error')
            import sys
            print("Unexpected error:", sys.exc_info()[0])
            import traceback
            for line in traceback.format_stack():
                print(line.strip())
            print('-------------')
        pass
    return func_to_decorate

def for_all_methods(decorator):
    def decorate(cls):
        for attr in cls.__dict__: # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate
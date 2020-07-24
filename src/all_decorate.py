# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 07:56:38 2020

@author: pchan
"""
import yaml, pathlib
with open(str(pathlib.Path(r'./control_config.yaml')), 'r') as file:
    control_config = yaml.safe_load(file)
strict_mode = False
import functools
import logging
if 'strict_mode' in control_config:
    strict_mode = control_config['strict_mode']
def debug_decorater( to_decorate):
    @functools.wraps(to_decorate)
    def func_to_decorate(*arg, **kwarg):
        try:
            return(to_decorate(*arg, **kwarg))
        except Exception as e:
            
            # TO_DO: write your own debug code
            print('error', e)
            logging.info('error' +  str(e))
            import sys
            print("Unexpected error:", sys.exc_info()[0])
            logging.info("Unexpected error:" +  str(sys.exc_info()[0]))
            import traceback
            for line in traceback.format_stack():
                print(line.strip())
            print('-------------')
            if strict_mode:
                raise
        pass
    return func_to_decorate

def for_all_methods(decorator):
    def decorate(cls):
        for attr in cls.__dict__: # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate
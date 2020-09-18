# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 08:36:51 2020

@author: pchan
"""

class worker_method_unauthorized(Exception):
    
    pass


class duplicate_worker_name_error(ValueError):
    
    pass


class data_ref_not_exist(FileNotFoundError):
    pass
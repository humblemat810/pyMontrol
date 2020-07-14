# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 00:41:21 2020

@author: pchan
"""

def process_data(data):
    # TO_DO: rewrite this class for custom local control
    from local_controller_entry_point import local_control_init, main_control_loop
    q = local_control_init(data)
    main_control_loop(q)
    pass

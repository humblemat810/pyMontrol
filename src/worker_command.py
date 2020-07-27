# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 22:51:47 2020

@author: pchan
"""


class worker_command:
    
    def __init__(self, command = None):
        self.command_json = command
        pass
    report_health_str = 'report_health'
    def worker_health(self, worker_name, sender):
        self.command_json = {self.report_health_str : {'to' : sender }}
        pass
    kill_str = 'kill'
    def kill_worker( self, worker_name):
        self.command_json = {self.kill_str : worker_name}
        pass
    reload_code_str = 'reload_code'
    def reload_code( self, worker_name):
        self.command_json = {self.reload_code_str : worker_name}
        pass
    pass


class command_report_health(worker_command):
    def __init__(self, worker_name,  sender):
        self.worker_health(worker_name, sender)
        pass
    pass
class command_kill_worker(worker_command):
    def __init__(self, worker_name):
        self.kill_worker(worker_name)
        pass
    pass

class reload_code(worker_command):
    def __init__(self, worker_name):
        self.reload_code(worker_name)
    pass

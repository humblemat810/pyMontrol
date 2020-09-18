

# simplest example of showing how to use this framework
from base_process_data import base_process_data
class print_to_console(base_process_data):
    
    
    
    def __init__(self, data_to_print):
        self.data = data_to_print
        self.default_settings = {}
        pass
    def process(self):
        super().process()
        print(self.data)
        pass
    pass


my_print_to_console = print_to_console('this is a data line')
my_print_to_console.run()
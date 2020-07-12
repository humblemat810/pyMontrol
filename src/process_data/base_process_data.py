from abc import ABCMeta, abstractmethod, abstractstaticmethod

class base_process_data(metaclass=ABCMeta):
    @property
    @abstractmethod
    def default_settings(self):
        return self._default_settings
        pass
    @default_settings.setter
    @abstractmethod
    def default_settings(self, settings):
        self._default_settings = settings
        pass
    
    def __init__(self):
        pass
    def run(self):
        self.pre_process()
        self.process()
        self.post_process()
        pass
    def pre_process(self):
        pass
    def post_process(self):
        pass
    @abstractmethod
    def process(self):
        pass
    
    
    pass

class base_process_default_settings(metaclass=ABCMeta):
    pass




class Database_service:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def connect(self):
        pass

    def disconnect(self):
        pass

    def insert(self, data):
        self.__validate_input(data)
        pass

    def __validate_input(self, data):
        pass
    

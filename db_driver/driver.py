from abc import abstractmethod, ABCMeta


class Driver(metaclass=ABCMeta):
    @abstractmethod
    def insert(self, query, data):
        pass

    @abstractmethod
    def update(self, data: dict):
        pass

    @abstractmethod
    def delete(self, data: dict):
        pass

    @abstractmethod
    def query(self, query):
        pass

    @abstractmethod
    def get_result(self, offset=0):
        pass

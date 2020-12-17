from abc import ABC,abstractmethod

class Exchange(ABC):

    @abstractmethod
    def get_tickets():
        pass

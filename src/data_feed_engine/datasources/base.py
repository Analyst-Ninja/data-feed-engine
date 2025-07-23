from abc import ABC, abstractmethod


class BaseDatasource(ABC):
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def connect(self):
        """
        Connect to datasource
        """

    @abstractmethod
    def disconnect(self):
        """
        Disconnect from datasource
        """

    def validate_connection(self):
        """
        Validation the connection to datasource
        """
        if self.connect():
            self.disconnect()
            return True

    @abstractmethod
    def get_data(self):
        """
        Retrieve data
        """

    @abstractmethod
    def put_data(self):
        """
        Store data
        """

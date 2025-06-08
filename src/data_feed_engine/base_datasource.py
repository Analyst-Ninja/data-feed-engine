from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame


class BaseDatasource(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass

    @abstractmethod
    def run(self) -> None:
        df = self.read()
        df_transformed = df.transform()
        self.write(df=df_transformed)

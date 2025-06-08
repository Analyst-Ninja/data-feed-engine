from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


class BaseDatasource(ABC):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass

    def run(self) -> None:
        self.df = self.read()
        self.df_transformed = self.transform(self.df)
        self.write(df=self.df_transformed)

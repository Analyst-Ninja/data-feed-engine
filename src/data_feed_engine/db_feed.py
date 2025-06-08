from data_feed_engine.base_datasource import BaseDatasource
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict


class DBFeed(BaseDatasource):
    def __init__(self, spark: SparkSession, config: Dict):
        super().__init__(spark)
        self.config = config

    def read(self) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.config["url"])
            .option("dbtable", self.config["dbtable"])
            .option("user", self.config["user"])
            .option("password", self.config["password"])
            .option("driver", self.config["driver"])
            .load()
        )

    def transform(self, df: DataFrame) -> DataFrame:
        # No Transformation
        return df

    def write(self, df: DataFrame) -> None:
        df.write.mode("overwrite").parquet(self.config["output_path"])

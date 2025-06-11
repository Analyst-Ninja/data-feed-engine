from data_feed_engine.base_datasource import BaseDatasource
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict
from dotenv import load_dotenv
import os
from utils.data_schema import RedditPosts

load_dotenv(
    "/media/de-ninja/codebase/Projects/14_data-feed-engine/data-feed-engine/.env"
)


class DBFeed(BaseDatasource):
    def __init__(self, spark: SparkSession, read_config: Dict, write_config: Dict):
        super().__init__(spark)

        self.read_config = read_config
        self.read_config.user = os.getenv("MYSQL_USER")
        self.read_config.password = os.getenv("MYSQL_PASSWORD")

        self.write_config = write_config

    def read_jdbc(self) -> DataFrame:
        return (
            self.spark.read.format("jdbc")
            .option("url", self.read_config.url)
            .option("dbtable", self.read_config.dbtable)
            .option("user", self.read_config.user)
            .option("password", self.read_config.password)
            .option("driver", self.read_config.driver)
            .load()
        )

    def transform(self, df: DataFrame) -> DataFrame:
        # Transformation - Rename the id -> post_id
        df = df.withColumnRenamed(RedditPosts.ID, "post_id")
        return df

    def write(self, df: DataFrame) -> None:
        df.write.mode(self.write_config.write_mode).parquet(
            self.write_config.output_path
        )

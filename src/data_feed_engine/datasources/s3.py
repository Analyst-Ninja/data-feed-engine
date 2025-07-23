import json
from typing import Union

import boto3
import pandas as pd
from pyspark.sql import DataFrame

from data_feed_engine.datasources.base import BaseDatasource
from data_feed_engine.factory.registry import register_datasource


@register_datasource("s3")
class S3Datasource(BaseDatasource):
    def __init__(self, config):
        self.config = config
        self.bucket = config["bucket"]
        self.prefix = config["prefix"]

    def connect(self):
        self.s3_client = boto3.client("s3")
        return True

    def disconnect(self):
        self.s3_client = None

    def fetch_with_python(self):
        latest_file = self._get_latest_file()
        file_path = "s3://{0}/{1}".format(self.bucket, latest_file)
        return pd.read_parquet(file_path)

    def fetch_with_spark(self):
        pass

    def get_data(self, run_date: str, incremental: bool = False):
        if self.validate_connection():
            self.connect()
            return self.fetch_with_python()

    def _get_latest_file(self):
        objects = self.s3_client.list_objects_v2(Bucket=self.bucket)["Contents"]
        return sorted(objects, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

    def put_data(
        self, df: Union[pd.DataFrame, DataFrame], run_date: str, execution_id: str
    ):
        if self.validate_connection():
            print("INNNNN")
            output_path = "s3://{0}/{1}/date={2}/execution_id={3}".format(
                self.config["bucket"], self.config["prefix"], run_date, execution_id
            )
            print(output_path)
            if isinstance(df, pd.DataFrame):
                df.to_parquet(output_path)
            elif isinstance(df, DataFrame):
                df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    with open("configs/reddit_posts_config.json", "r") as fh:
        config = json.load(fh)

    input_config = config["input_datasource"]
    output_config = config["output_datasource"]

    obj = S3Datasource(config=output_config)

    data = obj.get_data()

    print(data)

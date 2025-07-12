import json
from data_feed_engine.datasources.base import BaseDatasource
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

class JDBCDatasource(BaseDatasource):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.username = os.getenv(self.config["username"])
        self.password = os.getenv(self.config["password"])
        self.hostname = os.getenv(self.config["host"])
        self.port = os.getenv(self.config["port"])
        self.database = self.config["database"]

    def connect(self):
        if self.config["database_type"] == "mysql":
            conn_string = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
                self.username,
                self.password,
                self.hostname,
                self.port,
                self.database
            )
            self.conn = create_engine(conn_string).connect()
            return True
        else:
            raise Exception("Could Not Connect to DB")

    def disconnect(self):
        self.conn.close()

    def fetch_with_python(self):
        if self.validate_connection():
            self.connect()
            if "sql_file" in self.config.keys():
                with open("src/sql/{}".format(self.config["sql_file"])) as q:
                    query = q.read()
                res = pd.read_sql_query(query, self.conn)
            elif "table_name" in self.config.keys():
                res = pd.read_sql_table(self.config["table_name"], self.conn)
            else:
                raise Exception("Define either query or table to read from")
            self.disconnect()
        return res
    
    def fetch_with_spark(self):
        pass

    def get_data(self):
        if self.config["processing_engine"] == "python":
            return self.fetch_with_python()
        elif self.config["processing_engine"] == "spark":
            return self.fetch_with_spark()
        else:
            raise Exception("`processing_engine` should be either python or spark")

    def put_data(self):
        pass

if __name__ == "__main__":

    with open("data_pipeline_config/reddit_posts_config.json", "r") as fh:
        config = json.load(fh)["input_datasource"]

    obj = JDBCDatasource(config=config)

    data = obj.get_data()

    print(data)


    

    

    




    
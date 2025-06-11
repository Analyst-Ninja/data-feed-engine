from typing import Optional

from pydantic import BaseModel


class SparkReadConfig(BaseModel):
    url: str
    dbtable: str
    driver: str
    user: Optional[str] = ""
    password: Optional[str] = ""


class SparkWriteConfig(BaseModel):
    write_mode: Optional[str] = "overwrite"  # overwrite/ append
    output_path: str


class RedditPostsConfig(BaseModel):
    spark_read_config: SparkReadConfig
    spark_write_config: SparkWriteConfig

from typing import Literal, Optional

from pydantic import BaseModel


class JDBCDatasource(BaseModel):
    user: Optional[str] = ""
    password: Optional[str] = ""


class S3Datasource(BaseModel):
    name: str
    type: Literal["s3"]
    bucket: str
    prefix: str
    format: Literal["csv", "json", "parquet"]


class RedditPostsConfig(BaseModel):
    feed_name: str
    feed_type: Literal["jdbc", "s3", "api"]
    input_datasource: JDBCDatasource
    output_datasource: S3Datasource

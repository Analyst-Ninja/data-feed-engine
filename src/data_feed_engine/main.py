from pyspark.sql import SparkSession
from data_feed_engine.db_feed import DBFeed
import json
from utils.config import RedditPostsConfig

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("DB Feed Engine")
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.4.0")
        .getOrCreate()
    )

    with open(
        "/media/de-ninja/codebase/Projects/14_data-feed-engine/data-feed-engine/data_pipeline_config/reddit_posts_config.json",
        "r",
    ) as file:
        config = json.load(file)

    config = RedditPostsConfig(**config)

    read_config = config.spark_read_config
    write_config = config.spark_write_config

    feed = DBFeed(spark=spark, read_config=read_config, write_config=write_config)
    feed.run()

    spark.stop()

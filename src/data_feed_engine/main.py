from pyspark.sql import SparkSession
from data_feed_engine.db_feed import DBFeed

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("DB Feed Engine")
        .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.4.0")
        .getOrCreate()
    )

    config = {
        "url": "jdbc:mysql://localhost:3306/reddit_db",
        "user": "root",
        "password": "1234",
        "dbtable": "r_posts",
        "driver": "com.mysql.cj.jdbc.Driver",  # MySQL driver
        "output_path": "data/output/r_posts",
    }

    feed = DBFeed(spark=spark, config=config)
    feed.run()

    spark.stop()

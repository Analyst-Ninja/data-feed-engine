import json
import logging

from data_feed_engine.datasources import jdbc, s3
from data_feed_engine.factory import factory
from data_feed_engine.feeds import reddit_posts

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def run_feed_from_config(
    config_path: str, run_date: str, full_load: bool = False
) -> None:
    """Load config and runs a feed"""
    logger = logging.getLogger("Runner")
    logger.info(f"Loading configuration from {config_path}")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)

    except Exception as e:
        logger.error(f"Error: {e}")

    logger.info(f"Creating feed of type: {config.get('feed_type')}")
    feed = factory.create_feed(config)

    logger.info(f"Running feed: {config.get('feed_name')} for date {run_date}")
    feed.run(run_date=run_date, full_load=full_load)

    logger.info("\n----Execution Complete----\n")

from data_feed_engine.feeds.base import BaseFeed
from data_feed_engine.factory.registry import register_feed


@register_feed("jdbc")
class RedditPostsFeed(BaseFeed):
    def process(self, data, run_date):
        return data

    def perform_data_quality_checks(self, input_data, output_data):
        return None

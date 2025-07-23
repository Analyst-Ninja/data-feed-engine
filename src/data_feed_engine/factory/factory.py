import logging
from typing import TYPE_CHECKING, Dict

from .registry import DATASOURCE_REGISTRY, FEED_REGISTRY

logging.basicConfig(level=logging.INFO)

if not TYPE_CHECKING:
    from data_feed_engine.datasources.base import BaseDatasource
    from data_feed_engine.feeds.base import BaseFeed


def create_datasource(config: Dict) -> BaseDatasource:
    """Factory method to create datasource instance"""
    ds_type = config.get("type")
    if not ds_type:
        raise ValueError("Datasource config must include a `type`.")

    if ds_type not in DATASOURCE_REGISTRY:
        raise ValueError(
            f"Unsupported datasource type: {ds_type}. Registered types: {list(DATASOURCE_REGISTRY.keys())}"
        )

    ds_class = DATASOURCE_REGISTRY[ds_type]
    return ds_class(config)


def create_feed(config: Dict) -> BaseFeed:
    """Factory method to create feed instance"""
    feed_type = config.get("feed_type")
    if not feed_type:
        raise ValueError("Feed configuration must include a `feed_type`.")

    if feed_type not in FEED_REGISTRY:
        raise ValueError(
            f"Unsupported feed type: {feed_type}. Registered types: {list(FEED_REGISTRY.keys())}"
        )

    feed_class = FEED_REGISTRY[feed_type]
    return feed_class(config)

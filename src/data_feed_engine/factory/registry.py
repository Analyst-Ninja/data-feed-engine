from typing import Dict, Type, Callable

from data_feed_engine.feeds.base import BaseFeed
from data_feed_engine.datasources.base import BaseDatasource

DATASOURCE_REGISTRY: Dict[str, Type[BaseDatasource]] = {}
FEED_REGISTRY: Dict[str, Type[BaseFeed]] = {}


def register_datasource(name: str) -> Callable:
    """A decorator that register a datasource"""

    def decorator(cls: Type[BaseDatasource]) -> Callable:
        DATASOURCE_REGISTRY[name] = cls
        return cls

    return decorator


def register_feed(name: str) -> Callable:
    """A decorator that register a feed"""

    def decorator(cls: Type[BaseFeed]) -> Callable:
        FEED_REGISTRY[name] = cls
        return cls

    return decorator

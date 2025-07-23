from dataclasses import dataclass


@dataclass
class RedditPosts:
    ID: str = "id"
    SUB_REDDIT: str = "sub_reddit"
    POST_TYPE: str = "post_type"
    TITLE: str = "title"
    AUTHOR: str = "author"
    URL: str = "url"
    SCORE: str = "score"
    NUM_COMMENTS: str = "num_comments"
    UPVOTE_RATIO: str = "upvote_ratio"
    OVER_18: str = "over_18"
    EDITED: str = "edited"
    CREATED_AT: str = "created_at"
    FETCHED_AT: str = "fetched_at"
    ETL_INSERT_DATE: str = "etl_insert_date"

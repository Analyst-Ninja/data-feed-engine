SELECT sub_reddit, COUNT(*) AS post_count FROM r_posts
GROUP BY sub_reddit;
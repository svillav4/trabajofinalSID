WITH hashtag_data AS (
    SELECT 
        platform,
        best_hashtag_rank,
        COUNT(post_id) AS posts_with_top_hashtag
    FROM features_social_posts
    WHERE best_hashtag_rank IS NOT NULL
    GROUP BY platform, best_hashtag_rank
)
SELECT 
    platform,
    MIN(best_hashtag_rank)      AS mejor_ranking_hashtag,
    COUNT(*)                    AS frecuencia,
    AVG(posts_with_top_hashtag) AS promedio_posts
FROM hashtag_data
GROUP BY platform
ORDER BY mejor_ranking_hashtag ASC
LIMIT 20;

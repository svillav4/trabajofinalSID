SELECT 
    platform,
    COUNT(post_id)                          AS total_posts,
    ROUND(AVG(engagement_rate) * 100, 2)    AS avg_engagement_pct,
    ROUND(AVG(virality_score), 4)           AS avg_virality,
    SUM(CASE WHEN is_trending THEN 1 ELSE 0 END) AS trending_posts,
    ROUND(AVG(toxicity_score), 4)           AS avg_toxicity
FROM features_social_posts
GROUP BY platform
ORDER BY avg_engagement_pct DESC;

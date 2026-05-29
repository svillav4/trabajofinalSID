SELECT
    event_date,
    platform,
    COUNT(post_id)                       AS posts_del_dia,
    ROUND(AVG(engagement_rate) * 100, 2) AS engagement_promedio,
    SUM(CASE WHEN is_trending THEN 1 ELSE 0 END) AS posts_trending,
    ROUND(AVG(toxicity_score), 4)        AS toxicidad_promedio
FROM features_social_posts
GROUP BY event_date, platform
ORDER BY event_date ASC, platform;

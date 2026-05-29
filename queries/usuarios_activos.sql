SELECT
    user_id,
    platform,
    total_posts_by_user,
    active_days,
    ROUND(posting_frequency, 2)             AS posts_por_dia,
    ROUND(avg_engagement_per_user * 100, 2) AS engagement_promedio_pct,
    detected_language
FROM features_social_posts
GROUP BY 
    user_id, platform, total_posts_by_user,
    active_days, posting_frequency,
    avg_engagement_per_user, detected_language
ORDER BY total_posts_by_user DESC
LIMIT 20;

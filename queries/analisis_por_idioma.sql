SELECT
    detected_language,
    COUNT(post_id)                            AS total_posts,
    ROUND(AVG(engagement_rate) * 100, 2)      AS avg_engagement_pct,
    ROUND(AVG(toxicity_score), 4)             AS avg_toxicity,
    SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) AS posts_toxicos
FROM features_social_posts
GROUP BY detected_language
ORDER BY total_posts DESC;

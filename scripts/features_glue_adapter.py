import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, IntegerType, BooleanType
from better_profanity import profanity

# Inicialización
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_path',
    'output_path'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Leer desde curated
df = spark.read.option("recursiveFileLookup", "true").parquet(args['input_path'])

# 2. Engagement rate y métricas base
# (estas columnas las necesita el score de virality de tu compañero también)
df = df.withColumn(
    "engagement_rate",
    F.when(
        F.col("followers") > 0,
        (F.col("likes") + F.col("comments") + F.col("saves")) / F.col("followers")
    ).otherwise(0.0).cast(FloatType())
).withColumn(
    "virality_score",
    F.when(
        F.col("impressions") > 0,
        F.col("reach") / F.col("impressions")
    ).otherwise(0.0).cast(FloatType())
).withColumn(
    "is_trending",
    (F.col("engagement_rate") > 0.05).cast(BooleanType())
).withColumn(
    "hour_of_day",
    F.hour(F.col("event_timestamp")).cast(IntegerType())
)

# 3. Conteo de hashtags por post
# hashtags es un array en los datos originales
df = df.withColumn(
    "hashtag_count",
    F.when(
        F.col("hashtags").isNotNull(),
        F.size(F.col("hashtags"))
    ).otherwise(0).cast(IntegerType())
)

# 4. Frecuencia de publicación por usuario
# Calcula cuántos posts tiene cada usuario en el dataset
posts_per_user = df.groupBy("user_id").agg(
    F.count("post_id").alias("total_posts_by_user"),
    F.countDistinct("event_date").alias("active_days"),
    F.avg("engagement_rate").alias("avg_engagement_per_user")
)

df = df.join(posts_per_user, on="user_id", how="left")

# Frecuencia promedio: posts por día activo
df = df.withColumn(
    "posting_frequency",
    F.when(
        F.col("active_days") > 0,
        F.col("total_posts_by_user") / F.col("active_days")
    ).otherwise(0.0).cast(FloatType())
)

# 5. Ranking de hashtags por red social
# Explota el array de hashtags para contar frecuencia por plataforma
hashtag_df = df.select(
    "platform",
    F.explode_outer(F.col("hashtags")).alias("hashtag")
).filter(F.col("hashtag").isNotNull())

hashtag_ranking = hashtag_df.groupBy("platform", "hashtag").agg(
    F.count("*").alias("hashtag_frequency")
).withColumn(
    "hashtag_rank",
    F.rank().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("platform")
        .orderBy(F.desc("hashtag_frequency"))
    )
)

# Une el ranking al dataframe principal
# Cada post recibe el rank promedio de sus hashtags como señal
df_exploded = df.select(
    "post_id", "platform",
    F.explode_outer(F.col("hashtags")).alias("hashtag")
)

df_with_ranks = df_exploded.join(
    hashtag_ranking, on=["platform", "hashtag"], how="left"
)

avg_hashtag_rank = df_with_ranks.groupBy("post_id").agg(
    F.avg("hashtag_rank").alias("avg_hashtag_rank"),
    F.min("hashtag_rank").alias("best_hashtag_rank")
)

df = df.join(avg_hashtag_rank, on="post_id", how="left")

# 6. Score de toxicidad
profanity.load_censor_words()

def toxicity_score(text):
    """
    Retorna un score entre 0.0 y 1.0.
    0.0 = sin contenido tóxico detectado
    1.0 = muy tóxico
    """
    try:
        if not text or len(text.strip()) == 0:
            return 0.0
        # better_profanity detecta si hay palabras ofensivas
        has_profanity = profanity.contains_profanity(text)
        if not has_profanity:
            return 0.0
        # Estimación proporcional: ratio palabras ofensivas / total palabras
        words = text.split()
        if len(words) == 0:
            return 0.0
        censored = profanity.censor(text)
        asterisk_words = [w for w in censored.split() if '*' in w]
        return round(len(asterisk_words) / len(words), 4)
    except Exception:
        return 0.0

toxicity_udf = F.udf(toxicity_score, FloatType())

df = df.withColumn(
    "toxicity_score",
    toxicity_udf(F.col("content_clean"))
).withColumn(
    "is_toxic",
    (F.col("toxicity_score") > 0.1).cast(BooleanType())
)

# 7. Seleccionar columnas finales del schema acordado
df_features = df.select(
    "post_id",
    "platform",
    "detected_language",
    "event_date",
    "hour_of_day",
    "user_id",
    "engagement_rate",
    "virality_score",
    "is_trending",
    "hashtag_count",
    "avg_hashtag_rank",
    "best_hashtag_rank",
    "total_posts_by_user",
    "active_days",
    "posting_frequency",
    "avg_engagement_per_user",
    "toxicity_score",
    "is_toxic",
    "ingestion_timestamp"
)

# 8. Escribir en features
df_features.write \
    .mode("overwrite") \
    .partitionBy("event_date", "platform") \
    .parquet(args['output_path'])

job.commit()
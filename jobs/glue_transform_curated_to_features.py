import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, FloatType, IntegerType, BooleanType
from better_profanity import profanity

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_path',
    'output_path',
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Lectura
df = (
    spark.read
    .option("mergeSchema", "true")
    .option("basePath", args['input_path'])
    .parquet(args['input_path'] + "event_date=*/platform=*/*.parquet")
)

if "platform" not in df.columns:
    df = df.withColumn(
        "platform",
        F.regexp_extract(F.input_file_name(), r"platform=([^/]+)", 1)
    )

if "event_date" not in df.columns:
    df = df.withColumn(
        "event_date",
        F.coalesce(
            F.to_date(F.regexp_extract(F.input_file_name(), r"event_date=([^/]+)", 1)),
            F.to_date(F.col("event_timestamp"))
        )
    )

print(f"[INFO] Columnas disponibles: {df.columns}")
print(f"[INFO] Registros leídos: {df.count()}")

# Engagement rate

total_interacciones = (
    F.coalesce(F.col("likes"),    F.lit(0)) +
    F.coalesce(F.col("comments"), F.lit(0)) +
    F.coalesce(F.col("saves"),    F.lit(0))
)

df = df.withColumn(
    "engagement_rate",
    F.when(
        F.col("followers").isNotNull() & (F.col("followers") > 0),
        (total_interacciones / F.col("followers")).cast(DoubleType())
    ).when(
        F.col("platform").isin("x", "twitter") &
        F.col("impressions").isNotNull() & (F.col("impressions") > 0),
        (total_interacciones / F.col("impressions")).cast(DoubleType())
    ).when(
        (F.col("platform") == "reddit") &
        ((F.coalesce(F.col("likes"),    F.lit(0)) +
          F.coalesce(F.col("comments"), F.lit(0))) > 0),
        (
            F.coalesce(F.col("comments"), F.lit(0)) /
            (F.coalesce(F.col("likes"),    F.lit(0)) +
             F.coalesce(F.col("comments"), F.lit(0)))
        ).cast(DoubleType())
    ).otherwise(F.lit(None).cast(DoubleType()))
)

# Virality score

df = df.withColumn(
    "virality_score",
    F.when(
        F.col("platform").isin("instagram", "x", "twitter") &
        F.col("impressions").isNotNull() & (F.col("impressions") > 0) &
        F.col("reach").isNotNull(),
        (F.col("reach") / F.col("impressions")).cast(DoubleType())
    ).when(
        (F.col("platform") == "reddit") &
        F.col("likes").isNotNull() & (F.col("likes") > 0) &
        F.col("comments").isNotNull(),
        (F.col("comments") / F.col("likes")).cast(DoubleType())
    ).otherwise(F.lit(None).cast(DoubleType()))
)

# is_trending

df = df.withColumn(
    "umbral",
    F.when(F.col("platform") == "Instagram",          F.lit(0.06))
     .when(F.col("platform").isin("X"),    F.lit(0.01))
     .when(F.col("platform") == "Reddit",             F.lit(0.30))
     .otherwise(F.lit(0.06))
)

df = df.withColumn(
    "is_trending",
    F.when(
        F.col("engagement_rate").isNotNull(),
        F.col("engagement_rate") > F.col("umbral")
    ).otherwise(F.lit(None).cast(BooleanType()))
)

df = df.withColumn("hour_of_day", F.hour(F.col("event_timestamp")))

# Conteo de hashtags por post
df = df.withColumn(
    "hashtag_count",
    F.when(
        F.col("hashtags").isNotNull(),
        F.size(F.col("hashtags"))
    ).otherwise(F.lit(0)).cast(IntegerType())
)

# Ranking de hashtags por plataforma
hashtag_df = df.select(
    "platform",
    F.explode_outer(F.col("hashtags")).alias("hashtag")
).filter(F.col("hashtag").isNotNull())

hashtag_ranking = hashtag_df.groupBy("platform", "hashtag").agg(
    F.count("*").alias("hashtag_frequency")
).withColumn(
    "hashtag_rank",
    F.rank().over(
        Window.partitionBy("platform").orderBy(F.desc("hashtag_frequency"))
    )
)

df_exploded = df.select(
    "post_id", "platform",
    F.explode_outer(F.col("hashtags")).alias("hashtag")
)

avg_hashtag_rank = (
    df_exploded
    .join(hashtag_ranking, on=["platform", "hashtag"], how="left")
    .groupBy("post_id")
    .agg(
        F.avg("hashtag_rank").alias("avg_hashtag_rank"),
        F.min("hashtag_rank").alias("best_hashtag_rank"),
    )
)

df = df.join(avg_hashtag_rank, on="post_id", how="left")

# Frecuencia de publicación por usuario

posts_per_user = df.groupBy("user_id").agg(
    F.count("post_id").alias("total_posts_by_user"),
    F.countDistinct("event_date").alias("active_days"),
    F.avg("engagement_rate").alias("avg_engagement_per_user"),
)

df = df.join(posts_per_user, on="user_id", how="left")

df = df.withColumn(
    "posting_frequency",
    F.when(
        F.col("active_days") > 0,
        (F.col("total_posts_by_user") / F.col("active_days")).cast(FloatType())
    ).otherwise(F.lit(0.0).cast(FloatType()))
)

# Score de toxicidad (better_profanity)

profanity.load_censor_words()

def get_toxicity_score(text: str) -> float:
    try:
        if not text or len(text.strip()) == 0:
            return 0.0
        if not profanity.contains_profanity(text):
            return 0.0
        words = text.split()
        if len(words) == 0:
            return 0.0
        censored = profanity.censor(text)
        asterisk_words = [w for w in censored.split() if '*' in w]
        return round(len(asterisk_words) / len(words), 4)
    except Exception:
        return 0.0

toxicity_udf = F.udf(get_toxicity_score, FloatType())

df = df.withColumn(
    "toxicity_score",
    toxicity_udf(F.col("content"))
).withColumn(
    "is_toxic",
    (F.col("toxicity_score") > 0.1).cast(BooleanType())
)

# Selección final
df_output = df.select(
    F.col("post_id"),
    F.col("platform"),
    F.col("detected_language"),
    F.col("event_date"),
    F.col("hour_of_day").cast(IntegerType()),
    F.col("user_id"),
    F.round(F.col("engagement_rate"),         6).alias("engagement_rate"),
    F.round(F.col("virality_score"),          6).alias("virality_score"),
    F.col("is_trending"),
    F.col("hashtag_count"),
    F.round(F.col("avg_hashtag_rank"),        2).alias("avg_hashtag_rank"),
    F.col("best_hashtag_rank").cast(IntegerType()),
    F.col("total_posts_by_user").cast(IntegerType()),
    F.col("active_days").cast(IntegerType()),
    F.round(F.col("posting_frequency"),       4).alias("posting_frequency"),
    F.round(F.col("avg_engagement_per_user"), 6).alias("avg_engagement_per_user"),
    F.round(F.col("toxicity_score"),          4).alias("toxicity_score"),
    F.col("is_toxic"),
    F.col("ingestion_timestamp"),
)

print(f"[INFO] Registros con features calculadas: {df_output.count()}")

# Escritura particionada
df_output.write \
    .mode("overwrite") \
    .partitionBy("event_date", "platform") \
    .parquet(args['output_path'])

print(f"[INFO] Features escritas en: {args['output_path']}")

job.commit()
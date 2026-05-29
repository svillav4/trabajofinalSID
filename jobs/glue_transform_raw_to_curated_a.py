import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, LongType

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

_EMOJI_RE = re.compile(
    "["
    "\U00010000-\U0010FFFF"
    "\U0001F300-\U0001FAFF"
    "\U0001F000-\U0001F02F"
    "\u2600-\u26FF"
    "\u2700-\u27BF"
    "\uFE00-\uFE0F"
    "\U0001F1E0-\U0001F1FF"
    "]+",
    flags=re.UNICODE,
)
_SPECIAL_RE  = re.compile(r"[^\w\s.,;:\-'\"?!#@áéíóúüñÁÉÍÓÚÜÑ]", re.UNICODE)
_MULTI_SPACE = re.compile(r"\s{2,}")
_HASHTAG_RE  = re.compile(r"#(\w+)", re.UNICODE)
_MENTION_RE  = re.compile(r"@(\w+)", re.UNICODE)
_PUNCT_RE    = re.compile(r"[!?¡¿.]{2,}")


def _limpiar_texto(texto):
    if not isinstance(texto, str):
        return None
    t = texto.strip()
    t = _EMOJI_RE.sub("", t)
    t = _SPECIAL_RE.sub(" ", t)
    t = _PUNCT_RE.sub(lambda m: m.group()[0], t)
    t = _MULTI_SPACE.sub(" ", t).strip()
    return t if t else None


def _extraer_hashtags(texto):
    if not isinstance(texto, str):
        return []
    return list({h.lower() for h in _HASHTAG_RE.findall(texto)})


def _extraer_menciones(texto):
    if not isinstance(texto, str):
        return []
    return list({m.lower() for m in _MENTION_RE.findall(texto)})


udf_limpiar   = F.udf(_limpiar_texto, StringType())
udf_hashtags  = F.udf(_extraer_hashtags, ArrayType(StringType()))
udf_menciones = F.udf(_extraer_menciones, ArrayType(StringType()))

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("multiLine", "true")
    .json(args['input_path'])
)

print(f"[INFO] Registros leídos desde raw: {df.count()}")

df = df.withColumn(
    "content_raw",
    F.coalesce(
        F.col("caption"),
        F.col("texto"),
        F.concat_ws(
            " ",
            F.coalesce(F.col("titulo"), F.lit("")),
            F.coalesce(F.col("body"),   F.lit("")),
        ),
    )
)

df = df.withColumn("content_clean", udf_limpiar(F.col("content_raw")))

df = df.withColumn(
    "user_clean",
    F.lower(
        F.trim(
            F.regexp_replace(
                F.coalesce(F.col("usuario"), F.lit("")),
                r"^(u/|@)", ""
            )
        )
    )
)

if "hashtags" in df.columns:
    df = df.withColumn(
        "hashtags_clean",
        F.when(
            F.col("hashtags").isNotNull(),
            F.transform(F.col("hashtags"), lambda h: F.lower(F.trim(h)))
        ).otherwise(udf_hashtags(F.col("content_raw")))
    )
else:
    df = df.withColumn("hashtags_clean", udf_hashtags(F.col("content_raw")))

if "menciones" in df.columns:
    df = df.withColumn(
        "mentions_clean",
        F.when(
            F.col("menciones").isNotNull(),
            F.transform(F.col("menciones"), lambda m: F.lower(F.trim(m)))
        ).otherwise(udf_menciones(F.col("content_raw")))
    )
else:
    df = df.withColumn("mentions_clean", udf_menciones(F.col("content_raw")))

df = df.withColumn("hashtag_count", F.size(F.col("hashtags_clean")))
df = df.withColumn("mention_count", F.size(F.col("mentions_clean")))
df = df.withColumn("text_length",   F.length(F.col("content_clean")))

NUM_COLS = {
    "likes"          : "likes",
    "comentarios"    : "comments",
    "seguidores"     : "followers",
    "impresiones"    : "impressions",
    "alcance"        : "reach",
    "guardados"      : "saves",
    "num_comentarios": "comments",
    "upvotes"        : "likes",
}

processed_targets = set()
for src, dst in NUM_COLS.items():
    if src not in df.columns or dst in processed_targets:
        continue
    processed_targets.add(dst)
    df = df.withColumn(
        dst,
        F.when(
            F.col(src).cast(LongType()) >= 0,
            F.col(src).cast(LongType())
        ).otherwise(F.lit(None).cast(LongType()))
    )

for col in ("likes", "comments", "followers", "impressions", "reach", "saves"):
    if col not in df.columns:
        df = df.withColumn(col, F.lit(None).cast(LongType()))

if "timestamp" in df.columns:
    df = df.withColumn(
        "timestamp_parsed",
        F.coalesce(
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col("timestamp")),
        )
    )
else:
    df = df.withColumn("timestamp_parsed", F.lit(None).cast("timestamp"))

platform_col = "red_social" if "red_social" in df.columns else "platform"
df = df.withColumn("platform", F.lower(F.trim(F.col(platform_col))))

id_col = "id" if "id" in df.columns else "post_id"

df_output = df.select(
    F.col(id_col).alias("post_id"),
    F.col("platform"),
    F.col("user_clean").alias("user_id"),
    (F.col("nombre_completo") if "nombre_completo" in df.columns else F.lit(None).cast(StringType())).alias("display_name"),
    F.col("content_raw"),
    F.col("content_clean"),
    F.col("text_length"),
    F.col("likes"),
    F.col("comments"),
    F.col("followers"),
    F.col("impressions"),
    F.col("reach"),
    F.col("saves"),
    F.col("hashtags_clean").alias("hashtags"),
    F.col("mentions_clean").alias("mentions"),
    F.col("hashtag_count"),
    F.col("mention_count"),
    F.col("timestamp_parsed").alias("timestamp"),
    *[
        F.col(c).alias(c)
        for c in ("is_verified", "is_sponsored", "cuenta_verificada",
                  "es_patrocinado", "tipo_contenido", "ubicacion",
                  "filtro_usado")
        if c in df.columns
    ],
)

n_antes = df_output.count()

df_output = df_output.filter(
    F.col("post_id").isNotNull() &
    F.col("content_clean").isNotNull() &
    (F.col("text_length") > 2)
)

n_despues = df_output.count()
print(f"[INFO] Registros antes del filtro : {n_antes}")
print(f"[INFO] Registros después del filtro: {n_despues}")
print(f"[INFO] Descartados                : {n_antes - n_despues}")

(
    df_output
    .write
    .mode("overwrite")
    .partitionBy("platform")
    .parquet(args['output_path'])
)

print(f"[INFO] Datos limpios escritos en: {args['output_path']}")

job.commit()
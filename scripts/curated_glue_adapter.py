import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from langdetect import detect, LangDetectException

# Argumentos de entrada
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

# Leer datos desde S3
df = spark.read.json(args['input_path'])

# Procesar fechas
df = df.withColumn(
    "event_timestamp",
    F.to_timestamp(F.col("timestamp"))
).withColumn(
    "event_timestamp",
    F.to_utc_timestamp(F.col("event_timestamp"), "UTC")
).withColumn(
    "event_date",
    F.to_date(F.col("event_timestamp"))
).withColumn(
    "ingestion_timestamp",
    F.current_timestamp()
).drop("timestamp")


# Detecta idioma del contenido
def detect_language(text):
    try:
        if text and len(text.strip()) > 10:
            return detect(text)
    except LangDetectException:
        pass
    return "unknown"


detect_language_udf = F.udf(detect_language, StringType())

df = df.withColumn(
    "detected_language",
    detect_language_udf(F.col("content"))
)

# Limpieza básica
df = df.withColumn(
    "content_clean",
    F.trim(F.regexp_replace(F.col("content"), r'\s+', ' '))
).withColumn(
    "platform",
    F.lower(F.trim(F.col("platform")))
).dropDuplicates(["post_id"])

# Quitar registros incompletos
df = df.filter(
    F.col("post_id").isNotNull() &
    F.col("content").isNotNull() &
    F.col("event_timestamp").isNotNull()
)

# Guardar en formato parquet particionado
df.write \
    .mode("overwrite") \
    .partitionBy("event_date", "platform") \
    .parquet(args['output_path'])

job.commit()
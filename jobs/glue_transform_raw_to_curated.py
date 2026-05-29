import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from langdetect import detect, LangDetectException

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("mergeSchema", "true")
    .parquet(args['input_path'])
)

if "platform" not in df.columns:
    df = df.withColumn(
        "platform",
        F.regexp_extract(F.input_file_name(), r"platform=([^/]+)", 1)
    )

df = df.withColumnRenamed("content_clean", "content")

df = df.withColumn(
    "event_timestamp",
    F.to_utc_timestamp(F.col("timestamp"), "UTC")
).withColumn(
    "event_date",
    F.to_date(F.col("event_timestamp"))
).withColumn(
    "ingestion_timestamp",
    F.current_timestamp()
).drop("timestamp")

def detect_language(text):
    try:
        if text and len(text.strip()) > 10:
            return detect(text)
    except LangDetectException:
        pass
    return "unknown"

detect_language_udf = F.udf(detect_language, StringType())
df = df.withColumn("detected_language", detect_language_udf(F.col("content")))

df = df.filter(
    F.col("post_id").isNotNull() &
    F.col("content").isNotNull() &
    F.col("event_timestamp").isNotNull()
).dropDuplicates(["post_id"])

df.write \
    .mode("overwrite") \
    .partitionBy("event_date", "platform") \
    .parquet(args['output_path'])

job.commit()
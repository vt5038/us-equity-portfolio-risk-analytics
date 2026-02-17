import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE_PATH", "SILVER_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bronze_path = args["BRONZE_PATH"].rstrip("/")
silver_path = args["SILVER_PATH"].rstrip("/")

# Idempotent partition overwrites
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Read Bronze JSON (partitioned folders trade_date=YYYY-MM-DD)
raw = spark.read.json(f"{bronze_path}/trade_date=*")

# Standardize schema (Silver)
df = raw.select(
    F.col("ticker").cast("string").alias("ticker"),
    F.to_date(F.col("date")).alias("trade_date"),
    F.col("open").cast("double").alias("open"),
    F.col("high").cast("double").alias("high"),
    F.col("low").cast("double").alias("low"),
    F.col("close").cast("double").alias("close"),
    F.col("adjClose").cast("double").alias("adj_close"),
    F.col("volume").cast("long").alias("volume"),
    F.col("adjVolume").cast("long").alias("adj_volume"),
)

# Basic validation (SSOT v1): nulls/types/duplicates; invalid excluded
valid = (
    df.filter(F.col("ticker").isNotNull())
      .filter(F.col("trade_date").isNotNull())
      .filter(F.col("adj_close").isNotNull())
)

# Deduplicate on (ticker, trade_date) deterministically
w = Window.partitionBy("ticker", "trade_date").orderBy(F.col("adj_close").desc_nulls_last())
deduped = (
    valid.withColumn("rn", F.row_number().over(w))
         .filter(F.col("rn") == 1)
         .drop("rn")
)

# Metadata columns (SSOT allows execution timestamps as metadata)
out = (
    deduped.withColumn("ingested_at_utc", F.current_timestamp())
           .withColumn("source_system", F.lit("tiingo"))
)

# Write Silver Parquet partitioned by trade_date
(
    out.repartition("trade_date")
       .write.mode("overwrite")
       .partitionBy("trade_date")
       .parquet(silver_path)
)

job.commit()
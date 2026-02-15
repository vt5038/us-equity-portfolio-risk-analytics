import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

S3_BUCKET = "us-equity-portfolio-data-jayanth"

BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/portfolio/"
SILVER_HOLDINGS_PATH = f"s3://{S3_BUCKET}/silver/portfolio_holdings/"
SILVER_DIM_PORTFOLIO_PATH = f"s3://{S3_BUCKET}/silver/dim_portfolio/"

def main():
    spark = SparkSession.builder.appName("bronze_to_silver_portfolio_holdings").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    ingested_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    # Read CSV (Spark automatically reads partition column from path)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(BRONZE_PATH)
    )

    # Standardization
    df_clean = (
        df
        .withColumn("portfolio_name", F.trim(F.col("portfolio_name")))
        .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn("as_of_date", F.to_date("as_of_date"))
        .withColumn("ingested_at", F.to_timestamp(F.lit(ingested_at)))
        .withColumn("source", F.lit("portfolio_csv"))
    )

    # Basic data validation
    df_valid = df_clean.where(
        F.col("portfolio_name").isNotNull() &
        F.col("as_of_date").isNotNull() &
        F.col("ticker").isNotNull() &
        F.col("quantity").isNotNull() &
        (F.col("quantity") > 0)
    )

    # Generate surrogate portfolio_id (deterministic)
    window = Window.orderBy(F.col("portfolio_name").asc())

    dim_portfolio = (
        df_valid
        .select("portfolio_name")
        .distinct()
        .withColumn("portfolio_id", F.dense_rank().over(window).cast("int"))
        .select("portfolio_id", "portfolio_name")
    )

    holdings = (
        df_valid
        .join(dim_portfolio, on="portfolio_name", how="inner")
        .select(
            "portfolio_id",
            "portfolio_name",
            "as_of_date",
            "ticker",
            "quantity",
            "source",
            "ingested_at"
        )
        .dropDuplicates(["portfolio_id", "as_of_date", "ticker"])
    )

    # Write dimension (overwrite full table)
    dim_portfolio.write.mode("overwrite").parquet(SILVER_DIM_PORTFOLIO_PATH)

    # Partition holdings by as_of_date
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    holdings.write \
        .mode("overwrite") \
        .partitionBy("as_of_date") \
        .parquet(SILVER_HOLDINGS_PATH)

    print("Silver layer written successfully.")

if __name__ == "__main__":
    main()
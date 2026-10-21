import os
import sys
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, timestamp_seconds
from pyspark.sql.types import LongType, StringType, StructField, StructType


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
S3_BUCKET = os.getenv("PAGEVIEWS_BUCKET", "pageviews")
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "demo")
ICEBERG_TABLE = f"{ICEBERG_CATALOG}.bronze.pageviews"

PAGEVIEW_SCHEMA = StructType(
    [
        StructField("user_id", LongType(), True),
        StructField("url", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("received_at", LongType(), True),
    ]
)


def main():
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("seaweedfs-to-iceberg-loader")
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
            .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint", S3_ENDPOINT)
            .config(
                f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access",
                "true",
            )
            .config(
                f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
                "s3://warehouse/",
            )
            .getOrCreate()
        )

        source = f"s3a://{S3_BUCKET}/"
        print(f"Reading pageview JSON events from {source}")

        pageviews_df = (
            spark.read.schema(PAGEVIEW_SCHEMA).json(source)
            .select(
                col("user_id").cast("long"),
                col("url").cast("string"),
                col("channel").cast("string"),
                timestamp_seconds(col("received_at")).alias("received_at"),
            )
        )

        row_count = pageviews_df.count()
        if row_count == 0:
            raise RuntimeError(f"No pageview events were found in {source}")

        print(f"Writing {row_count} pageview events to {ICEBERG_TABLE}")
        pageviews_df.write.format("iceberg").mode("overwrite").save(ICEBERG_TABLE)
        print(f"Loaded {row_count} pageview events into {ICEBERG_TABLE}")
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        if spark is not None:
            spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())

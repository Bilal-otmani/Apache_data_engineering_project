import json
import logging
import math
import os
import random
import time
import uuid
from urllib.parse import urlsplit

import barnum
import boto3
import psycopg2
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionClosedError, EndpointConnectionError


users_seed_count = 10_000
item_seed_count = 1_000

item_inventory_min = 1_000
item_inventory_max = 5_000
item_price_min = 5
item_price_max = 500

purchase_gen_count = 100
purchase_gen_every_ms = 100
pageview_multiplier = 75

channels = ["organic search", "paid search", "referral", "social", "display"]
categories = ["widgets", "gadgets", "doodads", "clearance"]

postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_PORT", "5432")
postgres_user = os.environ["POSTGRES_USER"]
postgres_pass = os.environ["POSTGRES_PASSWORD"]
postgres_db = os.getenv("POSTGRES_DB", "oneshop")

s3_url = urlsplit(os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"))
if s3_url.scheme not in ("http", "https") or not s3_url.netloc or s3_url.path:
    raise ValueError("S3_ENDPOINT must be an HTTP(S) URL with a host and optional port")

s3_client = boto3.client(
    "s3",
    endpoint_url=f"{s3_url.scheme}://{s3_url.netloc}",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    config=Config(s3={"addressing_style": "path"}),
)
bucket_name = os.getenv("PAGEVIEWS_BUCKET", "pageviews")

item_insert = (
    "INSERT INTO items (name, category, price, inventory) VALUES (%s, %s, %s, %s)"
)
user_insert = (
    "INSERT INTO users (first_name, last_name, email) VALUES (%s, %s, %s)"
)
purchase_insert = (
    "INSERT INTO purchases "
    "(user_id, item_id, quantity, purchase_price, created_at) "
    "VALUES (%s, %s, %s, %s, %s)"
)


def ensure_bucket():
    for attempt in range(60):
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logging.info("S3 bucket %r is ready", bucket_name)
            return
        except ClientError as error:
            status_code = error.response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode"
            )
            error_code = error.response.get("Error", {}).get("Code")
            if status_code != 404 and error_code not in (
                "404",
                "NoSuchBucket",
                "NotFound",
            ):
                raise
            try:
                s3_client.create_bucket(Bucket=bucket_name)
                logging.info("Created S3 bucket %r", bucket_name)
                return
            except ClientError as create_error:
                if create_error.response.get("Error", {}).get("Code") not in (
                    "BucketAlreadyExists",
                    "BucketAlreadyOwnedByYou",
                ):
                    raise
                logging.info("S3 bucket %r is ready", bucket_name)
                return
        except (ConnectionClosedError, EndpointConnectionError):
            if attempt == 59:
                raise
            logging.warning("Waiting for SeaweedFS S3 endpoint...")
            time.sleep(1)
    raise RuntimeError("SeaweedFS S3 endpoint did not become available")


def generate_pageview(viewer_id, target_id, page_type):
    return {
        "user_id": viewer_id,
        "url": f"/{page_type}/{target_id}",
        "channel": random.choice(channels),
        "received_at": int(time.time()),
    }


def write_to_bucket(event):
    object_name = f"event_{int(time.time())}_{uuid.uuid4()}.json"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_name,
        Body=json.dumps(event).encode("utf-8"),
        ContentType="application/json",
    )


def seed_items(cursor):
    cursor.execute("SELECT EXISTS (SELECT 1 FROM items)")
    if cursor.fetchone()[0]:
        return

    cursor.executemany(
        item_insert,
        [
            (
                barnum.create_nouns(),
                random.choice(categories),
                random.randint(item_price_min * 100, item_price_max * 100) / 100,
                random.randint(item_inventory_min, item_inventory_max),
            )
            for _ in range(item_seed_count)
        ],
    )


def seed_users(cursor):
    cursor.execute("SELECT EXISTS (SELECT 1 FROM users)")
    if cursor.fetchone()[0]:
        return

    users = []
    for _ in range(users_seed_count):
        first_name, last_name = barnum.create_name()
        users.append(
            (
                first_name,
                last_name,
                None if random.random() < 0.1 else barnum.create_email(),
            )
        )
    cursor.executemany(user_insert, users)


def generate_load(connection, cursor, item_prices, user_ids):
    logging.info("Generating pageviews and purchases")
    for _ in range(purchase_gen_count):
        purchase_item_id, purchase_item_price = random.choice(item_prices)
        purchase_user_id = random.choice(user_ids)
        purchase_quantity = random.randint(1, 5)
        purchase_ts = time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time() - random.randint(0, 24 * 60 * 60)),
        )

        write_to_bucket(
            generate_pageview(purchase_user_id, purchase_item_id, "products")
        )

        pageview_count = int(pageview_multiplier + math.sin(time.time() / 1000) * 50)
        for _ in range(pageview_count):
            write_to_bucket(
                generate_pageview(
                    random.choice(user_ids),
                    random.choice(item_prices)[0],
                    "products",
                )
            )

        cursor.execute(
            purchase_insert,
            (
                purchase_user_id,
                purchase_item_id,
                purchase_quantity,
                purchase_item_price * purchase_quantity,
                purchase_ts,
            ),
        )
        connection.commit()
        time.sleep(purchase_gen_every_ms / 1000)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    ensure_bucket()

    connection = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_pass,
        dbname=postgres_db,
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                logging.info("Seeding data if tables are empty")
                seed_items(cursor)
                seed_users(cursor)
                connection.commit()

                cursor.execute("SELECT id, price FROM items")
                item_prices = cursor.fetchall()
                cursor.execute("SELECT id FROM users")
                user_ids = [row[0] for row in cursor.fetchall()]

                if not item_prices or not user_ids:
                    raise RuntimeError("Cannot generate load without users and items")

                generate_load(connection, cursor, item_prices, user_ids)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
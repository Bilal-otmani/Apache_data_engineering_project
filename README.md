<img width="6947" height="8984" alt="diagram" src="https://github.com/user-attachments/assets/2e2f7062-15d8-4325-bdda-f33406b42ff8" />
# Data Engineering Project

This repository contains a local data-engineering stack using Docker Compose, including
SeaweedFS/Iceberg, PostgreSQL, Spark, Trino, Airflow, Kafka/Debezium, ClickHouse,
Streamlit, Flink, Superset, and MailHog.

## Local setup

1. Copy `.env.example` to `.env` and set unique local passwords and secret keys. Do not
   commit `.env`.
2. Copy `seaweedfs/s3.json.example` to `seaweedfs/s3.json` and set the same SeaweedFS
   access key and secret key as in `.env`.
3. Copy `trino/etc/config.properties.example` to `trino/etc/config.properties` and
   `trino/etc/catalog/demo.properties.example` to
   `trino/etc/catalog/demo.properties`. Set a private internal shared secret, a
   keystore password, and the SeaweedFS credentials. Create the local Trino keystore
   and password file as required by your Trino setup; these files are intentionally
   excluded from Git.
4. Add the connector JARs required by the Spark and Flink mounts to `spark/jars/` and
   `flink-connectors/`. Third-party JAR files are intentionally excluded from Git.

The generated `segmented_users_*.csv` files are also excluded because they contain
names and email addresses. The project can generate fresh local data when run.

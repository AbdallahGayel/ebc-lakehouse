# =============================================================================
# flink/Dockerfile
#
# Apache Flink 1.20 with the connector + Iceberg + Polaris JARs the EBC
# Lakehouse stream-processing pipeline needs. Building these in (rather
# than mounting a volume) gives reproducible startup and lets the JM/TM
# discover Iceberg's REST catalog factory + the Kafka/Avro connectors at
# classloader scan time.
#
# Versions are pinned to a tested matrix:
#   • Flink            1.20.x  (LTS Java 17)
#   • Iceberg          1.7.0   (matches the Trino/Polaris side)
#   • Kafka connector  3.3.0-1.20
#   • Avro+SR          1.20.0
#   • Postgres CDC     3.2.0
#   • MongoDB CDC      3.2.0
#   • AWS S3 SDK       bundle 2.29.x (transitive of iceberg-aws-bundle)
# =============================================================================
# NOTE: pin to a tag that actually exists on Docker Hub. Apache Flink hasn't
# (yet) published 1.20.1-java17 — the latest 1.20-line tag is 1.20.0-java17.
# Once 1.20.1 is published the next bump is `FROM apache/flink:1.20.1-java17`
# + matching FLINK_AVRO_VERSION below.
FROM apache/flink:1.20.0-java17

ARG ICEBERG_VERSION=1.7.0
ARG FLINK_KAFKA_VERSION=3.3.0-1.20
ARG FLINK_AVRO_VERSION=1.20.0
ARG FLINK_CDC_VERSION=3.2.0
ARG AWS_BUNDLE_VERSION=2.29.39

USER root

# Tools needed to fetch + verify JARs at build time.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# ── Iceberg runtime + AWS S3 bundle (for Polaris REST catalog + MinIO) ────
# NOTE: iceberg-flink-runtime references org.apache.hadoop.conf.Configuration
# even when the catalog is REST + S3FileIO (no actual HDFS). The Flink 1.20
# image doesn't bundle Hadoop, and iceberg-aws-bundle doesn't either, so we
# add flink-shaded-hadoop-2-uber — last-published Apache shaded uber that
# pulls org.apache.hadoop.* into a single JAR.
RUN curl -fLso /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
 && curl -fLso /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && curl -fLso /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# ── Kafka + Avro + Schema Registry connectors ─────────────────────────────
RUN curl -fLso /opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_VERSION}.jar \
 && curl -fLso /opt/flink/lib/flink-sql-avro-confluent-registry-${FLINK_AVRO_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_AVRO_VERSION}/flink-sql-avro-confluent-registry-${FLINK_AVRO_VERSION}.jar

# ── Flink CDC connectors (Postgres + MongoDB + SQL Server) ────────────────
# SQL Server CDC replaces the Cassandra Debezium-on-Kafka-Connect path —
# Cassandra has no native Flink CDC source, but SQL Server's CDC capture
# instance does, which is why we migrated atm_sessions to mssql.
RUN curl -fLso /opt/flink/lib/flink-sql-connector-postgres-cdc-${FLINK_CDC_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/${FLINK_CDC_VERSION}/flink-sql-connector-postgres-cdc-${FLINK_CDC_VERSION}.jar \
 && curl -fLso /opt/flink/lib/flink-sql-connector-mongodb-cdc-${FLINK_CDC_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mongodb-cdc/${FLINK_CDC_VERSION}/flink-sql-connector-mongodb-cdc-${FLINK_CDC_VERSION}.jar \
 && curl -fLso /opt/flink/lib/flink-sql-connector-sqlserver-cdc-${FLINK_CDC_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-sqlserver-cdc/${FLINK_CDC_VERSION}/flink-sql-connector-sqlserver-cdc-${FLINK_CDC_VERSION}.jar

# Ownership: the apache/flink image runs as user `flink` (uid 9999).
RUN chown -R flink:flink /opt/flink/lib

USER flink

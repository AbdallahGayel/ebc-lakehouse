FROM apache/airflow:2.10.3

# Pin so pip refuses to upgrade Airflow itself (which happens silently if a
# provider pulls a 3.x-only version). The constraints file pins the providers
# to versions verified against 2.10.3 — runtime libs (dbt, trino client, etc)
# are installed separately because the constraints file also pins their deps
# (e.g. pyiceberg==0.7.1) at versions that may conflict with newer requests.
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root

# 1. System dependencies (build toolchain for native wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    postgresql-client \
    build-essential \
    gcc \
    g++ \
    make \
    libev-dev \
    libssl-dev \
    libffi-dev \
    pkg-config \
    python3-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Start script
COPY --chown=airflow:root start.sh /start.sh
RUN chmod +x /start.sh

USER airflow

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# 3. Constrained install: airflow + its providers. Constraints file forces
#    apache-airflow-providers-trino back to the 5.x line (Airflow 2.x
#    compatible) instead of the 6.x line which would upgrade Airflow to 3.x.
RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
        "apache-airflow==${AIRFLOW_VERSION}" \
        "apache-airflow-providers-trino" \
        "apache-airflow-providers-http" \
        "apache-airflow-providers-common-sql" \
        "apache-airflow-providers-openlineage"

# 4. Unconstrained runtime libs. These are imported by the DAGs but never by
#    airflow core, so version drift here doesn't risk an airflow upgrade.
RUN pip install --no-cache-dir \
        "trino[sqlalchemy]>=0.330.0" \
        "boto3>=1.34.0" \
        "pyiceberg[s3fs]>=0.8.0" \
        "pyarrow>=16.1.0" \
        "requests>=2.32.0" \
        "cassandra-driver>=3.29.0" \
        "pymongo>=4.7.2" \
        "confluent-kafka[avro,schemaregistry]>=2.6.0" \
        "fastavro>=1.9.4"

# 5. dbt — pinned to the 1.8 LTS line which is verified against dbt-trino 1.8.
RUN pip install --no-cache-dir \
        "dbt-core>=1.8.0,<1.9" \
        "dbt-trino>=1.8.0,<1.9"

# 6. OpenLineage python client (used by openlineage-dbt)
RUN pip install --no-cache-dir \
        "openlineage-python==1.15.0" \
        "openlineage-dbt==1.15.0"

ENV PATH="/home/airflow/.local/bin:${PATH}"

ENTRYPOINT ["/start.sh"]

FROM apache/superset:4.1.1

USER root

# Trino dialect (provides the trino:// SQLAlchemy URL scheme) and the
# Postgres CLI for the metadata-DB bootstrap step in docker-bootstrap.sh.
RUN pip install --no-cache-dir \
        "trino[sqlalchemy]>=0.330.0" \
        "sqlalchemy-trino>=0.5.0" \
    && apt-get update -qq \
    && apt-get install -y --no-install-recommends postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# The base image sets ENV SUPERSET_CONFIG_PATH=/app/superset_config.py.
# Override it to where we place the file so Superset finds it.
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

COPY superset_config.py    /app/pythonpath/superset_config.py
COPY docker-bootstrap.sh   /app/docker-bootstrap.sh
COPY register_databases.py /app/register_databases.py
RUN chmod +x /app/docker-bootstrap.sh

USER superset

ENTRYPOINT ["/app/docker-bootstrap.sh"]
CMD ["app-gunicorn"]

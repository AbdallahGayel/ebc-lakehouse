import os

# =============================================================================
# EBC Lakehouse — Apache Superset Configuration
# =============================================================================

# ── Security ──────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "ebc_superset_secret_change_in_prod")

# Allow connections to Docker-internal IP ranges (RFC1918).
# Required because Trino/Postgres are on the Docker bridge network.
PREVENT_UNSAFE_DB_CONNECTIONS = False

# ── Metadata database (Superset's own state) ─────────────────────────────────
_db_user = os.environ.get("DATABASE_USER",     "airflow")
_db_pass = os.environ.get("DATABASE_PASSWORD", "airflow")
_db_host = os.environ.get("DATABASE_HOST",     "postgres-meta")
_db_port = os.environ.get("DATABASE_PORT",     "5432")
_db_name = os.environ.get("DATABASE_DB",       "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{_db_user}:{_db_pass}@{_db_host}:{_db_port}/{_db_name}"
)

# ── Redis — result cache + Celery broker ──────────────────────────────────────
_redis_host = os.environ.get("REDIS_HOST", "redis")
_redis_port = os.environ.get("REDIS_PORT", "6379")
_redis_base = f"redis://{_redis_host}:{_redis_port}"

CACHE_CONFIG = {
    "CACHE_TYPE":            "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX":      "superset_results_",
    "CACHE_REDIS_URL":       f"{_redis_base}/0",
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE":            "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX":      "superset_data_",
    "CACHE_REDIS_URL":       f"{_redis_base}/1",
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE":            "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX":      "superset_filter_",
    "CACHE_REDIS_URL":       f"{_redis_base}/2",
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE":            "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,
    "CACHE_KEY_PREFIX":      "superset_explore_",
    "CACHE_REDIS_URL":       f"{_redis_base}/3",
}

# ── Celery (async SQL Lab queries + scheduled reports) ────────────────────────
class CeleryConfig:
    broker_url              = f"{_redis_base}/4"
    result_backend          = f"{_redis_base}/5"
    imports                 = ("superset.sql_lab", "superset.tasks.scheduler")
    worker_prefetch_multiplier = 10
    task_acks_late          = True
    task_annotations        = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
    }

CELERY_CONFIG = CeleryConfig

# ── SQL Lab settings ──────────────────────────────────────────────────────────
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300      # 5 min max for async Trino queries
SQLLAB_TIMEOUT              = 300
SUPERSET_WEBSERVER_TIMEOUT  = 300

# ── Feature flags ─────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING":  True,   # Jinja2 in SQL ({{ from_dttm }} etc.)
    "DASHBOARD_NATIVE_FILTERS":    True,
    "DASHBOARD_CROSS_FILTERS":     True,
    "DASHBOARD_RBAC":              True,
    "EMBEDDABLE_CHARTS":           True,
    "ALERTS_ATTACH_REPORTS":       True,
    "SCHEDULED_QUERIES":           True,
}

# ── Trino — recommended query settings ────────────────────────────────────────
# Applied as defaults when Superset queries the ebc_* schemas via Trino.
QUERY_COST_ESTIMATION_ALWAYS_FORBID_NONE = False

"""
Backend API — FastAPI + Couchbase
- Startup connection pool (one connection, reused for all requests)
- API key authentication via X-API-Key header
- Rate limiting: 60 requests/minute per IP
- Prometheus metrics at /metrics
- Structured JSON logging
"""
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.security import APIKeyHeader
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions

# ── Structured JSON logger ──────────────────────────────────────────────────
class _JSONFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "ts":      datetime.now(timezone.utc).isoformat(),
            "level":   record.levelname,
            "service": "backend",
            "msg":     record.getMessage(),
        }
        for k, v in record.__dict__.items():
            if k.startswith("_") or k in (
                "msg", "args", "levelname", "levelno", "pathname", "filename",
                "module", "exc_info", "exc_text", "stack_info", "lineno",
                "funcName", "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "name", "message",
            ):
                continue
            log[k] = v
        return json.dumps(log)

_h = logging.StreamHandler()
_h.setFormatter(_JSONFormatter())
logger = logging.getLogger("backend")
logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ── Config ──────────────────────────────────────────────────────────────────
COUCHBASE_HOST   = os.environ.get("COUCHBASE_HOST", "couchbase")
COUCHBASE_BUCKET = os.environ.get("COUCHBASE_BUCKET", "order_analytics")
COUCHBASE_USER   = os.environ.get("COUCHBASE_USERNAME", "Administrator")
COUCHBASE_PASS   = os.environ.get("COUCHBASE_PASSWORD", "password")

# Comma-separated list of valid API keys — set via environment variable
_raw_keys        = os.environ.get("API_KEYS", "dev-key-change-me")
VALID_API_KEYS   = {k.strip() for k in _raw_keys.split(",") if k.strip()}

# ── Prometheus metrics ──────────────────────────────────────────────────────
REQUEST_COUNT   = Counter("api_requests_total",       "Total API requests",  ["method", "endpoint", "status"])
REQUEST_LATENCY = Histogram("api_request_latency_seconds", "API request latency")

# ── Rate limiter ────────────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)

# ── Couchbase connection pool (initialized once at startup) ─────────────────
_cluster:    Cluster | None = None
_collection                 = None


def _build_connection():
    conn_str = (
        f"couchbases://{COUCHBASE_HOST}"
        if "cloud.couchbase.com" in (COUCHBASE_HOST or "")
        else f"couchbase://{COUCHBASE_HOST}"
    )
    cluster = Cluster(
        conn_str,
        ClusterOptions(
            PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASS),
            timeout_options=ClusterTimeoutOptions(
                kv_timeout=timedelta(seconds=10),
                connect_timeout=timedelta(seconds=10),
            ),
        ),
    )
    collection = cluster.bucket(COUCHBASE_BUCKET).default_collection()
    try:
        cluster.query(f"CREATE PRIMARY INDEX ON `{COUCHBASE_BUCKET}`")
        time.sleep(3)
    except Exception:
        pass  # index already exists
    return cluster, collection


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise Couchbase connection pool on startup; release on shutdown."""
    global _cluster, _collection
    for attempt in range(1, 11):
        try:
            _cluster, _collection = _build_connection()
            logger.info("couchbase_ready", extra={"host": COUCHBASE_HOST, "bucket": COUCHBASE_BUCKET})
            break
        except Exception as e:
            logger.warning("couchbase_waiting", extra={"attempt": attempt, "error": str(e)})
            time.sleep(3)
    if _cluster is None:
        logger.error("couchbase_unavailable")
    yield
    if _cluster:
        _cluster.close()
        logger.info("couchbase_disconnected")


# ── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(title="Kafka Enterprise Orders API", version="1.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Auth ─────────────────────────────────────────────────────────────────────
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(api_key: str = Security(_api_key_header)):
    if not api_key or api_key not in VALID_API_KEYS:
        logger.warning("unauthorized_request", extra={"api_key_provided": bool(api_key)})
        raise HTTPException(status_code=403, detail="Invalid or missing API key")
    return api_key


# ── Request logging middleware ───────────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code,
    ).inc()
    REQUEST_LATENCY.observe(duration)
    logger.info("request", extra={
        "method":   request.method,
        "path":     request.url.path,
        "status":   response.status_code,
        "duration": round(duration, 4),
        "client":   request.client.host if request.client else "unknown",
    })
    return response


# ── Endpoints ────────────────────────────────────────────────────────────────
@app.get("/healthz")
def healthz():
    return {"status": "ok", "couchbase": "connected" if _collection else "unavailable"}


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/analytics", dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
def get_analytics(request: Request):
    """Return the last 10 orders from Couchbase. Requires X-API-Key header."""
    if not _cluster:
        raise HTTPException(status_code=503, detail="Database unavailable")

    try:
        query  = f"SELECT * FROM `{COUCHBASE_BUCKET}` ORDER BY order_id DESC LIMIT 10"
        result = _cluster.query(query)
        orders = []
        for row in result.rows():
            row_data = row.get(COUCHBASE_BUCKET, row) if isinstance(row, dict) else row
            orders.append(row_data)
        return {"status": "ok", "orders": orders, "count": len(orders)}

    except Exception as e:
        logger.error("query_failed", extra={"error": str(e)})
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")

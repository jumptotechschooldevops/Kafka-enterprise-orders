from fastapi import FastAPI
import os
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from datetime import timedelta

app = FastAPI()

# =========================
# ENV VARIABLES
# =========================

COUCHBASE_HOST = os.getenv("COUCHBASE_HOST")
COUCHBASE_BUCKET = os.getenv("COUCHBASE_BUCKET")
COUCHBASE_USER = os.getenv("COUCHBASE_USERNAME")
COUCHBASE_PASS = os.getenv("COUCHBASE_PASSWORD")

cluster = None
collection = None

# =========================
# CONNECT TO COUCHBASE (FIXED)
# =========================

try:
    auth = PasswordAuthenticator(COUCHBASE_USER, COUCHBASE_PASS)

    cluster = Cluster(
        f"couchbases://{COUCHBASE_HOST}",
        ClusterOptions(auth)
    )

    # IMPORTANT: wait for connection
    cluster.wait_until_ready(timedelta(seconds=10))

    bucket = cluster.bucket(COUCHBASE_BUCKET)
    collection = bucket.default_collection()

    print("✅ Connected to Couchbase")

except Exception as e:
    print("❌ Couchbase connection failed:", str(e))


# =========================
# HEALTH CHECK
# =========================

@app.get("/healthz")
def healthz():
    return {"status": "ok"}


# =========================
# QUERY DATA
# =========================

@app.get("/api/analytics")
def get_analytics():
    try:
        result = cluster.query(f"SELECT * FROM `{COUCHBASE_BUCKET}` LIMIT 10")
        rows = [row for row in result]

        return {
            "status": "success",
            "data": rows
        }

    except Exception as e:
        return {"error": str(e)}


# =========================
# WRITE
# =========================

@app.get("/api/write")
def write_data():
    try:
        collection.upsert("order-1", {"order": "123", "status": "created"})
        return {"status": "written"}
    except Exception as e:
        return {"error": str(e)}


# =========================
# READ
# =========================

@app.get("/api/read")
def read_data():
    try:
        result = collection.get("order-1")
        return result.content_as[dict]
    except Exception as e:
        return {"error": str(e)}


# =========================
# SDK TEST
# =========================

@app.get("/api/sdk-test")
def sdk_test():
    try:
        collection.upsert("ecs-test", {"msg": "hello from ECS"})
        result = collection.get("ecs-test")

        return {
            "status": "success",
            "data": result.content_as[dict]
        }

    except Exception as e:
        return {"error": str(e)}

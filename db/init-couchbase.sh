#!/bin/sh
# Full Couchbase cluster initialization + bucket creation.
# Handles both first-run (cluster not initialized) and restart (already initialized).

set -e

COUCHBASE_HOST="${COUCHBASE_HOST:-localhost}"
COUCHBASE_PORT="${COUCHBASE_PORT:-8091}"
COUCHBASE_USER="${COUCHBASE_USERNAME:-Administrator}"
COUCHBASE_PASS="${COUCHBASE_PASSWORD:-password}"
BUCKET_NAME="${COUCHBASE_BUCKET:-order_analytics}"
BASE_URL="http://${COUCHBASE_HOST}:${COUCHBASE_PORT}"

echo "Waiting for Couchbase REST API to respond..."
until curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/pools" | grep -E '^(200|401)$' > /dev/null; do
    echo "  waiting..."
    sleep 2
done
echo "Couchbase REST API is up."

# ── Check if cluster is already initialized ──────────────────────────────────
CLUSTER_STATUS=$(curl -s "${BASE_URL}/pools" | grep -o '"isAdminCreds":[^,}]*' || echo "")
IS_INITIALIZED=$(curl -s -u "${COUCHBASE_USER}:${COUCHBASE_PASS}" "${BASE_URL}/pools/default" | grep -c '"clusterUUID"' || echo "0")

if [ "$IS_INITIALIZED" = "0" ]; then
    echo "Initializing Couchbase cluster for first time..."

    # Step 1: Set data storage paths
    curl -sf -X POST "${BASE_URL}/nodes/self/controller/settings" \
        -d "path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata" \
        -d "index_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata" \
        > /dev/null 2>&1 || true

    # Step 2: Configure node services (kv=data, n1ql=query, index=index)
    curl -sf -X POST "${BASE_URL}/node/controller/setupServices" \
        -d "services=kv%2Cn1ql%2Cindex" \
        > /dev/null 2>&1 || true

    # Step 3: Set admin credentials and initialize web console
    curl -sf -X POST "${BASE_URL}/settings/web" \
        -d "username=${COUCHBASE_USER}" \
        -d "password=${COUCHBASE_PASS}" \
        -d "port=SAME" \
        > /dev/null 2>&1 || true

    # Step 4: Initialize the cluster with memory quotas
    curl -sf -X POST -u "${COUCHBASE_USER}:${COUCHBASE_PASS}" \
        "${BASE_URL}/pools/default" \
        -d "memoryQuota=512" \
        -d "indexMemoryQuota=256" \
        > /dev/null 2>&1 || true

    # Step 5: Set index storage mode
    curl -sf -X POST -u "${COUCHBASE_USER}:${COUCHBASE_PASS}" \
        "${BASE_URL}/settings/indexes" \
        -d "storageMode=forestdb" \
        > /dev/null 2>&1 || true

    echo "Cluster initialized."
    sleep 3
else
    echo "Cluster already initialized."
fi

# ── Create bucket if it doesn't exist ────────────────────────────────────────
echo "Checking bucket '${BUCKET_NAME}'..."
BUCKET_EXISTS=$(curl -s -u "${COUCHBASE_USER}:${COUCHBASE_PASS}" \
    "${BASE_URL}/pools/default/buckets/${BUCKET_NAME}" \
    | grep -c '"name"' || echo "0")

if [ "$BUCKET_EXISTS" = "0" ]; then
    echo "Creating bucket: ${BUCKET_NAME}"
    curl -sf -X POST -u "${COUCHBASE_USER}:${COUCHBASE_PASS}" \
        "${BASE_URL}/pools/default/buckets" \
        -d "name=${BUCKET_NAME}" \
        -d "bucketType=couchbase" \
        -d "ramQuotaMB=128" \
        -d "replicaNumber=0" \
        -d "flushEnabled=1" \
        > /dev/null 2>&1
    echo "Bucket '${BUCKET_NAME}' created."
    sleep 5
else
    echo "Bucket '${BUCKET_NAME}' already exists."
fi

echo "Couchbase initialization complete!"

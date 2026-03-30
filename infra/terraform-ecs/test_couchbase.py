from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from datetime import timedelta

cluster = Cluster(
    "couchbases://cb.tqyuqcmej-poi8cj.cloud.couchbase.com",
    ClusterOptions(
        PasswordAuthenticator("app_user", "JumpToTech123!")
    )
)

cluster.wait_until_ready(timeout=timedelta(seconds=15))

print("Connected!")

bucket = cluster.bucket("order_analytics")
collection = bucket.default_collection()

# ✅ create document
collection.upsert("test-key", {"msg": "success"})

# ✅ read document
result = collection.get("test-key")

print(result.content_as[dict])
